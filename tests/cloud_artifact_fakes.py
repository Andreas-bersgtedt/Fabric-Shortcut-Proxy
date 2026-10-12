"""In-memory SDK doubles with conditional-write checks at commit time."""
from __future__ import annotations

from datetime import datetime, timezone
import io
import threading
from types import SimpleNamespace


class CloudError(Exception):
    def __init__(self, code):
        self.code = code
        self.response = {"ResponseMetadata": {"HTTPStatusCode": code}}
        super().__init__(str(code))


class _StreamingBody(io.BytesIO):
    def iter_chunks(self, size):
        return iter(lambda: self.read(size), b"")


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.sequence = 0
        self.lock = threading.Lock()
        self.fail_part = False
        self.last_body = None
        self.completed_options = None

    def put_object(self, *, Bucket, Key, Body, Metadata=None, IfMatch=None, IfNoneMatch=None):
        with self.lock:
            current = self.objects.get(Key)
            if (IfNoneMatch == "*" and current is not None) or (
                IfMatch is not None and (current is None or current["ETag"] != IfMatch)
            ):
                raise CloudError(412)
            self.sequence += 1
            self.objects[Key] = {
                "Body": bytes(Body), "Metadata": Metadata or {}, "ETag": str(self.sequence),
            }
        return {"ETag": str(self.sequence)}

    def head_object(self, *, Bucket, Key):
        with self.lock:
            if Key not in self.objects:
                raise CloudError(404)
            value = self.objects[Key]
            return {
                "ContentLength": len(value["Body"]), "ETag": value["ETag"],
                "LastModified": datetime(2026, 10, 9, tzinfo=timezone.utc),
            }

    def get_object(self, *, Bucket, Key, Range=None):
        with self.lock:
            if Key not in self.objects:
                raise CloudError(404)
            value = self.objects[Key]
            data = value["Body"]
            etag = value["ETag"]
        if Range:
            start, end = Range.removeprefix("bytes=").split("-")
            data = data[int(start):int(end) + 1 if end else None]
        body = _StreamingBody(data)
        self.last_body = body
        return {"Body": body, "ETag": etag}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop(Key, None)

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        start = int(ContinuationToken or 0)
        page = keys[start:start + 2]
        return {
            "Contents": [{"Key": key, "Size": len(self.objects[key]["Body"])} for key in page],
            "IsTruncated": start + 2 < len(keys),
            "NextContinuationToken": str(start + 2),
        }

    def create_multipart_upload(self, *, Bucket, Key, Metadata):
        upload_id = str(len(self.uploads) + 1)
        self.uploads[upload_id] = {"Metadata": Metadata, "Parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body):
        if self.fail_part:
            raise CloudError(503)
        self.uploads[UploadId]["Parts"][PartNumber] = Body
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, **conditions):
        self.completed_options = conditions
        upload = self.uploads[UploadId]
        self.put_object(
            Bucket=Bucket, Key=Key, Metadata=upload["Metadata"],
            Body=b"".join(upload["Parts"][part["PartNumber"]] for part in MultipartUpload["Parts"]),
            **conditions,
        )
        del self.uploads[UploadId]

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        del self.uploads[UploadId]


class _Writer(io.BytesIO):
    def __init__(self, blob, generation):
        super().__init__()
        self.blob = blob
        self.generation = generation

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.blob.upload_from_string(self.getvalue(), if_generation_match=self.generation)
        self.close()


class FakeGCSBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.metadata = {}
        self.generation = None

    def reload(self):
        with self.bucket.lock:
            if self.name not in self.bucket.objects:
                raise CloudError(404)
            value = self.bucket.objects[self.name]
            self.generation = value["generation"]
            self.size = len(value["body"])
            self.metadata = dict(value["metadata"])
            self.updated = datetime(2026, 10, 9, tzinfo=timezone.utc)

    def upload_from_string(self, data, *, if_generation_match=None):
        with self.bucket.lock:
            current = self.bucket.objects.get(self.name)
            generation = current["generation"] if current else 0
            if if_generation_match is not None and if_generation_match != generation:
                raise CloudError(412)
            self.bucket.sequence += 1
            self.bucket.objects[self.name] = {
                "body": bytes(data), "metadata": dict(self.metadata),
                "generation": self.bucket.sequence,
            }

    def download_as_bytes(self, *, if_generation_match=None, raw_download=True):
        with self.bucket.lock:
            current = self.bucket.objects.get(self.name)
            if current is None:
                raise CloudError(404)
            if if_generation_match is not None and current["generation"] != if_generation_match:
                raise CloudError(412)
            return current["body"]

    def open(self, mode, *, chunk_size, if_generation_match=None, raw_download=False):
        if mode == "wb":
            assert chunk_size % (256 * 1024) == 0
            return _Writer(self, if_generation_match)
        assert raw_download
        return io.BytesIO(self.download_as_bytes(if_generation_match=self.generation))

    def delete(self):
        if self.name not in self.bucket.objects:
            raise CloudError(404)
        del self.bucket.objects[self.name]


class FakeGCSBucket:
    def __init__(self):
        self.objects = {}
        self.lock = threading.Lock()
        self.sequence = 0

    def blob(self, name):
        return FakeGCSBlob(self, name)

    def list_blobs(self, *, prefix):
        for name, value in sorted(self.objects.items()):
            if name.startswith(prefix):
                yield SimpleNamespace(
                    name=name, size=len(value["body"]), generation=value["generation"],
                    updated=datetime(2026, 10, 9, tzinfo=timezone.utc),
                )
