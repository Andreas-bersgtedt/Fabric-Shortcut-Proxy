"""Storage proxy package — secured S3/NFS/SMB passthrough (devplan/StorageProxy.md).

Additive capability: a bucket listed in the mount table is served as byte
passthrough from a storage backend; every other bucket resolves through the
existing relational -> Iceberg path unchanged.
"""
