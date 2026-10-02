"""Shared request and rejection cases for the Python and C++ SigV4 verifiers."""

ACCESS_KEY = "FSPTESTACCESSKEY0001"
SECRET_KEY = "sigv4-test-secret"
BUCKET = "sigv4-test"
REGION = "us-east-1"

VALID_REQUESTS = (
    {
        "name": "get",
        "method": "GET",
        "path": f"/{BUCKET}/allowed/safe.txt",
        "query": "",
        "headers": {},
        "status": 200,
        "body": b"safe",
    },
    {
        "name": "head",
        "method": "HEAD",
        "path": f"/{BUCKET}/allowed/safe.txt",
        "query": "",
        "headers": {},
        "status": 200,
        "body": b"",
    },
    {
        "name": "range_get",
        "method": "GET",
        "path": f"/{BUCKET}/allowed/safe.txt",
        "query": "",
        "headers": {"range": "bytes=0-1"},
        "status": 206,
        "body": b"sa",
    },
    {
        "name": "list_first_page",
        "method": "GET",
        "path": f"/{BUCKET}/",
        "query": "list-type=2&max-keys=1&prefix=allowed%2F",
        "headers": {},
        "status": 200,
        "body_contains": b"<IsTruncated>true</IsTruncated>",
    },
    {
        "name": "list_continuation_page",
        "method": "GET",
        "path": f"/{BUCKET}/",
        "query": (
            "list-type=2&max-keys=1&prefix=allowed%2F"
            "&continuation-token=allowed%2Fnested%2Fitem.txt"
        ),
        "headers": {},
        "status": 200,
        "body_contains": b"<Key>allowed/safe.txt</Key>",
    },
    {
        "name": "encoded_space",
        "method": "GET",
        "path": f"/{BUCKET}/allowed/space%20name.txt",
        "query": "",
        "headers": {},
        "status": 200,
        "body": b"space",
    },
    {
        "name": "encoded_slashes",
        "method": "GET",
        "path": f"/{BUCKET}/allowed%2Fnested%2Fitem.txt",
        "query": "",
        "headers": {},
        "status": 200,
        "body": b"nested",
    },
    {
        "name": "double_encoded_percent",
        "method": "GET",
        "path": f"/{BUCKET}/allowed/%252F.txt",
        "query": "",
        "headers": {},
        "status": 200,
        "body": b"percent",
    },
)

INVALID_REQUESTS = (
    {"name": "altered_signature", "base": "get", "mutation": "signature",
     "error": "SignatureDoesNotMatch"},
    {"name": "malformed_date", "base": "get", "mutation": "malformed_date",
     "error": "AccessDenied"},
    {"name": "stale_date", "base": "get", "mutation": "stale_date",
     "error": "RequestTimeTooSkewed"},
    {"name": "scope_date_mismatch", "base": "get", "mutation": "scope_date",
     "error": "SignatureDoesNotMatch"},
    {"name": "malformed_scope", "base": "get", "mutation": "malformed_scope",
     "error": "AccessDenied"},
    {"name": "wrong_service", "base": "get", "mutation": "wrong_service",
     "error": "AccessDenied"},
    {"name": "altered_path", "base": "get", "mutation": "path",
     "error": "SignatureDoesNotMatch"},
    {"name": "altered_query", "base": "list_first_page", "mutation": "query",
     "error": "SignatureDoesNotMatch"},
    {"name": "altered_signed_header", "base": "range_get", "mutation": "range",
     "error": "SignatureDoesNotMatch"},
    {"name": "altered_payload_hash", "base": "get", "mutation": "payload_hash",
     "error": "SignatureDoesNotMatch"},
)


def mutate_request(mutation, headers, path, query):
    """Apply the same single-field tampering cases in both verifier test suites."""
    headers = dict(headers)
    if mutation == "signature":
        auth_key = next(key for key in headers if key.lower() == "authorization")
        headers[auth_key] = headers[auth_key][:-1] + (
            "0" if headers[auth_key][-1] != "0" else "1"
        )
    elif mutation == "malformed_date":
        date_key = next(key for key in headers if key.lower() == "x-amz-date")
        headers[date_key] = "20260230T010203Z"
    elif mutation in {"stale_date", "scope_date"}:
        date_key = next(key for key in headers if key.lower() == "x-amz-date")
        auth_key = next(key for key in headers if key.lower() == "authorization")
        current_date = headers[date_key][:8]
        replacement = "20000101" if mutation == "stale_date" else "19990101"
        headers[auth_key] = headers[auth_key].replace(
            f"/{current_date}/", f"/{replacement}/", 1
        )
        if mutation == "stale_date":
            headers[date_key] = f"{replacement}T000000Z"
    elif mutation == "wrong_service":
        auth_key = next(key for key in headers if key.lower() == "authorization")
        headers[auth_key] = headers[auth_key].replace(
            "/s3/aws4_request", "/ec2/aws4_request", 1
        )
    elif mutation == "malformed_scope":
        auth_key = next(key for key in headers if key.lower() == "authorization")
        headers[auth_key] = headers[auth_key].replace(
            "/us-east-1/s3/aws4_request", "/s3/aws4_request", 1
        )
    elif mutation == "path":
        path = path.replace("/safe.txt", "/space%20name.txt")
    elif mutation == "query":
        query = query.replace("max-keys=1", "max-keys=2", 1)
    elif mutation == "range":
        range_key = next(key for key in headers if key.lower() == "range")
        headers[range_key] = "bytes=1-2"
    elif mutation == "payload_hash":
        hash_key = next(
            key for key in headers if key.lower() == "x-amz-content-sha256"
        )
        headers[hash_key] = "0" * 64
    else:
        raise ValueError(f"Unknown SigV4 mutation: {mutation}")
    return headers, path, query
