# Policy authorization

Pithos has a file-based, AWS-IAM-inspired authorization backend. It is a
documented subset, not full IAM compatibility: bucket policies, roles,
sessions, permission boundaries, policy variables and cross-account grants are
not supported. The same-account boundary remains mandatory.

Enable it with `PITHOS_AUTHORIZER_TYPE=policy`. The policy file defaults to
`./policies.json` (`PITHOS_POLICY_PATH`). It is reloaded every five seconds
(`PITHOS_POLICY_RELOAD_INTERVAL_SECONDS`); zero disables reloads. Startup
requires a valid file. Failed reloads retain the last valid compiled snapshot.

```json
{
  "schemaVersion": 1,
  "policies": {
    "public-read": {
      "Version": "2012-10-17",
      "Statement": {
        "Sid": "ReadWebsite",
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::website/*"
      }
    }
  },
  "bindings": [{
    "policy": "public-read",
    "subjects": [{"type": "anonymous"}]
  }]
}
```

Authenticated subjects use `{"type":"principal","accountId":"...",
"principalId":"..."}` and survive access-key rotation. Several bindings are
combined. Explicit Deny wins over every Allow; no matching Allow is an implicit
deny. Validate files offline with `pithos validate-policy <path>`.

`Action` and `Resource` accept strings or arrays. Resources use S3 ARNs;
`*` and `?` wildcards are supported. Conditions support String, ARN, Numeric
and Date comparisons and negations, `Bool`, `IpAddress`, `NotIpAddress`,
`Null`, `IfExists`, `ForAnyValue` and `ForAllValues`. Supported context covers
the documented global `aws:*` keys, Pithos identity, list parameters, version
ID, auth/signature type, request/existing object tags and object-lock values.

## Operation mapping

| Pithos operation | Required action |
|---|---|
| ListBuckets | `s3:ListAllMyBuckets` |
| HeadBucket, ListObjects | `s3:ListBucket` |
| GetObject, HeadObject | `s3:GetObject` |
| versioned GetObject/HeadObject | `s3:GetObjectVersion` |
| PutObject, AppendObject, multipart writes | `s3:PutObject` |
| DeleteObject / versioned delete | `s3:DeleteObject` / `s3:DeleteObjectVersion` |
| CopyObject, UploadPartCopy | source `s3:GetObject` (or version) and destination `s3:PutObject` |
| AbortMultipartUpload | `s3:AbortMultipartUpload` |
| ListParts | `s3:ListMultipartUploadParts` |

Tag and object-lock headers add their tagging, retention or legal-hold action.
Multi-delete is checked exclusively per entry. Lua remains the default backend;
its existing boolean result maps `true` to Allow and `false` to ExplicitDeny.
Both backends evaluate anonymous operations; `ListBuckets` and `CreateBucket`
still require authentication because their ownership semantics require an
account identity.
