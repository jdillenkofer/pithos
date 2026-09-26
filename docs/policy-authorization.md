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
Action names match case-insensitively; resource names and `Like` condition
values match case-sensitively. `*` matches zero or more Unicode characters and
`?` matches one, including newlines. Other characters are literal, not regular
expression syntax. Wildcard patterns are compiled when loading the file and
reused by requests until the next successful reload.

## Conditions

Condition values must be a JSON string or a nonempty array of strings, including
numeric and boolean values: use `"30"` and `"true"`, not `30` and `true`.
Numeric values must be finite, dates must use RFC3339 (fractional seconds are
accepted), and IP values must be IP addresses or CIDRs. Invalid values reject
the file at startup or leave the previous snapshot active during reload.

All condition entries in a statement must match. For one context value,
positive operators match any of the supplied policy values; negated operators
match only when none of those values match. Use explicit set operators for
multivalued context such as tag keys:

| Operator | Comparison |
|---|---|
| `StringEquals`, `StringNotEquals` | Literal, case-sensitive equality or inequality |
| `StringEqualsIgnoreCase`, `StringNotEqualsIgnoreCase` | Case-insensitive equality or inequality |
| `StringLike`, `StringNotLike` | Wildcard match or its negation |
| `ArnEquals`, `ArnNotEquals` | Literal, case-sensitive equality or inequality; no ARN-specific expansion |
| `ArnLike`, `ArnNotLike` | The same wildcard matching as `StringLike` / `StringNotLike` |
| `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals` | Numeric comparison using floating-point values |
| `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals` | Comparison of RFC3339 timestamps |
| `Bool` | Boolean equality |
| `IpAddress`, `NotIpAddress` | IP equality or CIDR membership, or their negation |
| `Null` | `"true"` requires an absent key; `"false"` requires a present key; exactly one value |

Except for `Null`, operators may have the `IfExists` suffix and the
`ForAnyValue:` or `ForAllValues:` prefix, including both together.
`IfExists` makes an absent key satisfy the condition, including in a Deny
statement, except with `ForAnyValue:`. `ForAllValues:` requires every context value to match and is true
when the key is absent. `ForAnyValue:` requires at least one matching context
value and is false for absent keys or empty sets, including with negated
operators and `IfExists`. Without a set prefix, present multivalued
context also uses any-value matching. An absent key otherwise fails positive
comparisons and satisfies negated comparisons. These are Pithos subset semantics, not a promise of
full IAM equivalence. Pair conditions with `Null: {"key": "false"}` when the key
must be present.

Condition key names are case-insensitive, but the tag-name suffix after `/`
is case-sensitive. These are all supported keys:

| Key | Value and availability |
|---|---|
| `aws:CurrentTime` | Current UTC time as RFC3339, with second precision |
| `aws:EpochTime` | Current Unix time in seconds |
| `aws:PrincipalAccount` | Authenticated caller's account ID; absent for anonymous requests |
| `aws:ResourceAccount` | Target bucket owner's account ID when resolved by the server; copies use the source owner for read checks and the destination owner for write checks |
| `aws:SourceIp` | Resolved client IP, falling back to the direct peer IP |
| `aws:SecureTransport` | `"true"` for HTTPS, `"false"` otherwise |
| `aws:UserAgent` | Values of the `User-Agent` header; absent when omitted |
| `aws:Referer` | Values of the `Referer` header; absent when omitted |
| `pithos:PrincipalId` | Authenticated principal ID; absent for anonymous requests |
| `pithos:AccessKeyId` | Authenticated access-key ID; absent for anonymous requests |
| `pithos:AuthType`, `s3:authType` | `Anonymous`, `REST-HEADER`, or `REST-QUERY-STRING`, based on the authentication result |
| `s3:signatureversion` | Verified signature algorithm: `AWS4-HMAC-SHA256` for SigV4 or `AWS4-ECDSA-P256-SHA256` for SigV4a (header and presigned requests); absent for anonymous requests or missing verified algorithm metadata |
| `s3:prefix` | Raw `prefix` query values; absent when omitted, not implicitly an empty string |
| `s3:delimiter` | Raw `delimiter` query values; absent when omitted |
| `s3:max-keys` | Raw `max-keys` query values; absent when omitted, not the listing default |
| `s3:VersionId` | Explicitly requested version ID; for copy operations this is the source version; absent when omitted |
| `s3:RequestObjectTagKeys` | Keys of supplied object tags; absent for an empty tag set |
| `s3:RequestObjectTag/<tag-name>` | Supplied tag value, if present; an empty string is still present |
| `s3:ExistingObjectTag/<tag-name>` | Stored target-object tag value; uses source-object tags for the read side of a copy; absent if the tag/object is missing |
| `s3:object-lock-mode` | Requested mode (`GOVERNANCE` or `COMPLIANCE`), or the mode of a supplied bucket default retention |
| `s3:object-lock-retain-until-date` | Requested object-retention expiry as RFC3339 |
| `s3:object-lock-legal-hold` | Requested legal-hold status (`ON` or `OFF`) |
| `s3:object-lock-remaining-retention-days` | Days from the current time to the requested object-retention expiry, rounding partial days up; for bucket defaults, supplied days or years multiplied by 365; absent when neither is supplied |

`aws:SourceIp` and `aws:SecureTransport` honor the configured trusted-proxy
settings. Without forwarded-header trust, they use the direct connection.
User-Agent and Referer are client-supplied values and are not identity proofs.
List query keys are exposed as supplied, without operation-specific defaults.
Repeated query parameters and malformed query strings are rejected with
HTTP 400 (`InvalidArgument`) before authentication or authorization, for both
Lua and policy backends. This also applies to anonymous and website requests.
Accepted query strings are not rewritten, preserving signature verification;
malformed numeric/date context causes an authorization error and fails closed.

Request tags come from `PutObjectTagging` bodies or applicable `x-amz-tagging`
headers on `PutObject`, `CreateMultipartUpload`, and `CopyObject` with the
`REPLACE` tagging directive. Tags inherited by a copy are not request tags.
For `UploadPart`, `UploadPartCopy`, and `CompleteMultipartUpload`, request-tag
conditions use the tags stored at multipart initiation. Headers on these later
requests cannot replace those tags. The lookup is lazy and fails closed on
errors or when the storage backend cannot expose initiation tags (including
the S3-client backend, whose upstream ListParts API does not return tags).
Existing tags are fetched lazily only when a matching statement needs them;
lookup errors fail closed. Object-lock context describes the request, not
the object's previously stored lock state or an automatically inherited
bucket default. On object writes, the request's retain-until date takes
precedence when computing remaining retention days.

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

Applicable tag headers add `s3:PutObjectTagging`. Explicit retention and
legal-hold values on `PutObject`, `AppendObject`, `CreateMultipartUpload`, and
`CopyObject` add `s3:PutObjectRetention` and `s3:PutObjectLegalHold`, respectively.
Setting bucket default retention requires `s3:PutBucketObjectLockConfiguration`
on the bucket ARN, without an additional object-retention action. Dedicated
object-retention and legal-hold operations require only their respective
primary action (plus a separate governance-bypass check when applicable).
Multi-delete is checked exclusively per entry. Lua remains the default backend;
its existing boolean result maps `true` to Allow and `false` to ExplicitDeny.
Both backends evaluate anonymous operations; `ListBuckets` and `CreateBucket`
still require authentication because their ownership semantics require an
account identity.
