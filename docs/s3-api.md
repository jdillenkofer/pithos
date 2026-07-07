# S3 API Behavior

This page documents S3-compatible behavior that is implemented by Pithos and is useful when integrating SDKs or authorizer policies.

## Object Metadata

Pithos persists object metadata supplied on object-creating requests and returns it from `HeadObject` and `GetObject`.

Metadata is accepted on:

- `PutObject`
- `CreateMultipartUpload`, then applied when the upload is completed
- `CopyObject`

Supported metadata headers:

- `Content-Type`
- `Cache-Control`
- `Content-Disposition`
- `Content-Encoding`
- `Content-Language`
- `Expires`
- `x-amz-website-redirect-location`
- `x-amz-meta-*` user-defined metadata

User-defined metadata keys are stored in lowercase without the `x-amz-meta-` prefix. If the same metadata header appears more than once, Pithos joins the values with commas. The total size of user-defined metadata keys and values is limited to 2 KiB; oversized requests fail with `MetadataTooLarge`.

Overwriting an object replaces its metadata. A `PutObject` request without metadata clears metadata from the object it replaces.

For `CopyObject`, `x-amz-metadata-directive` controls destination metadata:

- `COPY` is the default and copies the source content type, system metadata, and user metadata.
- `REPLACE` uses the content type and metadata supplied on the copy request.
- `x-amz-website-redirect-location` is not copied from the source object. It is applied only when supplied on the copy request.

## Bucket Website Redirects

Pithos supports S3-style bucket website configuration through `PUT /<bucket>?website`, `GET /<bucket>?website`, and `DELETE /<bucket>?website`.

Supported website redirect behavior:

- `RedirectAllRequestsTo` redirects every authorized website endpoint request without looking up an object. `HostName` is required and `Protocol` may be `http` or `https`.
- `RoutingRules` are evaluated in order for normal index/error website configurations.
- Prefix-only routing rules apply before object lookup.
- `HttpErrorCodeReturnedEquals` rules apply after a generated website error, including missing objects that produce `404`.
- Rules with both `KeyPrefixEquals` and `HttpErrorCodeReturnedEquals` require both conditions to match.
- Redirect status codes `301`, `302`, `303`, `307`, and `308` are accepted. If omitted, Pithos stores and returns `301`.
- A rule cannot specify both `ReplaceKeyWith` and `ReplaceKeyPrefixWith`.
- Object-level `x-amz-website-redirect-location` still redirects successfully fetched objects when no routing rule has already matched.
- `HEAD` website requests return the same redirect status and `Location` header as `GET`, without a response body.
- When a request for `/<prefix>` misses but `/<prefix>/<index-suffix>` exists, Pithos returns a `302` redirect to `/<prefix>/`, matching browser-oriented S3 website directory behavior.

Remaining difference from AWS: Pithos does not implement unrelated static website features beyond index/error documents, redirects, routing rules, and the directory index redirect behavior described above.

## Storage Class

Pithos accepts, persists, and returns a per-object storage class.

The `x-amz-storage-class` request header is accepted on:

- `PutObject`
- `CreateMultipartUpload`, then applied to the completed object (all parts of one upload share the class chosen at creation)
- `CopyObject`

The class defaults to `STANDARD` when the header is absent. Recognized values are `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER_IR`, `GLACIER`, `DEEP_ARCHIVE`, `EXPRESS_ONEZONE`, and `OUTPOSTS`. An unrecognized value is rejected with `InvalidStorageClass` (HTTP 400).

The class is returned as the `x-amz-storage-class` response header on `GetObject` and `HeadObject` (omitted for `STANDARD`, matching AWS) and as the `StorageClass` element in `ListObjects`, `ListObjectsV2`, `ListObjectVersions`, `ListMultipartUploads`, and `ListParts`.

For `CopyObject`, the destination storage class comes from the `x-amz-storage-class` header on the copy request only; it is never carried over from the source, and it is not governed by `x-amz-metadata-directive`. A self copy (same bucket and key) is normally rejected, but is allowed when it supplies `x-amz-storage-class` to change the class in place.

**Storage classes are metadata labels.** Pithos does not implement AWS-style archival or a retrieval/restore workflow: every class, including `GLACIER` and `DEEP_ARCHIVE`, is immediately readable. Storage classes can optionally map to different part stores so a single bucket can tier data across backends — see [Storage Class Tiering](storage-backends.md#storage-class-tiering-named-part-stores).

## Bucket Lifecycle

Pithos supports the bucket lifecycle subresource (`PUT`/`GET`/`DELETE /<bucket>?lifecycle`) with the following actions:

- `Expiration` (by `Days` or `Date`).
- `AbortIncompleteMultipartUpload` (by `DaysAfterInitiation`).
- `Transition` (by `Days` or `Date`, to a target `StorageClass`).
- `NoncurrentVersionExpiration` (by `NoncurrentDays`, optionally keeping `NewerNoncurrentVersions`).

A background reconciler enforces these rules; because day-based due times round to the next midnight UTC, enforcement is prompt but never earlier than S3 semantics allow.

A `Transition` moves the object to the target storage class (relocating its part data to that class's part store) while preserving the object version and its metadata. Transitioned objects stay immediately readable — this is `STANDARD_IA`/`GLACIER_IR`-like behavior, not `GLACIER`/`DEEP_ARCHIVE` archival, regardless of the target class name. Transition validation follows S3: exactly one of `Days`/`Date`, `Days` may be `0`, `Date` must be midnight UTC, the target must be a recognized non-`STANDARD` class, targets must be distinct within a rule, and a `Days`-based transition must be strictly earlier than a `Days`-based expiration in the same rule. When an object is due for both expiration and transition, expiration wins.

`NoncurrentVersionTransition` is rejected with `NotImplemented`.

## Bucket Versioning

Pithos supports S3 bucket versioning with the `?versioning` and `?versions` bucket subresources.

`GET /<bucket>?versioning` returns the bucket's versioning configuration. A bucket that has never been configured returns a `VersioningConfiguration` response without a `Status`.

`PUT /<bucket>?versioning` accepts:

```xml
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>
```

or:

```xml
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Suspended</Status>
</VersioningConfiguration>
```

When versioning is enabled, `PutObject`, `CopyObject`, and `CompleteMultipartUpload` create new object versions and return `x-amz-version-id`. Older versions remain addressable.

When versioning is suspended or has never been enabled, SQL-backed storage uses the S3 `null` version. Existing completed objects are migrated to the `null` version when the database schema is upgraded, and subsequent writes in this state replace that `null` version.

`HeadObject` and `GetObject` accept `?versionId=<version-id>` and return `x-amz-version-id` when the selected object has a version ID. `DeleteObject` accepts the same `versionId` query parameter to delete a specific version. Multi-object delete supports `<VersionId>` entries.

Deleting by key in an enabled or suspended bucket creates a delete marker and returns delete marker metadata:

- `DeleteObject` returns `x-amz-delete-marker: true` and `x-amz-version-id`.
- `DeleteObjects` returns `DeleteMarker` and `DeleteMarkerVersionId` for key-only deletes.
- Reading a key whose latest version is a delete marker returns `404` with `x-amz-delete-marker: true`.
- Reading an explicit delete marker version returns `405` with `x-amz-delete-marker: true`, `x-amz-version-id`, and `Last-Modified`.

`GET /<bucket>?versions` lists object versions and delete markers. It supports `prefix`, `delimiter`, `key-marker`, `version-id-marker`, and `max-keys`, and returns `Version`, `DeleteMarker`, `CommonPrefixes`, `NextKeyMarker`, and `NextVersionIdMarker` elements.

## Multipart Upload Listing

`GET /<bucket>?uploads` supports `prefix`, `delimiter`, `key-marker`, `upload-id-marker`, and `max-uploads`.

When a response is truncated, resume with both `NextKeyMarker` and `NextUploadIdMarker`. The marker pair is significant because multiple pending multipart uploads can share the same object key.

## Authorization Operations

Versioned requests use distinct Lua authorizer operation names:

- `HeadObjectVersion`
- `GetObjectVersion`
- `DeleteObjectVersion`
- `GetBucketVersioning`
- `PutBucketVersioning`
- `ListObjectVersions`

See [Configuration](configuration.md#available-operations) for the complete operation list.
