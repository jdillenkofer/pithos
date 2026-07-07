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

Remaining difference from AWS: Pithos does not implement browser-oriented directory probing beyond the configured index suffix; redirect routing is limited to the conditions and redirect fields listed above.

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
