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
