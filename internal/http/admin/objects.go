package admin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
)

type objectCursor struct {
	Bucket, Prefix, Key, Version string
	Versions                     bool
}
type objectLink struct {
	Label, URL, ViewURL, Version, SizeText, Modified, Type, StorageClass string
	// Select is the value of the row's selection checkbox.
	Select               string
	Size                 int64
	DeleteMarker, Latest bool
}
type objectsView struct {
	CSRF, Bucket, Prefix, Parent, NextURL, VersionsURL, UploadURL string
	Versions                                                      bool
	Prefixes, Objects, Crumbs                                     []objectLink
	Header                                                        bucketHeader
}
type objectUploadView struct {
	CSRF, Bucket, Prefix, BackURL string
	Crumbs                        []objectLink
}

// listViewURL links viewable list rows to the inline viewer. Listings carry
// no content type, so the decision uses the key's extension.
func listViewURL(bucket, key, version string, deleteMarker bool) string {
	if deleteMarker {
		return ""
	}
	if kind, _ := previewOf(nil, key); kind == "" {
		return ""
	}
	return objectURL("/objects/view", bucket, key, version)
}

// objectType mirrors the S3 console's type column: the file extension.
func objectType(key string) string {
	if ext := strings.TrimPrefix(path.Ext(key), "."); ext != "" && !strings.Contains(ext, "/") {
		return strings.ToLower(ext)
	}
	return "-"
}

func storageClassLabel(class *string) string {
	if class == nil || *class == "" {
		return "Standard"
	}
	return *class
}

// prefixCrumbs links every enclosing prefix of key or prefix in a bucket.
func prefixCrumbs(bucket, prefix string, versions bool) []objectLink {
	var crumbs []objectLink
	q := url.Values{"bucket": {bucket}}
	if versions {
		q.Set("versions", "true")
	}
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == '/' {
			q.Set("prefix", prefix[:i+1])
			start := strings.LastIndex(prefix[:i], "/") + 1
			crumbs = append(crumbs, objectLink{Label: strings.TrimSuffix(prefix[start:i+1], "/"), URL: "/objects?" + q.Encode()})
		}
	}
	return crumbs
}

func formatModified(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04")
}

type objectDetailView struct {
	CSRF, Bucket, Key, Name, Version, DownloadURL, BucketURL, VersionsURL, DetailURL string
	Preview, ViewURL, PreviewText                                                    string
	PreviewTruncated                                                                 bool
	DeleteMarker                                                                     bool
	Crumbs                                                                           []objectLink
	Overview, Checksums, Metadata, TagList                                           []keyValue
	StorageClass, Retention, LegalHold                                               string
}

// objectError exposes only known protocol errors, never backend connection
// strings, remote URLs, or other provider details embedded in arbitrary errors.
func (a *App) objectError(w http.ResponseWriter, r *http.Request, err error) {
	status, message := objectErrorMessage(err)
	a.fail(w, r, status, message)
}

func objectErrorMessage(err error) (int, string) {
	status, message := http.StatusInternalServerError, "Object operation failed"
	switch {
	case errors.Is(err, storage.ErrNoSuchKey), errors.Is(err, storage.ErrNoSuchBucket):
		status, message = http.StatusNotFound, "Object or bucket not found"
	case errors.Is(err, storage.ErrObjectLockAccessDenied):
		status, message = http.StatusForbidden, "Object retention or legal hold prevents this operation"
	case errors.Is(err, storage.ErrInvalidObjectLockConfiguration), errors.Is(err, storage.ErrObjectLockConfigurationNotFound):
		status, message = http.StatusBadRequest, "Invalid or unavailable object lock configuration"
	case errors.Is(err, storage.ErrEntityTooLarge):
		status, message = http.StatusRequestEntityTooLarge, "Object is too large"
	case errors.Is(err, storage.ErrNotImplemented):
		status, message = http.StatusNotImplemented, "This storage backend does not support the operation"
	case errors.Is(err, storage.ErrPreconditionFailed):
		status, message = http.StatusConflict, "Object changed concurrently"
	}
	return status, message
}

func (a *App) registerObjects(mux *http.ServeMux) {
	mux.HandleFunc("GET /objects", a.objects)
	mux.HandleFunc("GET /objects/detail", a.objectDetail)
	mux.HandleFunc("GET /objects/download", a.download)
	mux.HandleFunc("GET /objects/view", a.view)
	a.registerVersions(mux)
	a.registerBatchDelete(mux)
	mux.HandleFunc("GET /objects/upload", a.uploadPage)
	mux.HandleFunc("POST /objects/upload", a.upload)
	mux.HandleFunc("POST /objects/delete", a.deleteObject)
	mux.HandleFunc("POST /objects/tags", a.objectTags)
	mux.HandleFunc("POST /objects/retention", a.objectRetention)
	mux.HandleFunc("POST /objects/legal-hold", a.objectLegalHold)
}
func objectURL(route, bucket, key, version string) string {
	q := url.Values{"bucket": {bucket}}
	if key != "" {
		q.Set("key", key)
	}
	if version != "" {
		q.Set("version", version)
	}
	return route + "?" + q.Encode()
}
func objectTarget(r *http.Request) (storage.BucketName, storage.ObjectKey, *string, error) {
	b, err := storage.NewBucketName(r.FormValue("bucket"))
	if err != nil {
		return b, storage.ObjectKey{}, nil, err
	}
	k, err := storage.NewObjectKey(r.FormValue("key"))
	if err != nil {
		return b, k, nil, err
	}
	var v *string
	if value := r.FormValue("version"); value != "" {
		v = &value
	}
	return b, k, v, nil
}
func (a *App) objects(w http.ResponseWriter, r *http.Request) {
	bucket, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	versions := r.URL.Query().Get("versions") == "true"
	cursor := objectCursor{Bucket: bucket.String(), Prefix: prefix, Versions: versions}
	if token := r.URL.Query().Get("continuation-token"); token != "" {
		if len(token) > 8192 {
			a.fail(w, r, 400, "Invalid continuation token")
			return
		}
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || json.Unmarshal(decoded, &cursor) != nil || cursor.Bucket != bucket.String() || cursor.Prefix != prefix || cursor.Versions != versions {
			a.fail(w, r, 400, "Invalid continuation token")
			return
		}
	}
	view := objectsView{CSRF: csrfToken(r), Bucket: bucket.String(), Prefix: prefix, Versions: versions}
	parent := ""
	if pos := strings.LastIndex(strings.TrimSuffix(prefix, "/"), "/"); pos >= 0 {
		parent = prefix[:pos+1]
	}
	q := url.Values{"bucket": {bucket.String()}, "prefix": {parent}, "versions": {fmt.Sprint(versions)}}
	view.Parent = "/objects?" + q.Encode()
	view.Crumbs = prefixCrumbs(bucket.String(), prefix, versions)
	view.Header = bucketHeader{Name: bucket.String(), Active: "objects"}
	view.UploadURL = "/objects/upload?" + url.Values{"bucket": {bucket.String()}, "prefix": {prefix}}.Encode()
	q.Set("prefix", prefix)
	q.Set("versions", fmt.Sprint(!versions))
	view.VersionsURL = "/objects?" + q.Encode()
	q.Set("versions", fmt.Sprint(versions))
	// Only hide the enclosing folder, not the partial filename used to filter.
	displayPrefix := prefix[:strings.LastIndex(prefix, "/")+1]
	delimiter := "/"
	var prefixes []string
	var truncated bool
	if versions {
		opts := storage.ListObjectVersionsOptions{Prefix: &prefix, Delimiter: &delimiter, MaxKeys: 100}
		// Copy the markers: the cursor is advanced below and must not alias options.
		if keyMarker := cursor.Key; keyMarker != "" {
			opts.KeyMarker = &keyMarker
			if versionMarker := cursor.Version; versionMarker != "" {
				opts.VersionIDMarker = &versionMarker
			}
		}
		result, err := a.options.Storage.ListObjectVersions(r.Context(), bucket, opts)
		if err != nil {
			a.objectError(w, r, err)
			return
		}
		prefixes = result.CommonPrefixes
		truncated = result.IsTruncated
		for _, o := range result.Versions {
			view.Objects = append(view.Objects, objectLink{Label: strings.TrimPrefix(o.Key.String(), displayPrefix), Version: o.VersionID, URL: objectURL("/objects/detail", bucket.String(), o.Key.String(), o.VersionID), ViewURL: listViewURL(bucket.String(), o.Key.String(), o.VersionID, o.IsDeleteMarker), Select: objectRef{Key: o.Key.String(), Version: o.VersionID}.Value(), Size: o.Size, SizeText: humanBytes(float64(o.Size)), Modified: formatModified(o.LastModified), Type: objectType(o.Key.String()), StorageClass: storageClassLabel(o.StorageClass), DeleteMarker: o.IsDeleteMarker, Latest: o.IsLatest})
		}
		if result.NextKeyMarker != nil {
			cursor.Key = *result.NextKeyMarker
		}
		if result.NextVersionIDMarker != nil {
			cursor.Version = *result.NextVersionIDMarker
		}
	} else {
		opts := storage.ListObjectsOptions{Prefix: &prefix, Delimiter: &delimiter, MaxKeys: 100}
		if startAfter := cursor.Key; startAfter != "" {
			opts.StartAfter = &startAfter
		}
		result, err := a.options.Storage.ListObjects(r.Context(), bucket, opts)
		if err != nil {
			a.objectError(w, r, err)
			return
		}
		prefixes = result.CommonPrefixes
		truncated = result.IsTruncated
		for _, o := range result.Objects {
			view.Objects = append(view.Objects, objectLink{Label: strings.TrimPrefix(o.Key.String(), displayPrefix), URL: objectURL("/objects/detail", bucket.String(), o.Key.String(), ""), ViewURL: listViewURL(bucket.String(), o.Key.String(), "", false), Select: objectRef{Key: o.Key.String()}.Value(), Size: o.Size, SizeText: humanBytes(float64(o.Size)), Modified: formatModified(o.LastModified), Type: objectType(o.Key.String()), StorageClass: storageClassLabel(o.StorageClass)})
			if o.Key.String() > cursor.Key {
				cursor.Key = o.Key.String()
			}
		}
		for _, p := range prefixes {
			if p > cursor.Key {
				cursor.Key = p
			}
		}
	}
	for _, p := range prefixes {
		q.Set("prefix", p)
		view.Prefixes = append(view.Prefixes, objectLink{Label: strings.TrimPrefix(p, displayPrefix), URL: "/objects?" + q.Encode()})
	}
	if truncated {
		b, _ := json.Marshal(cursor)
		q.Set("prefix", prefix)
		q.Set("continuation-token", base64.RawURLEncoding.EncodeToString(b))
		view.NextURL = "/objects?" + q.Encode()
	}
	a.render(w, r, "Objects", "objects", view)
}
func (a *App) objectDetail(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	version := ""
	if v != nil {
		version = *v
	}
	object, err := a.options.Storage.HeadObject(r.Context(), b, k, &storage.HeadObjectOptions{VersionID: v})
	var marker *storage.VersionDeleteMarkerMethodNotAllowedError
	var current *storage.CurrentDeleteMarkerError
	view := objectDetailView{CSRF: csrfToken(r), Bucket: b.String(), Key: k.String(), Name: path.Base(k.String()), Version: version, DownloadURL: objectURL("/objects/download", b.String(), k.String(), version), BucketURL: "/objects?" + url.Values{"bucket": {b.String()}}.Encode()}
	view.Crumbs = prefixCrumbs(b.String(), k.String()[:strings.LastIndex(k.String(), "/")+1], false)
	view.VersionsURL = objectURL("/objects/versions", b.String(), k.String(), "")
	view.DetailURL = objectURL("/objects/detail", b.String(), k.String(), "")
	if errors.As(err, &marker) || errors.As(err, &current) {
		view.DeleteMarker = true
		if marker != nil {
			view.Version = marker.VersionID
		}
		if current != nil {
			view.Version = current.VersionID
		}
	} else if err != nil {
		a.objectError(w, r, err)
		return
	} else {
		describeObject(&view, b.String(), k.String(), object)
		if kind, _ := previewOf(object.ContentType, k.String()); kind != "" {
			view.Preview, view.ViewURL = kind, objectURL("/objects/view", b.String(), k.String(), version)
			if kind == previewText {
				text, truncated, err := a.textPreview(r.Context(), b, k, pinnedVersion(v, object), object.Size)
				if err != nil {
					a.objectError(w, r, err)
					return
				}
				view.PreviewText, view.PreviewTruncated = text, truncated
			}
		}
		tags, tagErr := a.options.Storage.GetObjectTagging(r.Context(), b, k, &storage.ObjectTaggingOptions{VersionID: v})
		if tagErr != nil {
			a.objectError(w, r, tagErr)
			return
		}
		if tags == nil {
			tags = map[string]string{}
		}
		for key, value := range tags {
			view.TagList = append(view.TagList, keyValue{key, value})
		}
		sort.Slice(view.TagList, func(i, j int) bool { return view.TagList[i].Label < view.TagList[j].Label })
	}
	a.render(w, r, "Object details", "object-detail", view)
}

// describeObject fills the S3-console-style property sections.
func describeObject(view *objectDetailView, bucket, key string, object *storage.Object) {
	contentType := "-"
	if object.ContentType != nil && *object.ContentType != "" {
		contentType = *object.ContentType
	}
	version := "null"
	if object.VersionID != nil && *object.VersionID != "" {
		version = *object.VersionID
	}
	view.Overview = []keyValue{
		{"Bucket", bucket},
		{"Key", key},
		{"S3 URI", "s3://" + bucket + "/" + key},
		{"Last modified", formatModified(object.LastModified) + " UTC"},
		{"Size", fmt.Sprintf("%s (%s bytes)", humanBytes(float64(object.Size)), formatCount(float64(object.Size)))},
		{"Type", contentType},
		{"Entity tag (ETag)", strings.Trim(object.ETag, `"`)},
		{"Version ID", version},
	}
	view.StorageClass = storageClassLabel(object.StorageClass)
	for _, checksum := range []struct {
		name  string
		value *string
	}{{"CRC32", object.ChecksumCRC32}, {"CRC32C", object.ChecksumCRC32C}, {"CRC64NVME", object.ChecksumCRC64NVME}, {"SHA1", object.ChecksumSHA1}, {"SHA256", object.ChecksumSHA256}} {
		if checksum.value != nil && *checksum.value != "" {
			view.Checksums = append(view.Checksums, keyValue{checksum.name, *checksum.value})
		}
	}
	if object.ChecksumType != nil && *object.ChecksumType != "" && len(view.Checksums) > 0 {
		view.Checksums = append(view.Checksums, keyValue{"Checksum type", *object.ChecksumType})
	}
	view.Metadata = append(view.Metadata, keyValue{"Content-Type", contentType})
	for _, header := range []struct {
		name  string
		value *string
	}{{"Cache-Control", object.Metadata.CacheControl}, {"Content-Disposition", object.Metadata.ContentDisposition}, {"Content-Encoding", object.Metadata.ContentEncoding}, {"Content-Language", object.Metadata.ContentLanguage}, {"Expires", object.Metadata.Expires}, {"x-amz-website-redirect-location", object.Metadata.WebsiteRedirectLocation}} {
		if header.value != nil && *header.value != "" {
			view.Metadata = append(view.Metadata, keyValue{header.name, *header.value})
		}
	}
	userKeys := make([]string, 0, len(object.Metadata.UserMetadata))
	for name := range object.Metadata.UserMetadata {
		userKeys = append(userKeys, name)
	}
	sort.Strings(userKeys)
	for _, name := range userKeys {
		view.Metadata = append(view.Metadata, keyValue{"x-amz-meta-" + name, object.Metadata.UserMetadata[name]})
	}
	view.Retention, view.LegalHold = "Disabled", "Off"
	if retention := object.ObjectLock.Retention; retention != nil {
		view.Retention = fmt.Sprintf("%s until %s UTC", retention.Mode, retention.RetainUntilDate.UTC().Format("2006-01-02 15:04"))
	}
	if hold := object.ObjectLock.LegalHold; hold != nil && *hold == "ON" {
		view.LegalHold = "On"
	}
}

func (a *App) uploadPage(w http.ResponseWriter, r *http.Request) {
	bucket, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	a.render(w, r, "Upload", "object-upload", objectUploadView{
		CSRF: csrfToken(r), Bucket: bucket.String(), Prefix: prefix, Crumbs: prefixCrumbs(bucket.String(), prefix, false),
		BackURL: "/objects?" + url.Values{"bucket": {bucket.String()}, "prefix": {prefix}}.Encode(),
	})
}

func (a *App) download(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	object, readers, err := a.options.Storage.GetObject(r.Context(), b, k, nil, &storage.GetObjectOptions{VersionID: v})
	for _, reader := range readers {
		defer reader.Close()
	}
	if err != nil {
		a.objectError(w, r, err)
		return
	}
	if object == nil || len(readers) != 1 {
		a.fail(w, r, 500, "Invalid storage response")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(k.String())}))
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if object.Size >= 0 {
		w.Header().Set("Content-Length", fmt.Sprint(object.Size))
	}
	_, _ = io.Copy(w, readers[0])
}

type uploadLimitReader struct {
	reader    io.Reader
	remaining int64
}

func (r *uploadLimitReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		var one [1]byte
		n, err := r.reader.Read(one[:])
		if n > 0 {
			return 0, storage.ErrEntityTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}
func (a *App) upload(w http.ResponseWriter, r *http.Request) {
	reader, _ := r.Context().Value(uploadReaderKey{}).(*multipart.Reader)
	if reader == nil {
		var err error
		reader, err = r.MultipartReader()
		if err != nil {
			a.fail(w, r, 400, "Invalid multipart upload")
			return
		}
	}
	fields := make(map[string]string)
	for count := 0; count < 16; count++ {
		part, err := reader.NextPart()
		if err != nil {
			a.fail(w, r, 400, "Upload requires one file")
			return
		}
		name := part.FormName()
		if name != "file" {
			data, err := io.ReadAll(io.LimitReader(part, 65537))
			if err != nil || len(data) > 65536 {
				a.fail(w, r, 400, "Upload field too large")
				return
			}
			_ = part.Close()
			if name != "csrf_token" {
				if _, ok := fields[name]; ok {
					a.fail(w, r, 400, "Duplicate upload field")
					return
				}
				fields[name] = string(data)
			}
			continue
		}
		bucket, err := storage.NewBucketName(fields["bucket"])
		if err != nil {
			a.fail(w, r, 400, "Invalid bucket")
			return
		}
		keyName := fields["key"]
		if keyName == "" {
			keyName = part.FileName()
		}
		key, err := storage.NewObjectKey(keyName)
		if err != nil {
			a.fail(w, r, 400, "Invalid object key")
			return
		}
		contentType := part.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		// PutObject consumes the part directly. No ParseMultipartForm or temporary
		// files are used; storage computes size and checksums while reading.
		_, err = a.options.Storage.PutObject(r.Context(), bucket, key, &contentType, &uploadLimitReader{part, storage.MaxEntitySize}, nil, nil)
		if err != nil {
			a.objectError(w, r, err)
			return
		}
		a.audit(r, "AdminPutObject")
		http.Redirect(w, r, objectURL("/objects/detail", bucket.String(), key.String(), ""), http.StatusSeeOther)
		return
	}
	a.fail(w, r, 400, "Too many upload fields")
}
func (a *App) deleteObject(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	c := confirmation{Title: "Delete " + k.String() + "?", Message: "The object is deleted. In a versioned bucket a delete marker is created and earlier versions are kept.", Action: "Delete object"}
	if v != nil {
		c = confirmation{Title: "Permanently delete version " + *v + "?", Message: "This version of " + k.String() + " is deleted permanently. This cannot be undone.", Action: "Delete version"}
	}
	if !a.confirmed(w, r, c) {
		return
	}
	// Only allowlisted global admins reach this route, including the explicit
	// governance bypass capability. Compliance retention is never bypassed.
	bypass := r.FormValue("bypass_governance") == "yes"
	_, err = a.options.Storage.DeleteObject(r.Context(), b, k, &storage.DeleteObjectOptions{VersionID: v, BypassGovernanceRetention: bypass})
	if err != nil {
		a.objectError(w, r, err)
		return
	}
	if bypass {
		a.audit(r, "AdminBypassGovernanceRetention")
	}
	a.audit(r, "AdminDeleteObject")
	http.Redirect(w, r, objectURL("/objects", b.String(), "", ""), http.StatusSeeOther)
}
func (a *App) objectTags(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	var tags map[string]string
	if r.FormValue("editor") == "form" {
		tags, err = parseTagRows(r.Form)
		if err != nil {
			a.fail(w, r, 400, err.Error())
			return
		}
		if err := storage.ValidateTags(tags); err != nil {
			a.fail(w, r, 400, "Invalid object tags: "+err.Error())
			return
		}
	} else if json.Unmarshal([]byte(r.FormValue("tags")), &tags) != nil || storage.ValidateTags(tags) != nil {
		a.fail(w, r, 400, "Invalid object tags")
		return
	}
	if err := a.options.Storage.PutObjectTagging(r.Context(), b, k, tags, &storage.ObjectTaggingOptions{VersionID: v}); err != nil {
		a.objectError(w, r, err)
		return
	}
	a.audit(r, "AdminPutObjectTags")
	version := ""
	if v != nil {
		version = *v
	}
	http.Redirect(w, r, objectURL("/objects/detail", b.String(), k.String(), version), http.StatusSeeOther)
}
func (a *App) objectRetention(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	until, err := time.Parse(time.RFC3339, r.FormValue("retain_until"))
	if err != nil {
		a.fail(w, r, 400, "Retention date must be RFC3339")
		return
	}
	retention := &storage.ObjectRetention{Mode: storage.RetentionMode(r.FormValue("mode")), RetainUntilDate: until}
	if retention.Validate() != nil || !until.After(time.Now()) {
		a.fail(w, r, 400, "Invalid future retention")
		return
	}
	bypass := r.FormValue("bypass_governance") == "yes"
	if err := a.options.Storage.PutObjectRetention(r.Context(), b, k, retention, &storage.ObjectLockOptions{VersionID: v, BypassGovernanceRetention: bypass}); err != nil {
		a.objectError(w, r, err)
		return
	}
	if bypass {
		a.audit(r, "AdminBypassGovernanceRetention")
	}
	a.audit(r, "AdminPutObjectRetention")
	version := ""
	if v != nil {
		version = *v
	}
	http.Redirect(w, r, objectURL("/objects/detail", b.String(), k.String(), version), 303)
}
func (a *App) objectLegalHold(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	status := storage.LegalHoldStatus(r.FormValue("status"))
	if !status.Valid() {
		a.fail(w, r, 400, "Invalid legal hold")
		return
	}
	if err := a.options.Storage.PutObjectLegalHold(r.Context(), b, k, status, &storage.ObjectLockOptions{VersionID: v}); err != nil {
		a.objectError(w, r, err)
		return
	}
	a.audit(r, "AdminPutObjectLegalHold")
	version := ""
	if v != nil {
		version = *v
	}
	http.Redirect(w, r, objectURL("/objects/detail", b.String(), k.String(), version), 303)
}
