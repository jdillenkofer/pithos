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
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
)

type objectCursor struct {
	Bucket, Prefix, Key, Version string
	Versions                     bool
}
type objectLink struct {
	Label, URL, Version, SizeText, Modified string
	Size                                    int64
	DeleteMarker, Latest                    bool
}
type objectsView struct {
	CSRF, Bucket, Prefix, Parent, NextURL, VersionsURL string
	Versions                                           bool
	Prefixes, Objects, Crumbs                          []objectLink
}

func formatModified(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04")
}

type objectDetailView struct {
	CSRF, Bucket, Key, Version, JSON, Tags, DownloadURL, BucketURL string
	DeleteMarker                                                   bool
	Properties                                                     []usageTile
}

// objectError exposes only known protocol errors, never backend connection
// strings, remote URLs, or other provider details embedded in arbitrary errors.
func (a *App) objectError(w http.ResponseWriter, r *http.Request, err error) {
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
	a.fail(w, r, status, message)
}

func (a *App) registerObjects(mux *http.ServeMux) {
	mux.HandleFunc("GET /objects", a.objects)
	mux.HandleFunc("GET /objects/detail", a.objectDetail)
	mux.HandleFunc("GET /objects/download", a.download)
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
	// Breadcrumbs link every enclosing prefix of the current listing.
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == '/' {
			q.Set("prefix", prefix[:i+1])
			start := strings.LastIndex(prefix[:i], "/") + 1
			view.Crumbs = append(view.Crumbs, objectLink{Label: prefix[start : i+1], URL: "/objects?" + q.Encode()})
		}
	}
	q.Set("prefix", prefix)
	q.Set("versions", fmt.Sprint(!versions))
	view.VersionsURL = "/objects?" + q.Encode()
	q.Set("versions", fmt.Sprint(versions))
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
			view.Objects = append(view.Objects, objectLink{Label: strings.TrimPrefix(o.Key.String(), prefix), Version: o.VersionID, URL: objectURL("/objects/detail", bucket.String(), o.Key.String(), o.VersionID), Size: o.Size, SizeText: humanBytes(float64(o.Size)), Modified: formatModified(o.LastModified), DeleteMarker: o.IsDeleteMarker, Latest: o.IsLatest})
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
			view.Objects = append(view.Objects, objectLink{Label: strings.TrimPrefix(o.Key.String(), prefix), URL: objectURL("/objects/detail", bucket.String(), o.Key.String(), ""), Size: o.Size, SizeText: humanBytes(float64(o.Size)), Modified: formatModified(o.LastModified)})
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
		view.Prefixes = append(view.Prefixes, objectLink{Label: strings.TrimPrefix(p, prefix), URL: "/objects?" + q.Encode()})
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
	view := objectDetailView{CSRF: csrfToken(r), Bucket: b.String(), Key: k.String(), Version: version, DownloadURL: objectURL("/objects/download", b.String(), k.String(), version), BucketURL: "/objects?" + url.Values{"bucket": {b.String()}}.Encode()}
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
		raw, _ := json.MarshalIndent(object, "", "  ")
		view.JSON = string(raw)
		contentType, storageClass := "—", "STANDARD"
		if object.ContentType != nil && *object.ContentType != "" {
			contentType = *object.ContentType
		}
		if object.StorageClass != nil && *object.StorageClass != "" {
			storageClass = *object.StorageClass
		}
		view.Properties = []usageTile{
			{"Size", fmt.Sprintf("%s (%d bytes)", humanBytes(float64(object.Size)), object.Size)},
			{"Content type", contentType},
			{"Last modified", formatModified(object.LastModified) + " UTC"},
			{"ETag", object.ETag},
			{"Storage class", storageClass},
		}
		if object.VersionID != nil && *object.VersionID != "" {
			view.Properties = append(view.Properties, usageTile{"Version ID", *object.VersionID})
		}
		tags, tagErr := a.options.Storage.GetObjectTagging(r.Context(), b, k, &storage.ObjectTaggingOptions{VersionID: v})
		if tagErr != nil {
			a.objectError(w, r, tagErr)
			return
		}
		if tags == nil {
			tags = map[string]string{}
		}
		raw, _ = json.MarshalIndent(tags, "", "  ")
		view.Tags = string(raw)
	}
	a.render(w, r, "Object details", "object-detail", view)
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
	if r.FormValue("confirm") != "yes" {
		a.fail(w, r, 400, "Deletion requires confirmation")
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
	if json.Unmarshal([]byte(r.FormValue("tags")), &tags) != nil || storage.ValidateTags(tags) != nil {
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
