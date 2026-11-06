package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// Preview kinds an object can be shown as inside the console.
const (
	previewImage = "image"
	previewVideo = "video"
	previewAudio = "audio"
	previewText  = "text"
)

// textPreviewLimit bounds the text shown inline on the object page.
const textPreviewLimit = 64 * 1024

// Only formats browsers render without executing content are viewable. SVG,
// HTML, PDF and other active formats are never served inline as themselves:
// text-like types are served as text/plain, everything else is download-only.
var inlineMediaTypes = map[string]string{
	"image/png": previewImage, "image/jpeg": previewImage, "image/gif": previewImage, "image/webp": previewImage,
	"image/avif": previewImage, "image/bmp": previewImage, "image/x-icon": previewImage, "image/vnd.microsoft.icon": previewImage,
	"video/mp4": previewVideo, "video/webm": previewVideo, "video/ogg": previewVideo, "video/quicktime": previewVideo,
	"audio/mpeg": previewAudio, "audio/mp3": previewAudio, "audio/ogg": previewAudio, "audio/wav": previewAudio,
	"audio/x-wav": previewAudio, "audio/wave": previewAudio, "audio/webm": previewAudio, "audio/aac": previewAudio,
	"audio/flac": previewAudio, "audio/x-flac": previewAudio, "audio/mp4": previewAudio, "audio/x-m4a": previewAudio,
}

// extensionMediaTypes does not depend on the host's MIME tables, which are
// often missing in containers.
var extensionMediaTypes = map[string]string{
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp",
	".avif": "image/avif", ".bmp": "image/bmp", ".ico": "image/x-icon",
	".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm", ".ogv": "video/ogg", ".mov": "video/quicktime",
	".mp3": "audio/mpeg", ".oga": "audio/ogg", ".ogg": "audio/ogg", ".opus": "audio/ogg", ".wav": "audio/wav",
	".aac": "audio/aac", ".flac": "audio/flac", ".m4a": "audio/mp4",
}

var textMediaTypes = map[string]bool{
	"application/json": true, "application/xml": true, "application/yaml": true, "application/x-yaml": true,
	"application/toml": true, "application/javascript": true, "application/x-sh": true, "application/x-ndjson": true,
	"application/sql": true, "image/svg+xml": true,
}

var textExtensions = map[string]bool{
	".txt": true, ".md": true, ".markdown": true, ".log": true, ".csv": true, ".tsv": true, ".json": true, ".ndjson": true,
	".xml": true, ".yaml": true, ".yml": true, ".toml": true, ".ini": true, ".conf": true, ".cfg": true, ".env": true,
	".html": true, ".htm": true, ".css": true, ".js": true, ".ts": true, ".go": true, ".py": true, ".rs": true,
	".java": true, ".c": true, ".h": true, ".cpp": true, ".sh": true, ".sql": true, ".lua": true, ".svg": true,
}

// previewOf classifies an object and returns the media type to serve it with.
// The stored content type wins; generic binary types fall back to the key's
// extension, as browsers do for files.
func previewOf(contentType *string, key string) (kind, servedType string) {
	mediaType := ""
	if contentType != nil {
		mediaType, _, _ = mime.ParseMediaType(*contentType)
	}
	ext := strings.ToLower(path.Ext(key))
	if mediaType == "" || mediaType == "application/octet-stream" || mediaType == "binary/octet-stream" {
		if known, ok := extensionMediaTypes[ext]; ok {
			mediaType = known
		} else {
			mediaType, _, _ = mime.ParseMediaType(mime.TypeByExtension(ext))
		}
	}
	if kind, ok := inlineMediaTypes[mediaType]; ok {
		return kind, mediaType
	}
	if strings.HasPrefix(mediaType, "text/") || textMediaTypes[mediaType] || textExtensions[ext] {
		return previewText, "text/plain; charset=utf-8"
	}
	return "", ""
}

// objectRangeReader is an io.ReadSeeker over a stored object, so that
// http.ServeContent can answer Range requests. Seeking is free; each read
// after a seek opens one ranged GetObject that streams to the end.
type objectRangeReader struct {
	ctx     context.Context
	store   storage.Storage
	bucket  storage.BucketName
	key     storage.ObjectKey
	version *string
	size    int64
	offset  int64
	body    io.ReadCloser
}

func (r *objectRangeReader) Seek(offset int64, whence int) (int64, error) {
	next := offset
	switch whence {
	case io.SeekCurrent:
		next += r.offset
	case io.SeekEnd:
		next += r.size
	}
	if next < 0 {
		return 0, errors.New("seek before start of object")
	}
	if next != r.offset {
		r.closeBody()
	}
	r.offset = next
	return next, nil
}

func (r *objectRangeReader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
	if r.body == nil {
		start := r.offset
		_, readers, err := r.store.GetObject(r.ctx, r.bucket, r.key, []storage.ByteRange{{Start: &start}}, &storage.GetObjectOptions{VersionID: r.version})
		if err != nil || len(readers) != 1 {
			for _, reader := range readers {
				_ = reader.Close()
			}
			if err == nil {
				err = errors.New("invalid storage response")
			}
			return 0, err
		}
		r.body = readers[0]
	}
	n, err := r.body.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *objectRangeReader) closeBody() {
	if r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
}

// pinnedVersion reads later ranges from the version that was inspected, so
// a concurrent overwrite cannot mix two objects in one response.
func pinnedVersion(requested *string, object *storage.Object) *string {
	if requested != nil || object.VersionID == nil || *object.VersionID == "" || *object.VersionID == "null" {
		return requested
	}
	return object.VersionID
}

// view serves an object inline for the browser's own image, media or text
// viewer. The response is sandboxed and never served as an active type.
func (a *App) view(w http.ResponseWriter, r *http.Request) {
	b, k, v, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	object, err := a.options.Storage.HeadObject(r.Context(), b, k, &storage.HeadObjectOptions{VersionID: v})
	if err != nil {
		a.objectError(w, r, err)
		return
	}
	kind, servedType := previewOf(object.ContentType, k.String())
	if kind == "" {
		a.fail(w, r, http.StatusUnsupportedMediaType, "This object type cannot be viewed in the browser. Download it instead.")
		return
	}
	reader := &objectRangeReader{ctx: r.Context(), store: a.options.Storage, bucket: b, key: k, version: pinnedVersion(v, object), size: object.Size}
	defer reader.closeBody()
	w.Header().Set("Content-Type", servedType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": path.Base(k.String())}))
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'; img-src 'self'; media-src 'self'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if object.ETag != "" {
		w.Header().Set("ETag", object.ETag)
	}
	http.ServeContent(w, r, "", object.LastModified, reader)
}

// textPreview returns the beginning of a text object for inline display.
func (a *App) textPreview(ctx context.Context, b storage.BucketName, k storage.ObjectKey, version *string, size int64) (string, bool, error) {
	if size <= 0 {
		return "", false, nil
	}
	start, end := int64(0), min(size, textPreviewLimit+1)
	_, readers, err := a.options.Storage.GetObject(ctx, b, k, []storage.ByteRange{{Start: &start, End: &end}}, &storage.GetObjectOptions{VersionID: version})
	for _, reader := range readers {
		defer reader.Close()
	}
	if err != nil {
		return "", false, err
	}
	if len(readers) != 1 {
		return "", false, fmt.Errorf("invalid storage response")
	}
	data, err := io.ReadAll(io.LimitReader(readers[0], textPreviewLimit+1))
	if err != nil {
		return "", false, err
	}
	truncated := len(data) > textPreviewLimit
	if truncated {
		data = data[:textPreviewLimit]
		// Drop a multi-byte character cut in half by the limit.
		for i := 0; i < utf8.UTFMax-1 && len(data) > 0; i++ {
			if r, _ := utf8.DecodeLastRune(data); r != utf8.RuneError {
				break
			}
			data = data[:len(data)-1]
		}
	}
	return strings.ToValidUTF8(string(data), "\uFFFD"), truncated, nil
}
