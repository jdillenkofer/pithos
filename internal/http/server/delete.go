package server

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteObjectHandler")
	defer span.End()
	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	key, err := storage.NewObjectKey(r.PathValue(keyPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	versionID := httputils.GetQueryParam(r.URL.Query(), versionIDQuery)
	authOperation := authorization.OperationDeleteObject
	if versionID != nil {
		authOperation = authorization.OperationDeleteObjectVersion
	}
	shouldReturn := s.authorizeRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Deleting object", "bucket", bucketName.String(), "key", key.String())
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	var deleteOpts *storage.DeleteObjectOptions
	if ifMatch != nil || versionID != nil {
		deleteOpts = &storage.DeleteObjectOptions{IfMatchETag: ifMatch, VersionID: versionID}
	}
	deleteResult, err := s.storage.DeleteObject(ctx, bucketName, key, deleteOpts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if deleteResult != nil {
		if deleteResult.VersionID != nil {
			w.Header().Set(versionIDHeader, *deleteResult.VersionID)
		}
		if deleteResult.IsDeleteMarker {
			w.Header().Set(deleteMarkerHeader, "true")
		}
	}
	w.WriteHeader(204)
}

func (s *Server) abortMultipartUploadOrDeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// AbortMultipartUpload
	if query.Has(uploadIdQuery) {
		s.abortMultipartUploadHandler(w, r)
		return
	}

	// DeleteObjectTagging
	if query.Has(taggingQuery) {
		s.deleteObjectTaggingHandler(w, r)
		return
	}

	// DeleteObject
	s.deleteObjectHandler(w, r)
}

func (s *Server) postBucketHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has("delete") {
		s.deleteObjectsHandler(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

const maxDeleteObjects = 1000

const maxCompleteMultipartUploadBodySize int64 = 10 * 1024 * 1024
const maxDeleteObjectsBodySize int64 = 5 * 1024 * 1024
const maxPutBucketWebsiteBodySize int64 = 128 * 1024
const maxPutBucketVersioningBodySize int64 = 16 * 1024
const maxPutBucketCORSBodySize int64 = 128 * 1024
const maxPutObjectTaggingBodySize int64 = 128 * 1024

// maxPutBucketLifecycleBodySize bounds the ?lifecycle request body. A
// configuration may hold up to 1000 rules, so the limit is more generous than
// for the other bucket subresources.
const maxPutBucketLifecycleBodySize int64 = 1024 * 1024

func readLimitedBody(r *http.Request, w http.ResponseWriter, maxBodySize int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		if _, ok := err.(*http.MaxBytesError); ok {
			return nil, storage.ErrEntityTooLarge
		}
		return nil, err
	}
	return data, nil
}

func (s *Server) deleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteObjectsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := readLimitedBody(r, w, maxDeleteObjectsBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var req DeleteObjectsRequest
	if err := xml.Unmarshal(data, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(req.Objects) > maxDeleteObjects {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result := DeleteObjectsResult{
		Deleted: []*DeletedEntry{},
		Errors:  []*DeleteErrorEntry{},
	}

	// First pass: validate entries and collect valid keys for bulk delete.
	// We track the original string key alongside the parsed ObjectKey so we can
	// build the response even when validation fails.
	type validEntry struct {
		rawKey    string
		key       storage.ObjectKey
		versionID *string
		etag      *string
	}
	validEntries := make([]validEntry, 0, len(req.Objects))
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationDeleteObjects, ptrutils.ToPtr(bucketName.String()), nil, r)

	for _, obj := range req.Objects {
		key, err := storage.NewObjectKey(obj.Key)
		if err != nil {
			result.Errors = append(result.Errors, &DeleteErrorEntry{
				Key:     obj.Key,
				Code:    "InvalidArgument",
				Message: err.Error(),
			})
			continue
		}

		validEntries = append(validEntries, validEntry{rawKey: obj.Key, key: key, versionID: obj.VersionId, etag: obj.ETag})
	}

	// Second pass: authorize valid entries and collect entries to bulk-delete.
	authorizedEntries := make([]validEntry, 0, len(validEntries))
	for _, ve := range validEntries {
		allowed, err := s.authorizeDeleteObjectEntry(ctx, baseRequest, ve.key.String())
		if err != nil {
			handleError(err, w, r)
			return
		}
		if !allowed {
			result.Errors = append(result.Errors, &DeleteErrorEntry{
				Key:     ve.rawKey,
				Code:    "AccessDenied",
				Message: "Access Denied",
			})
			continue
		}
		authorizedEntries = append(authorizedEntries, ve)
	}

	// Third pass: bulk delete all authorized entries in one storage call.
	if len(authorizedEntries) > 0 {
		inputEntries := make([]storage.DeleteObjectsInputEntry, len(authorizedEntries))
		for i, ve := range authorizedEntries {
			inputEntries[i] = storage.DeleteObjectsInputEntry{Key: ve.key, VersionID: ve.versionID, IfMatchETag: ve.etag}
		}

		slog.InfoContext(r.Context(), "DeleteObjects: deleting objects", "bucket", bucketName.String(), "count", len(inputEntries))
		deleteResult, err := s.storage.DeleteObjects(ctx, bucketName, inputEntries)
		if err != nil {
			if err == storage.ErrNoSuchBucket {
				handleError(err, w, r)
				return
			}
			handleError(err, w, r)
			return
		}

		// Build a map from key+versionId -> ordered entries for result lookup.
		// Multiple request entries can share the same key (and version id), so we
		// keep a slice and consume in request order.
		resultByKeyVersion := make(map[string][]storage.DeleteObjectsEntry, len(deleteResult.Entries))
		for _, entry := range deleteResult.Entries {
			k := entry.Key.String() + "\x00"
			if entry.VersionID != nil {
				k += *entry.VersionID
			}
			resultByKeyVersion[k] = append(resultByKeyVersion[k], entry)
		}

		for _, ve := range authorizedEntries {
			k := ve.key.String() + "\x00"
			if ve.versionID != nil {
				k += *ve.versionID
			}
			entriesForKey := resultByKeyVersion[k]
			entry := storage.DeleteObjectsEntry{Key: ve.key, Deleted: true}
			ok := len(entriesForKey) > 0
			if ok {
				entry = entriesForKey[0]
				resultByKeyVersion[k] = entriesForKey[1:]
			}
			if !ok || entry.Deleted {
				if !req.Quiet {
					result.Deleted = append(result.Deleted, &DeletedEntry{Key: ve.rawKey, VersionId: entry.VersionID, DeleteMarker: entry.DeleteMarker, DeleteMarkerVersionId: entry.DeleteMarkerVersionID})
				}
			} else {
				result.Errors = append(result.Errors, &DeleteErrorEntry{
					Key:       ve.rawKey,
					VersionId: entry.VersionID,
					Code:      entry.ErrCode,
					Message:   entry.ErrMsg,
				})
			}
		}
	}

	writeXMLResponse(w, r, http.StatusOK, result)
}
