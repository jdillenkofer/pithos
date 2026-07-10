package server

import (
	"encoding/xml"
	"net/http"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func writeMalformedXML(w http.ResponseWriter, r *http.Request) {
	writeS3ErrorResponse(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", r.URL.Path)
}

// tagSetToMap converts a parsed Tagging XML body into a map, rejecting duplicate
// keys with ErrInvalidTag.
func tagSetToMap(tagging *Tagging) (map[string]string, error) {
	tags := map[string]string{}
	for _, t := range tagging.TagSet {
		if _, exists := tags[t.Key]; exists {
			return nil, storage.ErrInvalidTag
		}
		tags[t.Key] = t.Value
	}
	return tags, nil
}

func (s *Server) getObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getObjectTaggingHandler")
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
	authOperation := authorization.OperationGetObjectTagging
	if versionID != nil {
		authOperation = authorization.OperationGetObjectVersionTagging
	}
	shouldReturn := s.authorizeRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	var opts *storage.ObjectTaggingOptions
	if versionID != nil {
		opts = &storage.ObjectTaggingOptions{VersionID: versionID}
	}
	tags, err := s.storage.GetObjectTagging(ctx, bucketName, key, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := Tagging{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: make([]Tag, 0, len(tags)),
	}
	for k, v := range tags {
		response.TagSet = append(response.TagSet, Tag{Key: k, Value: v})
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	if versionID != nil {
		responseHeaders.Set(versionIDHeader, *versionID)
	}
	writeXMLResponse(w, r, http.StatusOK, response)
}

func (s *Server) putObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putObjectTaggingHandler")
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

	// Parse and validate the tag set before authorizing so the tags being set are
	// available to the authorizer as request tags (s3:RequestObjectTag), but hold
	// any validation error back until after authorization: unauthorized callers
	// must get 401/403 (as AWS does), not validation details.
	var tags map[string]string
	var malformedXML bool
	data, validationErr := readLimitedBody(r, w, maxPutObjectTaggingBodySize)
	if validationErr == nil {
		var request Tagging
		if err := xml.Unmarshal(data, &request); err != nil {
			malformedXML = true
		} else if tags, validationErr = tagSetToMap(&request); validationErr == nil {
			validationErr = storage.ValidateTags(tags)
		}
	}
	if malformedXML || validationErr != nil {
		tags = nil
	}

	versionID := httputils.GetQueryParam(r.URL.Query(), versionIDQuery)
	authOperation := authorization.OperationPutObjectTagging
	if versionID != nil {
		authOperation = authorization.OperationPutObjectVersionTagging
	}
	shouldReturn := s.authorizeRequestWithRequestTags(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), tags, w, r)
	if shouldReturn {
		return
	}

	if malformedXML {
		writeMalformedXML(w, r)
		return
	}
	if validationErr != nil {
		handleError(validationErr, w, r)
		return
	}

	var opts *storage.ObjectTaggingOptions
	if versionID != nil {
		opts = &storage.ObjectTaggingOptions{VersionID: versionID}
	}
	err = s.storage.PutObjectTagging(ctx, bucketName, key, tags, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if versionID != nil {
		w.Header().Set(versionIDHeader, *versionID)
	}
	w.WriteHeader(200)
}

func (s *Server) deleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteObjectTaggingHandler")
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
	authOperation := authorization.OperationDeleteObjectTagging
	if versionID != nil {
		authOperation = authorization.OperationDeleteObjectVersionTagging
	}
	shouldReturn := s.authorizeRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	var opts *storage.ObjectTaggingOptions
	if versionID != nil {
		opts = &storage.ObjectTaggingOptions{VersionID: versionID}
	}
	err = s.storage.DeleteObjectTagging(ctx, bucketName, key, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if versionID != nil {
		w.Header().Set(versionIDHeader, *versionID)
	}
	w.WriteHeader(204)
}
