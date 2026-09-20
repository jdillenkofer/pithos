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

func taggingResponse(tags map[string]string) Tagging {
	response := Tagging{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", TagSet: make([]Tag, 0, len(tags))}
	for key, value := range tags {
		response.TagSet = append(response.TagSet, Tag{Key: key, Value: value})
	}
	return response
}

func (s *Server) getBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketTaggingHandler")
	defer span.End()
	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	if s.authorizeRequest(ctx, authorization.OperationGetBucketTagging, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}
	tags, err := s.storage.GetBucketTagging(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	writeXMLResponse(w, r, http.StatusOK, taggingResponse(tags))
}

func (s *Server) putBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketTaggingHandler")
	defer span.End()
	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	data, validationErr := readLimitedBody(r, w, maxPutObjectTaggingBodySize)
	var request Tagging
	malformed := validationErr == nil && xml.Unmarshal(data, &request) != nil
	var tags map[string]string
	if validationErr == nil && !malformed {
		tags, validationErr = tagSetToMap(&request)
	}
	if validationErr == nil && !malformed {
		validationErr = storage.ValidateBucketTags(tags)
	}
	if s.authorizeRequestWithRequestTags(ctx, authorization.OperationPutBucketTagging, ptrutils.ToPtr(bucketName.String()), nil, tags, w, r) {
		return
	}
	if malformed {
		writeMalformedXML(w, r)
		return
	}
	if validationErr != nil {
		handleError(validationErr, w, r)
		return
	}
	if err := s.storage.PutBucketTagging(ctx, bucketName, tags); err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketTaggingHandler")
	defer span.End()
	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	if s.authorizeRequest(ctx, authorization.OperationDeleteBucketTagging, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}
	if err := s.storage.DeleteBucketTagging(ctx, bucketName); err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	response := taggingResponse(tags)

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
