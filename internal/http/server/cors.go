package server

import (
	"encoding/xml"
	"net/http"
	"strings"

	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) getBucketCORSHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketCORSHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetBucketCORS, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	config, err := s.storage.GetBucketCORSConfiguration(ctx, bucketName)
	if err != nil {
		if err == storage.ErrNoSuchCORSConfiguration {
			writeNoSuchCORSConfiguration(w, r)
			return
		}
		handleError(err, w, r)
		return
	}

	response := CORSConfiguration{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Rules: make([]CORSConfigurationRule, 0, len(config.Rules)),
	}
	for _, rule := range config.Rules {
		response.Rules = append(response.Rules, CORSConfigurationRule{
			ID:             rule.ID,
			AllowedOrigins: rule.AllowedOrigins,
			AllowedMethods: rule.AllowedMethods,
			AllowedHeaders: rule.AllowedHeaders,
			ExposeHeaders:  rule.ExposeHeaders,
			MaxAgeSeconds:  rule.MaxAgeSeconds,
		})
	}

	writeXMLResponse(w, r, http.StatusOK, response)
}

func (s *Server) putBucketCORSHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketCORSHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutBucketCORS, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := readLimitedBody(r, w, maxPutBucketCORSBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request CORSConfiguration
	err = xml.Unmarshal(data, &request)
	if err != nil {
		handleError(err, w, r)
		return
	}

	rules := make([]storage.CORSRule, 0, len(request.Rules))
	for _, rule := range request.Rules {
		rules = append(rules, storage.CORSRule{
			ID:             rule.ID,
			AllowedOrigins: rule.AllowedOrigins,
			AllowedMethods: rule.AllowedMethods,
			AllowedHeaders: rule.AllowedHeaders,
			ExposeHeaders:  rule.ExposeHeaders,
			MaxAgeSeconds:  rule.MaxAgeSeconds,
		})
	}

	normalizedRules, err := httpmiddleware.NormalizeAndValidateCORSRules(rules)
	if err != nil {
		writeInvalidRequest(w, r, err.Error())
		return
	}

	err = s.storage.PutBucketCORSConfiguration(ctx, bucketName, &storage.BucketCORSConfiguration{Rules: normalizedRules})
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteBucketCORSHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketCORSHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucketCORS, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	err = s.storage.DeleteBucketCORSConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) optionsBucketHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *Server) optionsObjectHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *Server) resolveCORSRulesForRequest(r *http.Request) []httpmiddleware.CORSRule {
	bucket, ok := bucketFromPath(r.URL.Path)
	if !ok {
		return nil
	}
	bucketName, err := storage.NewBucketName(bucket)
	if err != nil {
		return nil
	}
	config, err := s.storage.GetBucketCORSConfiguration(r.Context(), bucketName)
	if err != nil {
		return nil
	}
	return config.Rules
}

func bucketFromPath(requestPath string) (string, bool) {
	trimmedPath := strings.TrimPrefix(requestPath, "/")
	if trimmedPath == "" {
		return "", false
	}
	pathSegments := strings.SplitN(trimmedPath, "/", 2)
	bucket := strings.TrimSpace(pathSegments[0])
	if bucket == "" {
		return "", false
	}
	return bucket, true
}
