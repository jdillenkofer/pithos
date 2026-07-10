package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) getBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Getting bucket website configuration", "bucket", bucketName.String())
	config, err := s.storage.GetBucketWebsiteConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := websiteConfigurationResponseFromStorage(config)

	writeXMLResponse(w, r, http.StatusOK, response)
}

func websiteConfigurationResponseFromStorage(config *storage.WebsiteConfiguration) WebsiteConfigurationResponse {
	response := WebsiteConfigurationResponse{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}
	if config.IndexDocumentSuffix != "" {
		response.IndexDocument = &WebsiteConfigurationIndexDocument{
			Suffix: config.IndexDocumentSuffix,
		}
	}
	if config.ErrorDocumentKey != nil {
		response.ErrorDocument = &WebsiteConfigurationErrorDocument{
			Key: *config.ErrorDocumentKey,
		}
	}
	if config.RedirectAllRequestsTo != nil {
		response.RedirectAllRequestsTo = &WebsiteConfigurationRedirectAllRequestsTo{
			HostName: config.RedirectAllRequestsTo.HostName,
		}
		if config.RedirectAllRequestsTo.Protocol != nil {
			response.RedirectAllRequestsTo.Protocol = *config.RedirectAllRequestsTo.Protocol
		}
	}
	for _, rule := range config.RoutingRules {
		responseRule := WebsiteConfigurationRoutingRule{
			Redirect: &WebsiteConfigurationRedirect{
				HostName:             rule.Redirect.HostName,
				Protocol:             rule.Redirect.Protocol,
				ReplaceKeyPrefixWith: rule.Redirect.ReplaceKeyPrefixWith,
				ReplaceKeyWith:       rule.Redirect.ReplaceKeyWith,
				HttpRedirectCode:     rule.Redirect.HttpRedirectCode,
			},
		}
		if rule.Condition != nil {
			responseRule.Condition = &WebsiteConfigurationRoutingRuleCondition{
				KeyPrefixEquals:             rule.Condition.KeyPrefixEquals,
				HttpErrorCodeReturnedEquals: rule.Condition.HttpErrorCodeReturnedEquals,
			}
		}
		response.RoutingRules = append(response.RoutingRules, responseRule)
	}
	return response
}

func validateWebsiteProtocol(protocol *string) bool {
	if protocol == nil || *protocol == "" {
		return true
	}
	return *protocol == "http" || *protocol == "https"
}

func normalizeRedirectCode(code *string) (*string, bool) {
	if code == nil || *code == "" {
		return ptrutils.ToPtr(strconv.Itoa(http.StatusMovedPermanently)), true
	}
	switch *code {
	case "301", "302", "303", "307", "308":
		return code, true
	default:
		return nil, false
	}
}

func websiteConfigurationRequestToStorage(request WebsiteConfigurationRequest) (*storage.WebsiteConfiguration, error) {
	if request.RedirectAllRequestsTo != nil {
		if request.IndexDocument != nil || request.ErrorDocument != nil || len(request.RoutingRules) > 0 {
			return nil, ErrInvalidArgument
		}
		if request.RedirectAllRequestsTo.HostName == "" {
			return nil, ErrInvalidArgument
		}
		var protocol *string
		if request.RedirectAllRequestsTo.Protocol != "" {
			protocol = &request.RedirectAllRequestsTo.Protocol
		}
		if !validateWebsiteProtocol(protocol) {
			return nil, ErrInvalidArgument
		}
		return &storage.WebsiteConfiguration{
			RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{
				HostName: request.RedirectAllRequestsTo.HostName,
				Protocol: protocol,
			},
		}, nil
	}

	if request.IndexDocument == nil || request.IndexDocument.Suffix == "" {
		return nil, ErrInvalidArgument
	}

	config := &storage.WebsiteConfiguration{
		IndexDocumentSuffix: request.IndexDocument.Suffix,
	}
	if request.ErrorDocument != nil && request.ErrorDocument.Key != "" {
		config.ErrorDocumentKey = &request.ErrorDocument.Key
	}

	for _, rule := range request.RoutingRules {
		if rule.Redirect == nil {
			return nil, ErrInvalidArgument
		}
		redirectCode, ok := normalizeRedirectCode(rule.Redirect.HttpRedirectCode)
		if !ok || !validateWebsiteProtocol(rule.Redirect.Protocol) {
			return nil, ErrInvalidArgument
		}
		if rule.Redirect.ReplaceKeyWith != nil && rule.Redirect.ReplaceKeyPrefixWith != nil {
			return nil, ErrInvalidArgument
		}
		if rule.Redirect.HostName == nil && rule.Redirect.Protocol == nil && rule.Redirect.ReplaceKeyWith == nil && rule.Redirect.ReplaceKeyPrefixWith == nil {
			return nil, ErrInvalidArgument
		}
		if rule.Condition != nil && rule.Condition.KeyPrefixEquals == nil && rule.Condition.HttpErrorCodeReturnedEquals == nil {
			return nil, ErrInvalidArgument
		}

		storageRule := storage.WebsiteRoutingRule{
			Redirect: storage.WebsiteRedirect{
				HostName:             rule.Redirect.HostName,
				Protocol:             rule.Redirect.Protocol,
				ReplaceKeyPrefixWith: rule.Redirect.ReplaceKeyPrefixWith,
				ReplaceKeyWith:       rule.Redirect.ReplaceKeyWith,
				HttpRedirectCode:     redirectCode,
			},
		}
		if rule.Condition != nil {
			storageRule.Condition = &storage.WebsiteRoutingRuleCondition{
				KeyPrefixEquals:             rule.Condition.KeyPrefixEquals,
				HttpErrorCodeReturnedEquals: rule.Condition.HttpErrorCodeReturnedEquals,
			}
		}
		config.RoutingRules = append(config.RoutingRules, storageRule)
	}

	return config, nil
}

func (s *Server) putBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := readLimitedBody(r, w, maxPutBucketWebsiteBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request WebsiteConfigurationRequest
	err = xml.Unmarshal(data, &request)
	if err != nil {
		handleError(err, w, r)
		return
	}

	config, err := websiteConfigurationRequestToStorage(request)
	if err != nil {
		handleError(err, w, r)
		return
	}

	slog.InfoContext(r.Context(), "Putting bucket website configuration", "bucket", bucketName.String())
	err = s.storage.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Deleting bucket website configuration", "bucket", bucketName.String())
	err = s.storage.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

// websiteResolveKey resolves the object key for a website request,
// appending the index document suffix when the key is empty or ends with "/".
func websiteResolveKey(keyStr string, websiteConfig *storage.WebsiteConfiguration) string {
	if keyStr == "" || strings.HasSuffix(keyStr, "/") {
		return keyStr + websiteConfig.IndexDocumentSuffix
	}
	return keyStr
}

func websiteFindRoutingRule(config *storage.WebsiteConfiguration, requestKey string, errorStatusCode *int) *storage.WebsiteRoutingRule {
	if config == nil {
		return nil
	}
	for i := range config.RoutingRules {
		rule := &config.RoutingRules[i]
		if rule.Condition == nil {
			return rule
		}
		if rule.Condition.KeyPrefixEquals != nil && !strings.HasPrefix(requestKey, *rule.Condition.KeyPrefixEquals) {
			continue
		}
		if rule.Condition.HttpErrorCodeReturnedEquals != nil {
			if errorStatusCode == nil || *rule.Condition.HttpErrorCodeReturnedEquals != strconv.Itoa(*errorStatusCode) {
				continue
			}
		}
		return rule
	}
	return nil
}

func websiteRedirectLocation(r *http.Request, requestKey string, rule *storage.WebsiteRoutingRule) string {
	redirect := rule.Redirect
	targetKey := requestKey
	if redirect.ReplaceKeyWith != nil {
		targetKey = *redirect.ReplaceKeyWith
	} else if redirect.ReplaceKeyPrefixWith != nil {
		targetKey = *redirect.ReplaceKeyPrefixWith
		if rule.Condition != nil && rule.Condition.KeyPrefixEquals != nil {
			targetKey += strings.TrimPrefix(requestKey, *rule.Condition.KeyPrefixEquals)
		}
	}

	if redirect.HostName == nil && redirect.Protocol == nil {
		if strings.HasPrefix(targetKey, "/") {
			return targetKey
		}
		return "/" + targetKey
	}

	scheme := "http"
	if redirect.Protocol != nil && *redirect.Protocol != "" {
		scheme = *redirect.Protocol
	} else if r.URL.Scheme != "" {
		scheme = r.URL.Scheme
	}
	host := r.Host
	if redirect.HostName != nil && *redirect.HostName != "" {
		host = *redirect.HostName
	}
	u := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   "/" + strings.TrimPrefix(targetKey, "/"),
	}
	return u.String()
}

func websiteRedirectAllLocation(r *http.Request, requestKey string, redirect *storage.WebsiteRedirectAllRequestsTo) string {
	scheme := "http"
	if redirect.Protocol != nil && *redirect.Protocol != "" {
		scheme = *redirect.Protocol
	} else if r.URL.Scheme != "" {
		scheme = r.URL.Scheme
	}
	u := url.URL{
		Scheme:   scheme,
		Host:     redirect.HostName,
		Path:     "/" + strings.TrimPrefix(requestKey, "/"),
		RawQuery: r.URL.RawQuery,
	}
	return u.String()
}

func writeWebsiteRedirect(w http.ResponseWriter, location string, code *string) {
	statusCode := http.StatusMovedPermanently
	if code != nil {
		if parsedCode, err := strconv.Atoi(*code); err == nil {
			statusCode = parsedCode
		}
	}
	w.Header().Set(locationHeader, location)
	w.WriteHeader(statusCode)
}

func websiteDirectoryRedirectLocation(r *http.Request, requestKey string) string {
	u := url.URL{
		Path:     "/" + strings.TrimPrefix(requestKey, "/") + "/",
		RawQuery: r.URL.RawQuery,
	}
	return u.String()
}

func (s *Server) tryWebsiteDirectoryRedirect(ctx context.Context, w http.ResponseWriter, r *http.Request, bucketName storage.BucketName, websiteConfig *storage.WebsiteConfiguration, requestKey string) bool {
	if requestKey == "" || strings.HasSuffix(requestKey, "/") || websiteConfig.IndexDocumentSuffix == "" {
		return false
	}

	indexKey, err := storage.NewObjectKey(requestKey + "/" + websiteConfig.IndexDocumentSuffix)
	if err != nil {
		return false
	}
	if _, err := s.storage.HeadObject(ctx, bucketName, indexKey, nil); err != nil {
		return false
	}

	statusCode := strconv.Itoa(http.StatusFound)
	writeWebsiteRedirect(w, websiteDirectoryRedirectLocation(r, requestKey), &statusCode)
	return true
}

// websitePrepare fetches the website configuration, resolves the request key,
// and authorizes the request. It writes an error response and returns false if
// any step fails; on success it returns the resolved config, object key, and
// resolved key string.
func (s *Server) websitePrepare(ctx context.Context, w http.ResponseWriter, r *http.Request, operation string, bucketName storage.BucketName) (websiteConfig *storage.WebsiteConfiguration, objectKey storage.ObjectKey, resolvedKey string, ok bool) {
	websiteConfig, configErr := s.storage.GetBucketWebsiteConfiguration(ctx, bucketName)

	// Resolve key and authorize with it when possible, fall back to nil key
	// when the config is unavailable (key resolution requires the index suffix).
	// Auth always runs before any existence information is revealed.
	var keyStr *string
	if configErr == nil {
		resolvedKey = websiteResolveKey(r.PathValue(keyPath), websiteConfig)
		if k, err := storage.NewObjectKey(resolvedKey); err == nil {
			objectKey = k
			s := k.String()
			keyStr = &s
		}
	}

	isAuthenticated, _ := ctx.Value(authentication.IsAuthenticatedContextKey{}).(bool)
	var accessKeyId *string
	if isAuthenticated {
		keyIdStr, _ := ctx.Value(authentication.AccessKeyIdContextKey{}).(string)
		accessKeyId = &keyIdStr
	}

	bucketStr := bucketName.String()
	authRequest := &authorization.Request{
		Operation:     operation,
		Authorization: authorization.Authorization{AccessKeyId: accessKeyId},
		Bucket:        &bucketStr,
		Key:           keyStr,
		HttpRequest:   makeAuthorizationHTTPRequest(r),
	}
	allowed, err := s.requestAuthorizer.AuthorizeRequest(ctx, authRequest)
	if err != nil {
		writePlainError(w, http.StatusInternalServerError)
		return nil, storage.ObjectKey{}, "", false
	}
	if !allowed {
		if !isAuthenticated {
			writePlainError(w, http.StatusUnauthorized)
		} else {
			writePlainError(w, http.StatusForbidden)
		}
		return nil, storage.ObjectKey{}, "", false
	}

	// Auth passed — now it is safe to reveal specific error codes.
	if configErr != nil {
		if configErr == storage.ErrNoSuchWebsiteConfiguration || configErr == storage.ErrNoSuchBucket {
			writePlainError(w, http.StatusNotFound)
		} else {
			writePlainError(w, http.StatusInternalServerError)
		}
		return nil, storage.ObjectKey{}, "", false
	}

	if keyStr == nil && websiteConfig.RedirectAllRequestsTo == nil {
		s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
			fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
		return nil, storage.ObjectKey{}, "", false
	}

	return websiteConfig, objectKey, resolvedKey, true
}

func (s *Server) serveWebsiteGetObject(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.serveWebsiteGetObject")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		writePlainError(w, http.StatusBadRequest)
		return
	}

	websiteConfig, objectKey, resolvedKey, ok := s.websitePrepare(ctx, w, r, authorization.OperationGetObject, bucketName)
	if !ok {
		return
	}
	requestKey := r.PathValue(keyPath)
	if websiteConfig.RedirectAllRequestsTo != nil {
		writeWebsiteRedirect(w, websiteRedirectAllLocation(r, requestKey, websiteConfig.RedirectAllRequestsTo), nil)
		return
	}
	if rule := websiteFindRoutingRule(websiteConfig, requestKey, nil); rule != nil {
		writeWebsiteRedirect(w, websiteRedirectLocation(r, requestKey, rule), rule.Redirect.HttpRedirectCode)
		return
	}

	slog.InfoContext(r.Context(), "Website: getting object", "bucket", bucketName.String(), "key", resolvedKey)

	object, readers, err := s.storage.GetObject(ctx, bucketName, objectKey, nil, nil)
	if err != nil {
		if err == storage.ErrNoSuchKey {
			if s.tryWebsiteDirectoryRedirect(ctx, w, r, bucketName, websiteConfig, requestKey) {
				return
			}
			statusCode := http.StatusNotFound
			if rule := websiteFindRoutingRule(websiteConfig, requestKey, &statusCode); rule != nil {
				writeWebsiteRedirect(w, websiteRedirectLocation(r, requestKey, rule), rule.Redirect.HttpRedirectCode)
				return
			}
			s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
				fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
			return
		}
		writePlainError(w, http.StatusInternalServerError)
		return
	}

	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
	}()

	// Objects with x-amz-website-redirect-location are served as a 301 redirect
	// on the website endpoint, matching S3 static website hosting.
	if object.Metadata.WebsiteRedirectLocation != nil {
		w.Header().Set(locationHeader, *object.Metadata.WebsiteRedirectLocation)
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}

	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	setMetadataHeadersFromObject(responseHeaders, object)
	responseHeaders.Set(etagHeader, fmt.Sprintf("\"%s\"", object.ETag))
	responseHeaders.Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(http.StatusOK)

	writer := ioutils.NewTracingWriter(ctx, s.tracer, "WebsiteGetObject", w)
	if _, err := ioutils.CopyN(writer, readers[0], object.Size); err != nil {
		slog.DebugContext(ctx, "Stopped website object response copy", "error", err)
		return
	}
}

func (s *Server) serveWebsiteHeadObject(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.serveWebsiteHeadObject")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		writePlainError(w, http.StatusBadRequest)
		return
	}

	websiteConfig, objectKey, resolvedKey, ok := s.websitePrepare(ctx, w, r, authorization.OperationHeadObject, bucketName)
	if !ok {
		return
	}
	requestKey := r.PathValue(keyPath)
	if websiteConfig.RedirectAllRequestsTo != nil {
		writeWebsiteRedirect(w, websiteRedirectAllLocation(r, requestKey, websiteConfig.RedirectAllRequestsTo), nil)
		return
	}
	if rule := websiteFindRoutingRule(websiteConfig, requestKey, nil); rule != nil {
		writeWebsiteRedirect(w, websiteRedirectLocation(r, requestKey, rule), rule.Redirect.HttpRedirectCode)
		return
	}

	slog.InfoContext(r.Context(), "Website: head object", "bucket", bucketName.String(), "key", resolvedKey)

	object, err := s.storage.HeadObject(ctx, bucketName, objectKey, nil)
	if err != nil {
		if err == storage.ErrNoSuchKey {
			if s.tryWebsiteDirectoryRedirect(ctx, w, r, bucketName, websiteConfig, requestKey) {
				return
			}
			statusCode := http.StatusNotFound
			if rule := websiteFindRoutingRule(websiteConfig, requestKey, &statusCode); rule != nil {
				writeWebsiteRedirect(w, websiteRedirectLocation(r, requestKey, rule), rule.Redirect.HttpRedirectCode)
				return
			}
			s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
				fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
			return
		}
		writePlainError(w, http.StatusInternalServerError)
		return
	}

	// Objects with x-amz-website-redirect-location are served as a 301 redirect
	// on the website endpoint, matching S3 static website hosting.
	if object.Metadata.WebsiteRedirectLocation != nil {
		w.Header().Set(locationHeader, *object.Metadata.WebsiteRedirectLocation)
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}

	responseHeaders := w.Header()
	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}
	responseHeaders.Set(contentTypeHeader, contentType)
	setMetadataHeadersFromObject(responseHeaders, object)
	responseHeaders.Set(etagHeader, fmt.Sprintf("\"%s\"", object.ETag))
	gmtTimeLoc := time.FixedZone("GMT", 0)
	responseHeaders.Set(lastModifiedHeader, object.LastModified.In(gmtTimeLoc).Format(time.RFC1123))
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(http.StatusOK)
}

// serveErrorDocument tries to serve the configured error document for the bucket.
// If no error document is configured or the error document itself cannot be found,
// it falls back to a default HTML error page.
func (s *Server) serveErrorDocument(w http.ResponseWriter, r *http.Request,
	bucketName storage.BucketName, config *storage.WebsiteConfiguration,
	statusCode int, code string, message string) {

	if config.ErrorDocumentKey == nil {
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	ctx := r.Context()
	errorKey, err := storage.NewObjectKey(*config.ErrorDocumentKey)
	if err != nil {
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	object, readers, err := s.storage.GetObject(ctx, bucketName, errorKey, nil, nil)
	if err != nil {
		// Error document not found — fall back to default HTML error
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
	}()

	contentType := "text/html"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(statusCode)

	if r.Method == http.MethodHead {
		return
	}

	if _, err := io.Copy(w, readers[0]); err != nil {
		slog.DebugContext(ctx, "Stopped website error document response copy", "error", err)
		return
	}
}

// writePlainError writes a plain-text error response for the website endpoint.
// Using a body prevents browsers (notably Safari) from treating bodyless
// responses as file downloads.
func writePlainError(w http.ResponseWriter, statusCode int) {
	w.Header().Set(contentTypeHeader, "text/plain; charset=utf-8")
	body := fmt.Sprintf("%d %s\n", statusCode, http.StatusText(statusCode))
	w.Header().Set(contentLengthHeader, fmt.Sprintf("%d", len(body)))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

// writeHTMLError writes a simple HTML error response, similar to how AWS S3
// website hosting returns errors.
func (s *Server) writeHTMLError(w http.ResponseWriter, statusCode int, code string, message string) {
	w.Header().Set(contentTypeHeader, "text/html; charset=utf-8")
	w.WriteHeader(statusCode)
	body := fmt.Sprintf(`<html>
<head><title>%d %s</title></head>
<body>
<h1>%d %s</h1>
<ul>
<li>Code: %s</li>
<li>Message: %s</li>
</ul>
</body>
</html>`, statusCode, http.StatusText(statusCode), statusCode, http.StatusText(statusCode), html.EscapeString(code), html.EscapeString(message))
	w.Write([]byte(body))
}
