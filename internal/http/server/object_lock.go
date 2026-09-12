package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"io"
	"net/http"
	"time"
)

func (s *Server) recordAuthorizationDenied(ctx context.Context, request *authorization.Request) {
	recorder, ok := s.storage.(auditlog.AuthorizationDenialRecorder)
	if !ok {
		return
	}
	resource := auditlog.ResourceDetails{}
	if request.Bucket != nil {
		resource.Bucket = *request.Bucket
	}
	if request.Key != nil {
		resource.Key = *request.Key
	}
	if request.VersionID != nil {
		resource.VersionID = *request.VersionID
	}
	values := auditlog.ObjectLockValues{Days: request.ObjectLockDays, Years: request.ObjectLockYears}
	if request.ObjectLockEnabled != nil {
		values.Enabled = *request.ObjectLockEnabled
	}
	if request.ObjectLockMode != nil {
		values.Mode = *request.ObjectLockMode
	}
	if request.ObjectLockRetainUntilDate != nil {
		values.RetainUntilDate = *request.ObjectLockRetainUntilDate
	}
	if request.ObjectLockLegalHold != nil {
		values.LegalHold = *request.ObjectLockLegalHold
	}
	recorder.RecordAuthorizationDenied(ctx, auditlog.Operation(request.Operation), resource, &auditlog.ObjectLockDetails{Requested: values, BypassRequested: request.BypassGovernanceRetentionRequested})
}

const objectLockXMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

type objectLockConfigurationXML struct {
	XMLName xml.Name           `xml:"ObjectLockConfiguration"`
	Xmlns   string             `xml:"xmlns,attr,omitempty"`
	Enabled string             `xml:"ObjectLockEnabled"`
	Rule    *objectLockRuleXML `xml:"Rule,omitempty"`
}
type objectLockRuleXML struct {
	DefaultRetention *defaultRetentionXML `xml:"DefaultRetention"`
}
type defaultRetentionXML struct {
	Mode  string `xml:"Mode"`
	Days  *int32 `xml:"Days,omitempty"`
	Years *int32 `xml:"Years,omitempty"`
}
type retentionXML struct {
	XMLName xml.Name `xml:"Retention"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Mode    *string  `xml:"Mode,omitempty"`
	Until   *string  `xml:"RetainUntilDate,omitempty"`
}
type legalHoldXML struct {
	XMLName xml.Name `xml:"LegalHold"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status"`
}

type requestedLockContextKey struct{}

func parseRetention(mode, until *string) (*storage.ObjectRetention, error) {
	if mode == nil && until == nil {
		return nil, nil
	}
	if mode == nil || until == nil {
		return nil, ErrInvalidRequest
	}
	date, err := time.Parse(time.RFC3339Nano, *until)
	if err != nil || !date.After(time.Now()) {
		return nil, ErrInvalidArgument
	}
	r := &storage.ObjectRetention{Mode: storage.RetentionMode(*mode), RetainUntilDate: date.UTC()}
	return r, r.Validate()
}

func parseObjectLockHeaders(r *http.Request) (storage.ObjectLock, error) {
	lock := storage.ObjectLock{}
	for _, name := range []string{"x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-object-lock-legal-hold"} {
		if values, ok := r.Header[http.CanonicalHeaderKey(name)]; ok && (len(values) != 1 || values[0] == "") {
			return lock, ErrInvalidArgument
		}
	}
	rt, err := parseRetention(getHeaderAsPtr(r.Header, "x-amz-object-lock-mode"), getHeaderAsPtr(r.Header, "x-amz-object-lock-retain-until-date"))
	if err != nil {
		return lock, err
	}
	lock.Retention = rt
	if status := getHeaderAsPtr(r.Header, "x-amz-object-lock-legal-hold"); status != nil {
		hold := storage.LegalHoldStatus(*status)
		lock.LegalHold = &hold
	}
	return lock, lock.Validate()
}

func requestedBypass(r *http.Request) (bool, error) {
	value := getHeaderAsPtr(r.Header, "x-amz-bypass-governance-retention")
	if value == nil {
		return false, nil
	}
	switch *value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, ErrInvalidArgument
	}
}

func (s *Server) authorizeGovernanceBypass(ctx context.Context, bucket, key string, w http.ResponseWriter, r *http.Request) (bool, bool) {
	bypass, err := requestedBypass(r)
	if err != nil {
		handleError(err, w, r)
		return false, true
	}
	if bypass && s.authorizeRequest(ctx, authorization.OperationBypassGovernanceRetention, &bucket, &key, w, r) {
		return false, true
	}
	return bypass, false
}

func (s *Server) prepareUploadLock(w http.ResponseWriter, r *http.Request, bucket, key string) (storage.ObjectLock, bool) {
	lock, err := parseObjectLockHeaders(r)
	if err != nil {
		handleError(err, w, r)
		return lock, true
	}
	if lock.Retention != nil && s.authorizeRequest(r.Context(), authorization.OperationPutObjectRetention, &bucket, &key, w, r) {
		return lock, true
	}
	if lock.LegalHold != nil && s.authorizeRequest(r.Context(), authorization.OperationPutObjectLegalHold, &bucket, &key, w, r) {
		return lock, true
	}
	return lock, false
}

func (s *Server) objectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	operation := authorization.OperationGetObjectLockConfiguration
	var config *storage.ObjectLockConfiguration
	if r.Method == http.MethodPut {
		operation = authorization.OperationPutObjectLockConfiguration
		data, err := readObjectLockBody(r, w)
		if err != nil {
			handleError(err, w, r)
			return
		}
		var body objectLockConfigurationXML
		if err := xml.Unmarshal(data, &body); err != nil {
			handleError(ErrInvalidRequest, w, r)
			return
		}
		config = &storage.ObjectLockConfiguration{ObjectLockEnabled: body.Enabled}
		if body.Rule != nil {
			if body.Rule.DefaultRetention == nil {
				handleError(ErrInvalidRequest, w, r)
				return
			}
			d := body.Rule.DefaultRetention
			config.DefaultRetention = &storage.DefaultRetention{Mode: storage.RetentionMode(d.Mode), Days: d.Days, Years: d.Years}
		}
		if err := config.Validate(); err != nil {
			handleError(err, w, r)
			return
		}
	}
	request, authenticated := makeAuthorizationRequest(ctx, operation, ptrutils.ToPtr(bucket.String()), nil, r)
	if config != nil {
		request.ObjectLockEnabled = &config.ObjectLockEnabled
		if d := config.DefaultRetention; d != nil {
			mode := string(d.Mode)
			request.ObjectLockMode = &mode
			request.ObjectLockDays = d.Days
			request.ObjectLockYears = d.Years
		}
	}
	if s.runAuthorization(ctx, request, authenticated, w, r) {
		return
	}
	if r.Method == http.MethodPut {
		if err := s.storage.PutObjectLockConfiguration(ctx, bucket, config); err != nil {
			handleError(err, w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	config, err = s.storage.GetObjectLockConfiguration(ctx, bucket)
	if err != nil {
		handleError(err, w, r)
		return
	}
	body := objectLockConfigurationXML{Xmlns: objectLockXMLNS, Enabled: config.ObjectLockEnabled}
	if d := config.DefaultRetention; d != nil {
		body.Rule = &objectLockRuleXML{DefaultRetention: &defaultRetentionXML{Mode: string(d.Mode), Days: d.Days, Years: d.Years}}
	}
	writeXMLResponse(w, r, http.StatusOK, body)
}

func (s *Server) objectProtectionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	key, err := storage.NewObjectKey(r.PathValue(keyPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	isRetention := r.URL.Query().Has("retention")
	operation := authorization.OperationGetObjectLegalHold
	if isRetention {
		operation = authorization.OperationGetObjectRetention
	}
	lock := storage.ObjectLock{}
	if r.Method == http.MethodPut {
		data, err := readObjectLockBody(r, w)
		if err != nil {
			handleError(err, w, r)
			return
		}
		if isRetention {
			operation = authorization.OperationPutObjectRetention
			var body retentionXML
			if err := xml.Unmarshal(data, &body); err != nil {
				handleError(ErrInvalidRequest, w, r)
				return
			}
			lock.Retention, err = parseRetention(body.Mode, body.Until)
		} else {
			operation = authorization.OperationPutObjectLegalHold
			var body legalHoldXML
			if err := xml.Unmarshal(data, &body); err != nil {
				handleError(ErrInvalidRequest, w, r)
				return
			}
			status := storage.LegalHoldStatus(body.Status)
			lock.LegalHold = &status
			err = lock.Validate()
		}
		if err != nil {
			handleError(err, w, r)
			return
		}
		ctx = context.WithValue(ctx, requestedLockContextKey{}, lock)
		r = r.WithContext(ctx)
	}
	if s.authorizeRequest(ctx, operation, ptrutils.ToPtr(bucket.String()), ptrutils.ToPtr(key.String()), w, r) {
		return
	}
	opts := &storage.ObjectLockOptions{VersionID: httputils.GetQueryParam(r.URL.Query(), versionIDQuery)}
	if r.Method == http.MethodPut {
		if isRetention {
			bypass, stop := s.authorizeGovernanceBypass(ctx, bucket.String(), key.String(), w, r)
			if stop {
				return
			}
			opts.BypassGovernanceRetention = bypass
			err = s.storage.PutObjectRetention(ctx, bucket, key, lock.Retention, opts)
		} else {
			err = s.storage.PutObjectLegalHold(ctx, bucket, key, *lock.LegalHold, opts)
		}
		if err != nil {
			handleError(err, w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if isRetention {
		retention, err := s.storage.GetObjectRetention(ctx, bucket, key, opts)
		if err != nil {
			handleError(err, w, r)
			return
		}
		body := retentionXML{Xmlns: objectLockXMLNS}
		if retention != nil {
			mode := string(retention.Mode)
			until := retention.RetainUntilDate.UTC().Format(time.RFC3339Nano)
			body.Mode, body.Until = &mode, &until
		}
		writeXMLResponse(w, r, http.StatusOK, body)
	} else {
		status, err := s.storage.GetObjectLegalHold(ctx, bucket, key, opts)
		if err != nil {
			handleError(err, w, r)
			return
		}
		body := legalHoldXML{Xmlns: objectLockXMLNS, Status: "OFF"}
		if status != nil {
			body.Status = string(*status)
		}
		writeXMLResponse(w, r, http.StatusOK, body)
	}
}

func (s *Server) setObjectLockHeaders(w http.ResponseWriter, r *http.Request, object *storage.Object) {
	bucket, key := r.PathValue(bucketPath), r.PathValue(keyPath)
	allowed := func(operation string) bool {
		request, _ := makeAuthorizationRequest(r.Context(), operation, &bucket, &key, r)
		request.VersionID = object.VersionID
		s.bindExistingObjectTagsResolver(request, &bucket, &key, object.VersionID)
		ok, err := s.requestAuthorizer.AuthorizeRequest(r.Context(), request)
		return err == nil && ok
	}
	if rt := object.ObjectLock.Retention; rt != nil && allowed(authorization.OperationGetObjectRetention) {
		w.Header().Set("x-amz-object-lock-mode", string(rt.Mode))
		w.Header().Set("x-amz-object-lock-retain-until-date", rt.RetainUntilDate.UTC().Format(time.RFC3339Nano))
	}
	if hold := object.ObjectLock.LegalHold; hold != nil && allowed(authorization.OperationGetObjectLegalHold) {
		w.Header().Set("x-amz-object-lock-legal-hold", string(*hold))
	}
}

func readObjectLockBody(r *http.Request, w http.ResponseWriter) ([]byte, error) {
	data, err := readLimitedBody(r, w, 16*1024)
	if err != nil {
		return nil, err
	}
	input, err := extractChecksumInput(r)
	if err != nil {
		return nil, err
	}
	_, checksums, err := checksumutils.CalculateChecksumsStreaming(r.Context(), bytes.NewReader(data), func(reader io.Reader) error { _, err := io.Copy(io.Discard, reader); return err })
	if err != nil {
		return nil, err
	}
	if err := metadatastore.ValidateChecksums(input, *checksums); err != nil {
		return nil, err
	}
	return data, nil
}
