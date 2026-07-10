package server

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func writeNoSuchLifecycleConfiguration(w http.ResponseWriter, r *http.Request) {
	writeS3ErrorResponse(w, r, http.StatusNotFound, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", r.URL.Path)
}

func writeLifecycleValidationError(w http.ResponseWriter, r *http.Request, validationErr *storage.LifecycleValidationError) {
	writeS3ErrorResponse(w, r, http.StatusBadRequest, validationErr.Code, validationErr.Message, r.URL.Path)
}

func writeNotImplemented(w http.ResponseWriter, r *http.Request, message string) {
	writeS3ErrorResponse(w, r, http.StatusNotImplemented, "NotImplemented", message, r.URL.Path)
}

// lifecycleDateFormats are the ISO 8601 variants accepted for the Expiration
// Date element. S3 always reports dates at midnight UTC.
var lifecycleDateFormats = []string{
	time.RFC3339,
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02",
}

func parseLifecycleDate(value string) (*time.Time, error) {
	for _, format := range lifecycleDateFormats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			parsed = parsed.UTC()
			return &parsed, nil
		}
	}
	return nil, fmt.Errorf("invalid lifecycle date %q", value)
}

func convertLifecycleTagFromXML(tag *LifecycleConfigurationTag) *storage.LifecycleTag {
	if tag == nil {
		return nil
	}
	return &storage.LifecycleTag{Key: tag.Key, Value: tag.Value}
}

func convertLifecycleTagToXML(tag *storage.LifecycleTag) *LifecycleConfigurationTag {
	if tag == nil {
		return nil
	}
	return &LifecycleConfigurationTag{Key: tag.Key, Value: tag.Value}
}

// convertLifecycleConfigurationFromXML maps the parsed request body to the
// storage model. It returns a *storage.LifecycleValidationError when a value
// cannot be represented (e.g. a malformed Date).
func convertLifecycleConfigurationFromXML(request *LifecycleConfiguration) (*storage.BucketLifecycleConfiguration, *storage.LifecycleValidationError) {
	config := &storage.BucketLifecycleConfiguration{
		Rules: make([]storage.LifecycleRule, 0, len(request.Rules)),
	}
	for _, rule := range request.Rules {
		converted := storage.LifecycleRule{
			ID:     rule.ID,
			Status: rule.Status,
			Prefix: rule.Prefix,
		}
		if rule.Filter != nil {
			filter := &storage.LifecycleFilter{
				Prefix:                rule.Filter.Prefix,
				Tag:                   convertLifecycleTagFromXML(rule.Filter.Tag),
				ObjectSizeGreaterThan: rule.Filter.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    rule.Filter.ObjectSizeLessThan,
			}
			if rule.Filter.And != nil {
				and := &storage.LifecycleFilterAnd{
					Prefix:                rule.Filter.And.Prefix,
					ObjectSizeGreaterThan: rule.Filter.And.ObjectSizeGreaterThan,
					ObjectSizeLessThan:    rule.Filter.And.ObjectSizeLessThan,
				}
				for _, tag := range rule.Filter.And.Tags {
					and.Tags = append(and.Tags, storage.LifecycleTag{Key: tag.Key, Value: tag.Value})
				}
				filter.And = and
			}
			converted.Filter = filter
		}
		if rule.Expiration != nil {
			expiration := &storage.LifecycleExpiration{
				Days:                      rule.Expiration.Days,
				ExpiredObjectDeleteMarker: rule.Expiration.ExpiredObjectDeleteMarker,
			}
			if rule.Expiration.Date != nil {
				date, err := parseLifecycleDate(*rule.Expiration.Date)
				if err != nil {
					return nil, &storage.LifecycleValidationError{
						Code:    "InvalidArgument",
						Message: "'Date' must be in ISO 8601 format",
					}
				}
				expiration.Date = date
			}
			converted.Expiration = expiration
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			converted.AbortIncompleteMultipartUpload = &storage.LifecycleAbortIncompleteMultipartUpload{
				DaysAfterInitiation: rule.AbortIncompleteMultipartUpload.DaysAfterInitiation,
			}
		}
		for _, transition := range rule.Transitions {
			convertedTransition := storage.LifecycleTransition{
				Days:         transition.Days,
				StorageClass: transition.StorageClass,
			}
			if transition.Date != nil {
				date, err := parseLifecycleDate(*transition.Date)
				if err != nil {
					return nil, &storage.LifecycleValidationError{
						Code:    "InvalidArgument",
						Message: "'Date' must be in ISO 8601 format",
					}
				}
				convertedTransition.Date = date
			}
			converted.Transitions = append(converted.Transitions, convertedTransition)
		}
		for _, transition := range rule.NoncurrentVersionTransitions {
			converted.NoncurrentVersionTransitions = append(converted.NoncurrentVersionTransitions, storage.LifecycleNoncurrentVersionTransition{
				NoncurrentDays:          transition.NoncurrentDays,
				NewerNoncurrentVersions: transition.NewerNoncurrentVersions,
				StorageClass:            transition.StorageClass,
			})
		}
		if rule.NoncurrentVersionExpiration != nil {
			converted.NoncurrentVersionExpiration = &storage.LifecycleNoncurrentVersionExpiration{
				NoncurrentDays:          rule.NoncurrentVersionExpiration.NoncurrentDays,
				NewerNoncurrentVersions: rule.NoncurrentVersionExpiration.NewerNoncurrentVersions,
			}
		}
		config.Rules = append(config.Rules, converted)
	}
	return config, nil
}

func convertLifecycleConfigurationToXML(config *storage.BucketLifecycleConfiguration) *LifecycleConfiguration {
	response := &LifecycleConfiguration{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Rules: make([]LifecycleConfigurationRule, 0, len(config.Rules)),
	}
	for _, rule := range config.Rules {
		converted := LifecycleConfigurationRule{
			ID:     rule.ID,
			Status: rule.Status,
			Prefix: rule.Prefix,
		}
		if rule.Filter != nil {
			filter := &LifecycleConfigurationFilter{
				Prefix:                rule.Filter.Prefix,
				Tag:                   convertLifecycleTagToXML(rule.Filter.Tag),
				ObjectSizeGreaterThan: rule.Filter.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    rule.Filter.ObjectSizeLessThan,
			}
			if rule.Filter.And != nil {
				and := &LifecycleConfigurationAndOperator{
					Prefix:                rule.Filter.And.Prefix,
					ObjectSizeGreaterThan: rule.Filter.And.ObjectSizeGreaterThan,
					ObjectSizeLessThan:    rule.Filter.And.ObjectSizeLessThan,
				}
				for _, tag := range rule.Filter.And.Tags {
					and.Tags = append(and.Tags, LifecycleConfigurationTag{Key: tag.Key, Value: tag.Value})
				}
				filter.And = and
			}
			converted.Filter = filter
		}
		if rule.Expiration != nil {
			expiration := &LifecycleConfigurationExpiration{
				Days:                      rule.Expiration.Days,
				ExpiredObjectDeleteMarker: rule.Expiration.ExpiredObjectDeleteMarker,
			}
			if rule.Expiration.Date != nil {
				expiration.Date = ptrutils.ToPtr(rule.Expiration.Date.UTC().Format(time.RFC3339))
			}
			converted.Expiration = expiration
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			converted.AbortIncompleteMultipartUpload = &LifecycleConfigurationAbortIncompleteMultipartUpload{
				DaysAfterInitiation: rule.AbortIncompleteMultipartUpload.DaysAfterInitiation,
			}
		}
		for _, transition := range rule.Transitions {
			convertedTransition := LifecycleConfigurationTransition{
				Days:         transition.Days,
				StorageClass: transition.StorageClass,
			}
			if transition.Date != nil {
				convertedTransition.Date = ptrutils.ToPtr(transition.Date.UTC().Format(time.RFC3339))
			}
			converted.Transitions = append(converted.Transitions, convertedTransition)
		}
		for _, transition := range rule.NoncurrentVersionTransitions {
			converted.NoncurrentVersionTransitions = append(converted.NoncurrentVersionTransitions, LifecycleConfigurationNoncurrentVersionTransition{
				NoncurrentDays:          transition.NoncurrentDays,
				NewerNoncurrentVersions: transition.NewerNoncurrentVersions,
				StorageClass:            transition.StorageClass,
			})
		}
		if rule.NoncurrentVersionExpiration != nil {
			converted.NoncurrentVersionExpiration = &LifecycleConfigurationNoncurrentVersionExpiration{
				NoncurrentDays:          rule.NoncurrentVersionExpiration.NoncurrentDays,
				NewerNoncurrentVersions: rule.NoncurrentVersionExpiration.NewerNoncurrentVersions,
			}
		}
		response.Rules = append(response.Rules, converted)
	}
	return response
}

func (s *Server) getBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketLifecycleHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetBucketLifecycle, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	config, err := s.storage.GetBucketLifecycleConfiguration(ctx, bucketName)
	if err != nil {
		if err == storage.ErrNoSuchLifecycleConfiguration {
			writeNoSuchLifecycleConfiguration(w, r)
			return
		}
		handleError(err, w, r)
		return
	}

	response := convertLifecycleConfigurationToXML(config)

	writeXMLResponse(w, r, http.StatusOK, response)
}

func (s *Server) putBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketLifecycleHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutBucketLifecycle, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := readLimitedBody(r, w, maxPutBucketLifecycleBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request LifecycleConfiguration
	err = xml.Unmarshal(data, &request)
	if err != nil {
		writeMalformedXML(w, r)
		return
	}

	config, validationErr := convertLifecycleConfigurationFromXML(&request)
	if validationErr != nil {
		writeLifecycleValidationError(w, r, validationErr)
		return
	}

	if validationErr := storage.ValidateBucketLifecycleConfiguration(config); validationErr != nil {
		writeLifecycleValidationError(w, r, validationErr)
		return
	}

	err = s.storage.PutBucketLifecycleConfiguration(ctx, bucketName, config)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketLifecycleHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucketLifecycle, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	err = s.storage.DeleteBucketLifecycleConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}
