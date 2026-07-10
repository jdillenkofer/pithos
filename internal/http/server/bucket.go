package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listBucketsHandler")
	defer span.End()

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListBuckets, nil, nil, w, r)
	if shouldReturn {
		return
	}
	slog.InfoContext(r.Context(), "Listing Buckets")
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		handleError(err, w, r)
		return
	}
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListBuckets, nil, nil, r)
	listAllMyBucketsResult := ListAllMyBucketsResult{
		Buckets: []*BucketResult{},
	}
	for _, bucket := range buckets {
		allowed, err := s.authorizeListBucket(ctx, baseRequest, bucket.Name.String())
		if err != nil {
			handleError(err, w, r)
			return
		}
		if !allowed {
			continue
		}
		listAllMyBucketsResult.Buckets = append(listAllMyBucketsResult.Buckets, &BucketResult{
			Name:         bucket.Name.String(),
			CreationDate: bucket.CreationDate.UTC().Format(time.RFC3339),
		})
	}
	writeXMLResponse(w, r, http.StatusOK, listAllMyBucketsResult)
}

func (s *Server) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.headBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	shouldReturn := s.authorizeRequest(ctx, authorization.OperationHeadBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}
	slog.InfoContext(r.Context(), "Head bucket", "bucket", bucketName)
	_, err = s.storage.HeadBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) routeBucketGetHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(versioningQuery) {
		s.getBucketVersioningHandler(w, r)
		return
	}
	if query.Has(versionsQuery) {
		s.listObjectVersionsHandler(w, r)
		return
	}
	if query.Has(corsQuery) {
		s.getBucketCORSHandler(w, r)
		return
	}
	if query.Has(lifecycleQuery) {
		s.getBucketLifecycleHandler(w, r)
		return
	}
	if query.Has(notificationQuery) {
		s.getBucketNotificationHandler(w, r)
		return
	}
	if query.Has(websiteQuery) {
		s.getBucketWebsiteHandler(w, r)
		return
	}
	if query.Has(uploadsQuery) {
		s.listMultipartUploadsHandler(w, r)
		return
	}

	listType := query.Get(listTypeQuery)
	if listType == "2" {
		s.listObjectsV2Handler(w, r)
		return
	}
	s.listObjectsHandler(w, r)
}

func (s *Server) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listMultipartUploadsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListMultipartUploads, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	keyMarker := httputils.GetQueryParam(query, keyMarkerQuery)
	uploadIdMarker := httputils.GetQueryParam(query, uploadIdMarkerQuery)
	maxUploads := query.Get(maxUploadsQuery)
	maxUploadsI64, err := strconv.ParseInt(maxUploads, 10, 32)
	if err != nil || maxUploadsI64 < 0 || maxUploadsI64 > maxListLimit {
		maxUploadsI64 = 1000
	}
	maxUploadsI32 := int32(maxUploadsI64)

	opts := storage.ListMultipartUploadsOptions{Prefix: prefix, Delimiter: delimiter, KeyMarker: keyMarker, UploadIdMarker: uploadIdMarker, MaxUploads: maxUploadsI32}
	slog.InfoContext(r.Context(), "Listing MultipartUploads")
	result, nextKeyMarker, nextUploadIDMarker, err := s.listAndFilterMultipartUploads(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listMultipartUploadsResult := ListMultipartUploadsResult{
		Bucket:             result.BucketName.String(),
		KeyMarker:          ptrutils.ToPtr(result.KeyMarker),
		UploadIdMarker:     ptrutils.ToPtr(result.UploadIdMarker),
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIDMarker,
		MaxUploads:         maxUploadsI32,
		IsTruncated:        result.IsTruncated,
		Delimiter:          ptrutils.ToPtr(result.Delimiter),
		Prefix:             ptrutils.ToPtr(result.Prefix),
		Uploads:            []*UploadResult{},
		CommonPrefixes:     []*CommonPrefixResult{},
	}

	for _, upload := range result.Uploads {
		listMultipartUploadsResult.Uploads = append(listMultipartUploadsResult.Uploads, &UploadResult{
			Key:          upload.Key.String(),
			UploadId:     upload.UploadId.String(),
			Initiated:    upload.Initiated.UTC().Format(time.RFC3339),
			StorageClass: storage.EffectiveStorageClass(upload.StorageClass),
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listMultipartUploadsResult.CommonPrefixes = append(listMultipartUploadsResult.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}
	writeXMLResponse(w, r, http.StatusOK, listMultipartUploadsResult)
}

func (s *Server) listAndFilterMultipartUploads(ctx context.Context, r *http.Request, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, *string, *string, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}
	collectedUploads := []storage.Upload{}
	collectedPrefixes := []string{}
	seenPrefixes := map[string]struct{}{}
	keyMarker := opts.KeyMarker
	uploadIDMarker := opts.UploadIdMarker
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListMultipartUploads, ptrutils.ToPtr(bucketName.String()), nil, r)
	var nextKeyMarker *string
	var nextUploadIDMarker *string

	for {
		result, err := s.storage.ListMultipartUploads(ctx, bucketName, storage.ListMultipartUploadsOptions{
			Prefix:         opts.Prefix,
			Delimiter:      opts.Delimiter,
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
			MaxUploads:     maxUploads,
		})
		if err != nil {
			return nil, nil, nil, err
		}

		lastKeyMarker := keyMarker
		lastUploadIDMarker := uploadIDMarker
		for uploadIndex, upload := range result.Uploads {
			uploadKey := upload.Key.String()
			uploadID := upload.UploadId.String()
			lastKeyMarker = &uploadKey
			lastUploadIDMarker = &uploadID
			allowed, err := s.authorizeListMultipartUpload(ctx, baseRequest, uploadKey, uploadID)
			if err != nil {
				return nil, nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedUploads = append(collectedUploads, upload)
			if int32(len(collectedUploads)) >= maxUploads {
				hasMore := uploadIndex < len(result.Uploads)-1 || len(result.CommonPrefixes) > 0 || result.IsTruncated
				if hasMore {
					nextKeyMarker = lastKeyMarker
					nextUploadIDMarker = lastUploadIDMarker
					return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, NextKeyMarker: *lastKeyMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, NextUploadIdMarker: *lastUploadIDMarker, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: true}, nextKeyMarker, nextUploadIDMarker, nil
				}
				return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
			}
		}
		for _, commonPrefix := range result.CommonPrefixes {
			lastKeyMarker = &commonPrefix
			lastUploadIDMarker = ptrutils.ToPtr("")
			allowed, err := s.authorizeListMultipartUpload(ctx, baseRequest, commonPrefix, "")
			if err != nil {
				return nil, nil, nil, err
			}
			if !allowed {
				continue
			}
			if _, exists := seenPrefixes[commonPrefix]; exists {
				continue
			}
			seenPrefixes[commonPrefix] = struct{}{}
			collectedPrefixes = append(collectedPrefixes, commonPrefix)
		}

		if !result.IsTruncated {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		if lastKeyMarker == nil || lastUploadIDMarker == nil {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		if keyMarker != nil && uploadIDMarker != nil && *keyMarker == *lastKeyMarker && *uploadIDMarker == *lastUploadIDMarker {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		keyMarker = ptrutils.ToPtr(*lastKeyMarker)
		uploadIDMarker = ptrutils.ToPtr(*lastUploadIDMarker)
		nextKeyMarker = keyMarker
		nextUploadIDMarker = uploadIDMarker
	}
}

func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listObjectsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	marker := httputils.GetQueryParam(query, markerQuery)
	startAfter := httputils.GetQueryParam(query, startAfterQuery)

	if marker != nil {
		startAfter = marker
	}

	maxKeys := query.Get(maxKeysQuery)
	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 || maxKeysI64 > maxListLimit {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	opts := storage.ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, StartAfter: startAfter, MaxKeys: maxKeysI32}
	slog.InfoContext(r.Context(), "Listing objects", "bucket", bucketName.String())
	result, nextMarker, err := s.listAndFilterObjects(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listBucketResult := ListBucketResult{
		Name:           bucketName.String(),
		Prefix:         prefix,
		Delimiter:      delimiter,
		StartAfter:     httputils.GetQueryParam(query, startAfterQuery), // Original start-after from request
		Marker:         marker,
		KeyCount:       int32(len(result.Objects)),
		MaxKeys:        maxKeysI32,
		CommonPrefixes: []*CommonPrefixResult{},
		IsTruncated:    result.IsTruncated,
		Contents:       []*ContentResult{},
	}

	// Set NextMarker if results are truncated and we have objects
	if result.IsTruncated && nextMarker != nil {
		listBucketResult.NextMarker = nextMarker
	}

	for _, object := range result.Objects {
		listBucketResult.Contents = append(listBucketResult.Contents, &ContentResult{
			Key:          object.Key.String(),
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: storage.EffectiveStorageClass(object.StorageClass),
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listBucketResult.CommonPrefixes = append(listBucketResult.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}
	writeXMLResponse(w, r, http.StatusOK, listBucketResult)
}

func (s *Server) listAndFilterObjects(ctx context.Context, r *http.Request, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, *string, error) {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	collectedObjects := []storage.Object{}
	collectedPrefixes := []string{}
	seenPrefixes := map[string]struct{}{}
	startAfter := opts.StartAfter
	var nextMarker *string
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, r)

	for {
		result, err := s.storage.ListObjects(ctx, bucketName, storage.ListObjectsOptions{
			Prefix:     opts.Prefix,
			Delimiter:  opts.Delimiter,
			StartAfter: startAfter,
			MaxKeys:    maxKeys,
		})
		if err != nil {
			return nil, nil, err
		}

		lastScanned := startAfter
		for objectIndex, object := range result.Objects {
			key := object.Key.String()
			lastScanned = &key
			allowed, err := s.authorizeListObject(ctx, baseRequest, key, object.Tags)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedObjects = append(collectedObjects, object)
			if int32(len(collectedObjects)) >= maxKeys {
				hasMore := objectIndex < len(result.Objects)-1 || len(result.CommonPrefixes) > 0 || result.IsTruncated
				if hasMore {
					nextMarker = lastScanned
					return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: true}, nextMarker, nil
				}
				return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
			}
		}
		for _, commonPrefix := range result.CommonPrefixes {
			lastScanned = &commonPrefix
			allowed, err := s.authorizeListObject(ctx, baseRequest, commonPrefix, nil)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			if _, exists := seenPrefixes[commonPrefix]; exists {
				continue
			}
			seenPrefixes[commonPrefix] = struct{}{}
			collectedPrefixes = append(collectedPrefixes, commonPrefix)
		}

		if !result.IsTruncated {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		if lastScanned == nil {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		if startAfter != nil && *startAfter == *lastScanned {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		startAfter = ptrutils.ToPtr(*lastScanned)
		nextMarker = startAfter
	}
}

func (s *Server) listObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listObjectsV2Handler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	continuationToken := httputils.GetQueryParam(query, continuationTokenQuery)
	maxKeys := query.Get(maxKeysQuery)

	startAfter := httputils.GetQueryParam(query, startAfterQuery)
	if continuationToken != nil {
		startAfter = continuationToken
	}

	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 || maxKeysI64 > maxListLimit {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	opts := storage.ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, StartAfter: startAfter, MaxKeys: maxKeysI32}
	slog.InfoContext(r.Context(), "Listing objects V2", "bucket", bucketName.String())
	result, nextToken, err := s.listAndFilterObjects(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// Prepare the V2 response
	listBucketV2Result := ListBucketV2Result{
		Name:              bucketName.String(),
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeysI32,
		KeyCount:          int32(len(result.Objects)),
		IsTruncated:       result.IsTruncated,
		ContinuationToken: continuationToken,
		StartAfter:        httputils.GetQueryParam(query, startAfterQuery), // Original start-after from request
		CommonPrefixes:    []*CommonPrefixResult{},
		Contents:          []*ContentResult{},
	}

	if result.IsTruncated && nextToken != nil {
		listBucketV2Result.NextContinuationToken = nextToken
	}

	for _, object := range result.Objects {
		contentResult := &ContentResult{
			Key:          object.Key.String(),
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: storage.EffectiveStorageClass(object.StorageClass),
		}

		listBucketV2Result.Contents = append(listBucketV2Result.Contents, contentResult)
	}

	for _, commonPrefix := range result.CommonPrefixes {
		listBucketV2Result.CommonPrefixes = append(listBucketV2Result.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}

	writeXMLResponse(w, r, http.StatusOK, listBucketV2Result)
}

func (s *Server) routeBucketPutHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(versioningQuery) {
		s.putBucketVersioningHandler(w, r)
		return
	}
	if query.Has(corsQuery) {
		s.putBucketCORSHandler(w, r)
		return
	}
	if query.Has(lifecycleQuery) {
		s.putBucketLifecycleHandler(w, r)
		return
	}
	if query.Has(notificationQuery) {
		s.putBucketNotificationHandler(w, r)
		return
	}
	if query.Has(websiteQuery) {
		s.putBucketWebsiteHandler(w, r)
		return
	}
	s.createBucketHandler(w, r)
}

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.createBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCreateBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Creating bucket", "bucket", bucketName.String())
	err = s.storage.CreateBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	responseHeaders := w.Header()
	responseHeaders.Set(locationHeader, bucketName.String())
	w.WriteHeader(200)
}

func (s *Server) routeBucketDeleteHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(corsQuery) {
		s.deleteBucketCORSHandler(w, r)
		return
	}
	if query.Has(lifecycleQuery) {
		s.deleteBucketLifecycleHandler(w, r)
		return
	}
	if query.Has(websiteQuery) {
		s.deleteBucketWebsiteHandler(w, r)
		return
	}
	s.deleteBucketHandler(w, r)
}

func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Deleting bucket", "bucket", bucketName.String())
	err = s.storage.DeleteBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}
