package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

func (s *Server) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.headObjectHandler")
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
	authOperation := authorization.OperationHeadObject
	if versionID != nil {
		authOperation = authorization.OperationHeadObjectVersion
	}
	shouldReturn := s.authorizeRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "Head object", "bucket", bucketName.String(), "key", key.String())

	var headOpts *storage.HeadObjectOptions
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	if ifMatch != nil || ifNoneMatch != nil || versionID != nil {
		headOpts = &storage.HeadObjectOptions{}
		if versionID != nil {
			headOpts.VersionID = versionID
		}
		if ifMatch != nil {
			headOpts.IfMatchETag = ifMatch
		}
		if ifNoneMatch != nil {
			headOpts.IfNoneMatchETag = ifNoneMatch
		}
	}

	object, err := s.storage.HeadObject(ctx, bucketName, key, headOpts)
	if err != nil {
		if currentDeleteMarkerErr, ok := err.(*storage.CurrentDeleteMarkerError); ok {
			responseHeaders := w.Header()
			responseHeaders.Set(deleteMarkerHeader, "true")
			if currentDeleteMarkerErr.VersionID != "" {
				responseHeaders.Set(versionIDHeader, currentDeleteMarkerErr.VersionID)
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if versionDeleteMarkerErr, ok := err.(*storage.VersionDeleteMarkerMethodNotAllowedError); ok {
			responseHeaders := w.Header()
			responseHeaders.Set(deleteMarkerHeader, "true")
			if versionDeleteMarkerErr.VersionID != "" {
				responseHeaders.Set(versionIDHeader, versionDeleteMarkerErr.VersionID)
			}
			responseHeaders.Set(lastModifiedHeader, versionDeleteMarkerErr.LastModified.UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err == storage.ErrNotModified {
			w.WriteHeader(304)
			return
		}
		if err == storage.ErrPreconditionFailed {
			w.WriteHeader(412)
			return
		}
		handleError(err, w, r)
		return
	}
	responseHeaders := w.Header()
	setETagHeaderFromObject(responseHeaders, object)
	setChecksumHeadersFromObject(responseHeaders, object)
	if object.VersionID != nil {
		responseHeaders.Set(versionIDHeader, *object.VersionID)
	}
	setTagCountHeaderFromObject(responseHeaders, object)
	setMetadataHeadersFromObject(responseHeaders, object)
	setStorageClassHeaderFromObject(responseHeaders, object)

	gmtTimeLoc := time.FixedZone("GMT", 0)
	responseHeaders.Set(lastModifiedHeader, object.LastModified.In(gmtTimeLoc).Format(time.RFC1123))
	if object.ContentType != nil {
		responseHeaders.Set(contentTypeHeader, *object.ContentType)
	}
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
}

// generateContentRangeValue creates a Content-Range header value from a storage.ByteRange.
// For suffix ranges (Start=nil), it uses the object size to calculate the actual range.
func generateContentRangeValue(br storage.ByteRange, objectSize int64) string {
	start := int64(0)
	if br.Start != nil {
		start = *br.Start
	} else if br.End != nil {
		// Suffix range: calculate start from object size
		suffixLength := min(*br.End, objectSize)
		start = objectSize - suffixLength
	}

	end := objectSize - 1
	if br.Start != nil && br.End != nil {
		// Normal range with explicit end (exclusive, so subtract 1 for inclusive).
		// Clamp to objectSize to match the storage layer's normalizeAndValidateRanges behavior.
		end = min(*br.End, objectSize) - 1
	} else if br.Start == nil && br.End != nil {
		// Suffix range
		end = objectSize - 1
	}

	contentRangeValue := fmt.Sprintf("bytes %d-%d/%d", start, end, objectSize)
	return contentRangeValue
}

var errInvalidByteRange error = fmt.Errorf("invalid byte range")

// parseRangeHeader parses HTTP Range header and returns storage.ByteRange array.
// It converts HTTP ranges (inclusive end) to storage ranges (exclusive end) automatically.
// Suffix ranges (bytes=-N) are passed through as-is to be resolved by the storage layer.
func parseRangeHeader(rangeHeader string) ([]storage.ByteRange, error) {
	var ranges []storage.ByteRange
	if rangeHeader == "" {
		return ranges, nil
	}

	rangeUnitAndRangesSplit := strings.SplitN(rangeHeader, "=", 2)
	if len(rangeUnitAndRangesSplit) != 2 || rangeUnitAndRangesSplit[0] != "bytes" {
		return nil, errInvalidByteRange
	}

	rangesSplit := strings.SplitSeq(rangeUnitAndRangesSplit[1], ",")
	for rangeVal := range rangesSplit {
		rangeVal = strings.TrimSpace(rangeVal)
		byteSplit := strings.SplitN(rangeVal, "-", 2)
		if len(byteSplit) != 2 {
			return nil, errInvalidByteRange
		}

		var start *int64
		var end *int64

		if byteSplit[0] != "" {
			startByte, err := strconv.ParseInt(byteSplit[0], 10, 64)
			if err != nil {
				return nil, errInvalidByteRange
			}
			start = &startByte
		}
		if byteSplit[1] != "" {
			endByte, err := strconv.ParseInt(byteSplit[1], 10, 64)
			if err != nil {
				return nil, errInvalidByteRange
			}
			end = &endByte
		}
		if start == nil && end == nil {
			return nil, errInvalidByteRange
		}

		if start == nil && end != nil {
			// Suffix range (bytes=-N): pass through as-is
			ranges = append(ranges, storage.ByteRange{Start: nil, End: end})
		} else if start != nil {
			// Normal range: convert inclusive end to exclusive end
			var exclusiveEnd *int64
			if end != nil {
				excEnd := *end + 1
				exclusiveEnd = &excEnd
			}
			ranges = append(ranges, storage.ByteRange{Start: start, End: exclusiveEnd})
		}
	}

	return ranges, nil
}

func (s *Server) getObjectOrListPartsHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(uploadIdQuery) {
		s.listPartsHandler(w, r)
		return
	}
	if query.Has(taggingQuery) {
		s.getObjectTaggingHandler(w, r)
		return
	}
	s.getObjectHandler(w, r)
}

func (s *Server) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listPartsHandler")
	defer span.End()

	query := r.URL.Query()

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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListParts, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberMarker := httputils.GetQueryParam(query, partNumberMarkerQuery)
	maxParts := query.Get(maxPartsQuery)
	maxPartsI64, err := strconv.ParseInt(maxParts, 10, 32)
	if err != nil {
		maxPartsI64 = 1000
	}
	if maxPartsI64 < 0 || maxPartsI64 > 1000 {
		w.WriteHeader(400)
		return
	}
	maxPartsI32 := int32(maxPartsI64)

	result, nextPartNumberMarker, err := s.listAndFilterParts(ctx, r, bucketName, key, uploadId, storage.ListPartsOptions{
		PartNumberMarker: partNumberMarker,
		MaxParts:         maxPartsI32,
	})
	if err != nil {
		handleError(err, w, r)
		return
	}

	listPartsResult := ListPartsResult{
		Bucket:               result.BucketName.String(),
		Key:                  result.Key.String(),
		UploadId:             result.UploadId.String(),
		PartNumberMarker:     ptrutils.ToPtr(result.PartNumberMarker),
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             result.MaxParts,
		IsTruncated:          result.IsTruncated,
		Parts: sliceutils.Map(func(part *storage.MultipartPart) *PartResult {
			return &PartResult{
				ETag:              part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      part.LastModified.UTC().Format(time.RFC3339),
				PartNumber:        part.PartNumber,
				Size:              part.Size,
			}
		}, result.Parts),
		StorageClass: storage.EffectiveStorageClass(result.StorageClass),
	}

	writeXMLResponse(w, r, http.StatusOK, listPartsResult)
}

func (s *Server) listAndFilterParts(ctx context.Context, r *http.Request, bucketName storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, *string, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}
	partNumberMarker := opts.PartNumberMarker
	collectedParts := make([]*storage.MultipartPart, 0, maxParts)
	var nextPartNumberMarker *string
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListParts, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), r)

	for {
		result, err := s.storage.ListParts(ctx, bucketName, key, uploadID, storage.ListPartsOptions{
			PartNumberMarker: partNumberMarker,
			MaxParts:         maxParts,
		})
		if err != nil {
			return nil, nil, err
		}

		lastPartNumberMarker := partNumberMarker
		for partIndex, part := range result.Parts {
			partNumberStr := strconv.Itoa(int(part.PartNumber))
			lastPartNumberMarker = &partNumberStr
			allowed, err := s.authorizeListPart(ctx, baseRequest, part.PartNumber)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedParts = append(collectedParts, part)
			if int32(len(collectedParts)) >= maxParts {
				hasMore := partIndex < len(result.Parts)-1 || result.IsTruncated
				if hasMore {
					nextPartNumberMarker = lastPartNumberMarker
					return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nextPartNumberMarker, MaxParts: maxParts, IsTruncated: true, Parts: collectedParts, StorageClass: result.StorageClass}, nextPartNumberMarker, nil
				}
				return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts, StorageClass: result.StorageClass}, nil, nil
			}
		}

		if !result.IsTruncated {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts, StorageClass: result.StorageClass}, nil, nil
		}
		if lastPartNumberMarker == nil {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts, StorageClass: result.StorageClass}, nil, nil
		}
		if partNumberMarker != nil && *partNumberMarker == *lastPartNumberMarker {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts, StorageClass: result.StorageClass}, nil, nil
		}
		partNumberMarker = ptrutils.ToPtr(*lastPartNumberMarker)
		nextPartNumberMarker = partNumberMarker
	}
}

func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getObjectHandler")
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
	authOperation := authorization.OperationGetObject
	if versionID != nil {
		authOperation = authorization.OperationGetObjectVersion
	}
	shouldReturn := s.authorizeRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	rangeHeaderValue := r.Header.Get(rangeHeader)
	slog.InfoContext(r.Context(), "Getting object", "bucket", bucketName.String(), "key", key.String())

	// Parse range header and convert to storage.ByteRange (validation will be done in GetObject)
	storageRanges, err := parseRangeHeader(rangeHeaderValue)
	if err != nil {
		w.WriteHeader(416)
		return
	}

	var getOpts *storage.GetObjectOptions
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	if ifMatch != nil || ifNoneMatch != nil || versionID != nil {
		getOpts = &storage.GetObjectOptions{}
		if versionID != nil {
			getOpts.VersionID = versionID
		}
		if ifMatch != nil {
			getOpts.IfMatchETag = ifMatch
		}
		if ifNoneMatch != nil {
			getOpts.IfNoneMatchETag = ifNoneMatch
		}
	}

	// GetObject now returns metadata and readers in a single transaction
	// It also validates the ranges and returns ErrInvalidRange if invalid
	object, readers, err := s.storage.GetObject(ctx, bucketName, key, storageRanges, getOpts)
	if err != nil {
		if currentDeleteMarkerErr, ok := err.(*storage.CurrentDeleteMarkerError); ok {
			responseHeaders := w.Header()
			responseHeaders.Set(deleteMarkerHeader, "true")
			if currentDeleteMarkerErr.VersionID != "" {
				responseHeaders.Set(versionIDHeader, currentDeleteMarkerErr.VersionID)
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if versionDeleteMarkerErr, ok := err.(*storage.VersionDeleteMarkerMethodNotAllowedError); ok {
			responseHeaders := w.Header()
			responseHeaders.Set(deleteMarkerHeader, "true")
			if versionDeleteMarkerErr.VersionID != "" {
				responseHeaders.Set(versionIDHeader, versionDeleteMarkerErr.VersionID)
			}
			responseHeaders.Set(lastModifiedHeader, versionDeleteMarkerErr.LastModified.UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err == storage.ErrNotModified {
			w.WriteHeader(304)
			return
		}
		handleError(err, w, r)
		return
	}

	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}
	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	setETagHeaderFromObject(responseHeaders, object)
	if object.VersionID != nil {
		responseHeaders.Set(versionIDHeader, *object.VersionID)
	}

	// Calculate sizes for each range
	var sizes []int64
	var totalSize int64 = 0
	if len(storageRanges) > 0 {
		for _, byteRange := range storageRanges {
			var size int64
			if byteRange.Start == nil && byteRange.End != nil {
				// Suffix range: size is the suffix length (or object size if smaller)
				suffixLength := *byteRange.End
				size = min(suffixLength, object.Size)
			} else if byteRange.Start != nil {
				// Normal range (End is exclusive)
				// Clamp end to object size to match what normalizeAndValidateRanges does in the storage layer.
				var end int64 = object.Size
				if byteRange.End != nil {
					end = min(*byteRange.End, object.Size)
				}
				size = end - *byteRange.Start
			}
			sizes = append(sizes, size)
			totalSize += size
		}
		// Wrap readers with tracing
		for i, reader := range readers {
			readers[i] = ioutils.NewTracingReadCloser(ctx, s.tracer, "GetObjectRange", reader)
		}
	} else {
		// No range specified - we only include the headers for requests without range headers
		setChecksumHeadersFromObject(responseHeaders, object)
		// Wrap the single reader with tracing
		readers[0] = ioutils.NewTracingReadCloser(ctx, s.tracer, "GetObject", readers[0])
		size := object.Size
		sizes = append(sizes, size)
		totalSize = size
	}

	defer (func() {
		for _, reader := range readers {
			reader.Close()
		}
	})()
	responseHeaders.Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	setTagCountHeaderFromObject(responseHeaders, object)
	setMetadataHeadersFromObject(responseHeaders, object)
	setStorageClassHeaderFromObject(responseHeaders, object)
	responseHeaders.Set(acceptRangesHeader, "bytes")
	if len(storageRanges) > 1 {
		separator := ulid.Make().String()
		rangeHeaderLength := int64(0)
		rangeHeaders := []string{}
		for idx := range readers {
			contentRangeValue := generateContentRangeValue(storageRanges[idx], object.Size)
			rangeHeader := fmt.Sprintf("%s: %s\r\n\r\n", contentRangeHeader, contentRangeValue)
			rangeHeaders = append(rangeHeaders, rangeHeader)
			rangeHeaderLength += int64(len(rangeHeader))
		}
		separatorLength := int64(len(separator))
		rangesCount := int64(len(storageRanges))
		startCrlfLength := (rangesCount - 1) * 2 /* \r\n */
		separatorLineLength := (2 /* -- */ + 2 /* \r\n */ + separatorLength)
		endSeparatorLineLength := separatorLineLength + 2 /* \r\n */ + 2 /* -- at the end */
		totalSize = totalSize + startCrlfLength + rangeHeaderLength + separatorLineLength*rangesCount + endSeparatorLineLength
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		responseHeaders.Set(contentTypeHeader, fmt.Sprintf("multipart/byteranges; boundary=%v", separator))
		w.WriteHeader(206)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		for idx := range readers {
			if idx > 0 {
				if _, err := io.WriteString(writer, "\r\n"); err != nil {
					slog.DebugContext(ctx, "Stopped multipart response write", "error", err)
					return
				}
			}
			if _, err := io.WriteString(writer, fmt.Sprintf("--%s\r\n", separator)); err != nil {
				slog.DebugContext(ctx, "Stopped multipart response write", "error", err)
				return
			}

			if _, err := io.WriteString(writer, rangeHeaders[idx]); err != nil {
				slog.DebugContext(ctx, "Stopped multipart response write", "error", err)
				return
			}

			if _, err := ioutils.CopyN(writer, readers[idx], sizes[idx]); err != nil {
				slog.DebugContext(ctx, "Stopped multipart response copy", "error", err)
				return
			}
		}
		if _, err := io.WriteString(writer, fmt.Sprintf("\r\n--%s--\r\n", separator)); err != nil {
			slog.DebugContext(ctx, "Stopped multipart response final write", "error", err)
			return
		}
	} else if len(storageRanges) == 1 {
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		contentRangeValue := generateContentRangeValue(storageRanges[0], object.Size)
		responseHeaders.Set(contentRangeHeader, contentRangeValue)
		w.WriteHeader(206)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		if _, err := ioutils.CopyN(writer, readers[0], totalSize); err != nil {
			slog.DebugContext(ctx, "Stopped single range response copy", "error", err)
			return
		}
	} else {
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		w.WriteHeader(200)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		if _, err := ioutils.CopyN(writer, readers[0], totalSize); err != nil {
			slog.DebugContext(ctx, "Stopped full response copy", "error", err)
			return
		}
	}
}
