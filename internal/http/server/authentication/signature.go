package authentication

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

const contentSHA256Header = "x-amz-content-sha256"
const contentSHA256UnsignedPayload = "UNSIGNED-PAYLOAD"
const contentSHA256StreamingUnsignedPayload = "STREAMING-UNSIGNED-PAYLOAD"
const contentSHA256StreamingUnsignedPayloadTrailing = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
const contentSHA256StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
const contentSHA256StreamingPayloadTrailing = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

const signatureAlgorithm = "AWS4-HMAC-SHA256"
const expectedService = "s3"
const expectedRequest = "aws4_request"

const contentEncodingAwsChunked = "aws-chunked"

// maxMemoryCacheSize is the maximum size of a payload that will be cached in memory
// before switching to a disk-based cache.
const maxMemoryCacheSize = 10 * 1000 * 1000

var ErrChunkSignatureMismatch = errors.New("chunk signature mismatch")

type AccessKeyIdContextKey struct{}
type AuthTypeContextKey struct{}
type RequestIDContextKey struct{}
type ClientIPContextKey struct{}

func hmacSha256(secret []byte, data []byte) []byte {
	hmac := hmac.New(sha256.New, secret)
	hmac.Write([]byte(data))
	dataHmac := hmac.Sum(nil)

	return dataHmac
}

func createSigningKey(secretAccessKey string, date string, region string, service string, request string) []byte {
	dateKey := hmacSha256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSha256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSha256(dateRegionKey, []byte(service))
	signingKey := hmacSha256(dateRegionServiceKey, []byte(request))
	return signingKey
}

func createSignature(signingKey []byte, stringToSign string) string {
	data := hmacSha256(signingKey, []byte(stringToSign))
	hexData := hex.EncodeToString(data)
	return hexData
}

type pair struct {
	key string
	val string
}

func generateCanonicalHttpMethod(r *http.Request) string {
	return r.Method
}

func generateCanonicalURI(r *http.Request) string {
	escapedPath := r.URL.EscapedPath()
	if escapedPath == "" {
		return "/"
	}

	canonicalURI := ""
	for idx := 0; idx < len(escapedPath); idx++ {
		ch := escapedPath[idx]
		if ch == '/' {
			canonicalURI += "/"
			continue
		}

		if ch == '%' && idx+2 < len(escapedPath) && isHexChar(escapedPath[idx+1]) && isHexChar(escapedPath[idx+2]) {
			canonicalURI += "%"
			canonicalURI += strings.ToUpper(string(escapedPath[idx+1]))
			canonicalURI += strings.ToUpper(string(escapedPath[idx+2]))
			idx += 2
			continue
		}

		if isUnreservedChar(ch) {
			canonicalURI += string(ch)
			continue
		}

		canonicalURI += fmt.Sprintf("%%%02X", ch)
	}

	return canonicalURI
}

func isHexChar(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isUnreservedChar(ch byte) bool {
	if ch >= 'A' && ch <= 'Z' {
		return true
	}
	if ch >= 'a' && ch <= 'z' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	return ch == '-' || ch == '.' || ch == '_' || ch == '~'
}

func uriEncode(input string) string {
	output := url.QueryEscape(input)
	output = strings.ReplaceAll(output, "+", "%20")
	output = strings.ReplaceAll(output, "*", "%2A")
	output = strings.ReplaceAll(output, "%7E", "~")
	return output
}

func generateCanonicalQueryString(r *http.Request) string {
	queryStrings := []pair{}
	for queryKey, queryValues := range r.URL.Query() {
		if queryKey == "X-Amz-Signature" {
			continue
		}
		encodedQueryKey := uriEncode(queryKey)
		for _, queryVal := range queryValues {
			encodedQueryVal := uriEncode(queryVal)
			queryStrings = append(queryStrings, pair{
				key: encodedQueryKey,
				val: encodedQueryVal,
			})
		}
	}
	slices.SortFunc(queryStrings, func(a, b pair) int {
		byKey := cmp.Compare(a.key, b.key)
		if byKey != 0 {
			return byKey
		}
		return cmp.Compare(a.val, b.val)
	})

	canonicalQueryString := ""
	for idx, queryStringPair := range queryStrings {
		canonicalQueryString += queryStringPair.key + "=" + queryStringPair.val
		if idx < len(queryStrings)-1 {
			canonicalQueryString += "&"
		}
	}
	return canonicalQueryString
}

func includeInCanonicalHeaders(headerKey string, headersToInclude []string) bool {
	return slices.Contains(headersToInclude, headerKey)
}

func mustBeSignedHeader(headerKey string) bool {
	if headerKey == "content-md5" {
		return true
	}
	if strings.HasPrefix(headerKey, "x-amz-") {
		return true
	}
	return false
}

func generateCanonicalHeaders(r *http.Request, headersToInclude []string) string {
	canonicalHeaders := ""
	headers := []pair{}

	headers = append(headers, pair{
		key: "host",
		val: strings.TrimSpace(r.Host),
	})
	for headerKey, headerValues := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if includeInCanonicalHeaders(headerKey, headersToInclude) {
			headerVal := strings.TrimSpace(strings.Join(headerValues, ","))
			headers = append(headers, pair{
				key: headerKey,
				val: headerVal,
			})
		}
	}
	slices.SortFunc(headers, func(a, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	for _, header := range headers {
		canonicalHeaders += header.key + ":" + header.val + "\n"
	}
	return canonicalHeaders
}

func generateSignedHeaders(r *http.Request, headersToInclude []string) string {
	signedHeaders := ""
	headers := []pair{}

	headers = append(headers, pair{
		key: "host",
		val: strings.TrimSpace(r.Host),
	})
	for headerKey, headerValues := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if includeInCanonicalHeaders(headerKey, headersToInclude) {
			headerVal := strings.TrimSpace(strings.Join(headerValues, ","))
			headers = append(headers, pair{
				key: headerKey,
				val: headerVal,
			})
		}
	}
	slices.SortFunc(headers, func(a, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	for idx, header := range headers {
		signedHeaders += header.key
		if idx < len(headers)-1 {
			signedHeaders += ";"
		}
	}
	return signedHeaders
}

func generateHashedPayload(r *http.Request) (*string, error) {
	// Use smart cache (memory up to maxMemoryCacheSize, then disk)
	reader, err := ioutils.NewSmartCachedReadSeekCloser(r.Body, maxMemoryCacheSize)
	if err != nil {
		return nil, err
	}
	sha256Hash := sha256.New()
	_, err = ioutils.Copy(sha256Hash, reader)
	if err != nil {
		return nil, err
	}
	dataSha256 := sha256Hash.Sum(nil)
	hexSha256 := hex.EncodeToString(dataSha256)

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	r.Body = reader
	return &hexSha256, nil
}

func generateCanonicalRequest(r *http.Request, headersToInclude []string, isPresigned bool) (*string, error) {
	canonicalRequest := generateCanonicalHttpMethod(r) + "\n"
	canonicalRequest += generateCanonicalURI(r) + "\n"
	canonicalRequest += generateCanonicalQueryString(r) + "\n"
	canonicalRequest += generateCanonicalHeaders(r, headersToInclude) + "\n"
	canonicalRequest += generateSignedHeaders(r, headersToInclude) + "\n"

	contentSHA256 := r.Header.Get(contentSHA256Header)
	if isPresigned || contentSHA256 == contentSHA256UnsignedPayload {
		canonicalRequest += contentSHA256UnsignedPayload
	} else if contentSHA256 == contentSHA256StreamingUnsignedPayload {
		canonicalRequest += contentSHA256StreamingUnsignedPayload
	} else if contentSHA256 == contentSHA256StreamingUnsignedPayloadTrailing {
		canonicalRequest += contentSHA256StreamingUnsignedPayloadTrailing
	} else if contentSHA256 == contentSHA256StreamingPayload {
		canonicalRequest += contentSHA256StreamingPayload
	} else if contentSHA256 == contentSHA256StreamingPayloadTrailing {
		canonicalRequest += contentSHA256StreamingPayloadTrailing
	} else {
		hashedPayload, err := generateHashedPayload(r)
		if err != nil {
			return nil, err
		}
		canonicalRequest += *hashedPayload
	}
	return &canonicalRequest, nil
}

func generateStringToSign(r *http.Request, timestamp string, scope string, headersToInclude []string, isPresigned bool) (*string, error) {
	canonicalRequest, err := generateCanonicalRequest(r, headersToInclude, isPresigned)
	if err != nil {
		return nil, err
	}
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(*canonicalRequest))
	dataSha256 := sha256Hash.Sum(nil)
	canonicalRequestHexSha256 := hex.EncodeToString(dataSha256)

	stringToSign := signatureAlgorithm + "\n" + timestamp + "\n" + scope + "\n" + canonicalRequestHexSha256
	return &stringToSign, nil
}

func generateStringToSignForChunk(timestamp string, scope string, previousSignature string, chunkHasher hash.Hash) string {
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(""))
	dataSha256 := sha256Hash.Sum(nil)
	emptyHashHex := hex.EncodeToString(dataSha256)

	return signatureAlgorithm + "-PAYLOAD" + "\n" + timestamp + "\n" + scope + "\n" + previousSignature + "\n" + emptyHashHex + "\n" + hex.EncodeToString(chunkHasher.Sum(nil))
}

func generateStringToSignForTrailerChunk(timestamp string, scope string, previousSignature string, trailingChecksumHeader string) string {
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(trailingChecksumHeader + "\n"))
	dataSha256 := sha256Hash.Sum(nil)
	hexHash := hex.EncodeToString(dataSha256)
	return signatureAlgorithm + "-TRAILER" + "\n" + timestamp + "\n" + scope + "\n" + previousSignature + "\n" + hexHash
}

func createScope(date string, region string, service string, request string) string {
	return date + "/" + region + "/" + service + "/" + request
}

func checkAuthentication(validCredentials []Credentials, expectedRegion string, r *http.Request) (usedAccessKeyId *string, authenticated bool) {
	now := time.Now().UTC()
	expectedDate := now.Format("20060102")

	var credential string
	var timestamp string
	var expirationDuration time.Duration
	var signedHeaders string
	var signature string
	var isPresigned bool

	isAwsChunked := false
	contentEncodingHeader := r.Header.Get("Content-Encoding")
	if contentEncodingHeader != "" && strings.HasPrefix(contentEncodingHeader, contentEncodingAwsChunked) {
		isAwsChunked = true
	}

	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader == "" {
		slog.DebugContext(r.Context(), "Authorization header is missing checking for query parameters")
		isPresigned = true
		query := r.URL.Query()
		credential = query.Get("X-Amz-Credential")
		timestamp = query.Get("X-Amz-Date")
		expires := query.Get("X-Amz-Expires")
		slog.DebugContext(r.Context(), "Using presigned auth query parameters")
		parsedExpired, err := strconv.ParseInt(expires, 10, 32)
		if err != nil {
			slog.DebugContext(r.Context(), "Failed to parse X-Amz-Expires: "+err.Error())
			return nil, false
		}
		if parsedExpired < 1 || parsedExpired > 604800 {
			slog.DebugContext(r.Context(), "X-Amz-Expires must be between 1 and 604800 seconds")
			return nil, false
		}
		expirationDuration = time.Duration(parsedExpired) * time.Second
		signedHeaders = query.Get("X-Amz-SignedHeaders")
		signature = query.Get("X-Amz-Signature")
	} else {
		slog.DebugContext(r.Context(), "Authorization header is present")
		isPresigned = false
		authorizationHeader, found := strings.CutPrefix(authorizationHeader, signatureAlgorithm)
		if !found {
			slog.DebugContext(r.Context(), "Authorization header does not start with "+signatureAlgorithm)
			return nil, false
		}
		authFields := strings.Split(authorizationHeader, ",")
		if len(authFields) != 3 {
			slog.DebugContext(r.Context(), "Authorization header does not contain exactly 3 fields")
			return nil, false
		}

		credential = strings.TrimSpace(authFields[0])
		credential, found = strings.CutPrefix(credential, "Credential=")
		if !found {
			slog.DebugContext(r.Context(), "Authorization header does not contain Credential field")
			return nil, false
		}

		// Use Date header (https://developer.mozilla.org/de/docs/Web/HTTP/Headers/Date), if x-amz-date is not specified
		timestamp = r.Header.Get("x-amz-date")
		if timestamp == "" {
			timestamp = r.Header.Get("Date")
		}

		// Default expiration for non presigned urls
		expirationDuration = 5 * time.Minute

		signedHeaders = strings.TrimSpace(authFields[1])
		signedHeaders, found = strings.CutPrefix(signedHeaders, "SignedHeaders=")
		if !found {
			slog.DebugContext(r.Context(), "Authorization header does not contain SignedHeaders field")
			return nil, false
		}

		signature = strings.TrimSpace(authFields[2])
		signature, found = strings.CutPrefix(signature, "Signature=")
		if !found {
			slog.DebugContext(r.Context(), "Authorization header does not contain Signature field")
			return nil, false
		}
	}

	accessKeyIdAndScope := strings.Split(credential, "/")
	if len(accessKeyIdAndScope) != 5 {
		slog.DebugContext(r.Context(), "Credential field does not contain exactly 5 parts")
		return nil, false
	}
	accessKeyId := accessKeyIdAndScope[0]
	foundIndex := slices.IndexFunc(validCredentials, func(c Credentials) bool {
		return c.AccessKeyId == accessKeyId
	})
	if foundIndex < 0 {
		slog.DebugContext(r.Context(), "Access key ID not found in valid credentials")
		return nil, false
	}
	expectedCredentials := validCredentials[foundIndex]
	date := accessKeyIdAndScope[1]
	if date != expectedDate {
		slog.DebugContext(r.Context(), "Date in credential does not match expected date")
		return nil, false
	}
	region := accessKeyIdAndScope[2]
	if region != expectedRegion {
		slog.DebugContext(r.Context(), "Region in credential does not match expected region")
		return nil, false
	}

	service := accessKeyIdAndScope[3]
	if service != expectedService {
		slog.DebugContext(r.Context(), "Service in credential does not match expected service")
		return nil, false
	}

	request := accessKeyIdAndScope[4]
	if request != expectedRequest {
		slog.DebugContext(r.Context(), "Request in credential does not match expected request")
		return nil, false
	}

	scope := createScope(expectedDate, region, service, request)

	parsedTimestamp, err := time.Parse("20060102T150405Z", timestamp)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to parse timestamp: "+err.Error())
		return nil, false
	}
	beforeTimestamp := parsedTimestamp.Add(-15 * time.Minute)
	expiredTimestamp := parsedTimestamp.Add(expirationDuration)
	if now.Before(beforeTimestamp) || now.After(expiredTimestamp) {
		slog.DebugContext(r.Context(), "Timestamp is not within the valid range ("+beforeTimestamp.Format(time.RFC3339)+" - "+expiredTimestamp.Format(time.RFC3339)+")")
		return nil, false
	}

	rawSignedHeadersArray := strings.Split(signedHeaders, ";")
	signedHeadersArray := make([]string, 0, len(rawSignedHeadersArray))
	for _, signedHeader := range rawSignedHeadersArray {
		signedHeader = strings.ToLower(strings.TrimSpace(signedHeader))
		if signedHeader != "" {
			signedHeadersArray = append(signedHeadersArray, signedHeader)
		}
	}
	if !slices.Contains(signedHeadersArray, "host") {
		slog.DebugContext(r.Context(), "Signed headers do not include host")
		return nil, false
	}
	for headerKey := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if mustBeSignedHeader(headerKey) && !slices.Contains(signedHeadersArray, headerKey) {
			slog.DebugContext(r.Context(), "Request contains unsigned security-sensitive header", "header", headerKey)
			return nil, false
		}
	}

	stringToSign, err := generateStringToSign(r, timestamp, scope, signedHeadersArray, isPresigned)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to generate string to sign: "+err.Error())
		return nil, false
	}
	signingKey := createSigningKey(expectedCredentials.SecretAccessKey, expectedDate, region, expectedService, expectedRequest)
	calculatedSignature := createSignature(signingKey, *stringToSign)
	isSignatureValid := subtle.ConstantTimeCompare([]byte(signature), []byte(calculatedSignature)) == 1
	if !isSignatureValid {
		slog.DebugContext(r.Context(), "Signature does not match calculated signature")
		return nil, false
	}

	if isAwsChunked {
		slog.DebugContext(r.Context(), "Request is using AWS Chunked Transfer Encoding")
		contentEncodingHeader, _ := strings.CutPrefix(contentEncodingHeader, contentEncodingAwsChunked+",")
		if contentEncodingHeader != "" {
			r.Header.Set("Content-Encoding", contentEncodingHeader)
		} else {
			r.Header.Del("Content-Encoding")
		}
		r.Header.Set("Content-Length", r.Header.Get("x-amz-decoded-content-length"))
		r.Header.Del("x-amz-decoded-content-length")
		contentSHA256 := r.Header.Get(contentSHA256Header)
		trailingHeader := contentSHA256 == contentSHA256StreamingUnsignedPayloadTrailing || contentSHA256 == contentSHA256StreamingPayloadTrailing
		hasTrailingHeaderWithSignature := contentSHA256 == contentSHA256StreamingPayloadTrailing
		skipChunkValidation := contentSHA256 == contentSHA256StreamingUnsignedPayloadTrailing || contentSHA256 == contentSHA256StreamingUnsignedPayload
		r.Body = newAwsChunkReadCloser(r.Context(), r.Body, timestamp, scope, calculatedSignature, signingKey, trailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation)
	}

	return &accessKeyId, isSignatureValid
}

type awsChunkReadCloser struct {
	ctx                            context.Context
	innerCloser                    io.Closer
	innerBuf                       *bufio.Reader
	chunkBytesRemaining            int64
	chunkSignature                 string
	timestamp                      string
	scope                          string
	previousSignature              string
	chunkHasher                    hash.Hash
	signingKey                     []byte
	hasTrailingHeader              bool
	hasTrailingHeaderWithSignature bool
	skipChunkValidation            bool
}

func newAwsChunkReadCloser(ctx context.Context, inner io.ReadCloser, timestamp string, scope string, previousSignature string, signingKey []byte, hasTrailingHeader bool, hasTrailingHeaderWithSignature bool, skipChunkValidation bool) *awsChunkReadCloser {
	return &awsChunkReadCloser{
		ctx:                            ctx,
		innerCloser:                    inner,
		innerBuf:                       bufio.NewReader(inner),
		chunkBytesRemaining:            -1, // -1 indicates that we are not currently reading a chunk
		chunkSignature:                 "",
		timestamp:                      timestamp,
		scope:                          scope,
		previousSignature:              previousSignature,
		chunkHasher:                    sha256.New(),
		signingKey:                     signingKey,
		hasTrailingHeader:              hasTrailingHeader,
		hasTrailingHeaderWithSignature: hasTrailingHeaderWithSignature,
		skipChunkValidation:            skipChunkValidation,
	}
}

func (r *awsChunkReadCloser) validateSignature() error {
	stringToSign := generateStringToSignForChunk(r.timestamp, r.scope, r.previousSignature, r.chunkHasher)
	calculatedSignature := createSignature(r.signingKey, stringToSign)
	isSignatureValid := subtle.ConstantTimeCompare([]byte(r.chunkSignature), []byte(calculatedSignature)) == 1
	if !isSignatureValid {
		slog.DebugContext(r.ctx, "Chunk signature does not match calculated chunk signature")
		return ErrChunkSignatureMismatch
	}

	r.chunkHasher.Reset()
	r.previousSignature = r.chunkSignature
	return nil
}

func (r *awsChunkReadCloser) Read(p []byte) (n int, err error) {
	if r.chunkBytesRemaining <= 0 {
		chunkMetadata, err := r.innerBuf.ReadBytes('\n')
		if err != nil {
			return 0, err
		}
		split := bytes.SplitN(bytes.Trim(chunkMetadata, "\r\n"), []byte(";chunk-signature="), 2)
		hexLen := string(split[0])
		if len(split) != 2 {
			if !r.skipChunkValidation {
				return 0, ErrChunkSignatureMismatch
			}
		} else {
			signature := string(split[1])
			r.chunkSignature = signature
		}

		length, err := strconv.ParseUint(hexLen, 16, 64)
		if err != nil {
			return 0, err
		}
		r.chunkBytesRemaining = int64(length)
		if length == 0 {
			_, err := r.innerBuf.Discard(2) // Discard the trailing \r\n
			if err != nil {
				return 0, err
			}
			if !r.skipChunkValidation {
				err = r.validateSignature()
				if err != nil {
					return 0, err
				}
			}
			if r.hasTrailingHeader {
				// @TODO: validate the trailing header checksum
				checksumHeader, _ := r.innerBuf.ReadString('\n')
				trailerSignature, _ := r.innerBuf.ReadString('\n')
				checksumHeader = strings.TrimSpace(checksumHeader)
				trailerSignature = strings.TrimSpace(trailerSignature)
				trailerSignature = strings.TrimPrefix(trailerSignature, "x-amz-trailer-signature:")
				slog.DebugContext(r.ctx, "Validating trailing headers")

				if r.hasTrailingHeaderWithSignature {
					stringToSign := generateStringToSignForTrailerChunk(r.timestamp, r.scope, r.previousSignature, checksumHeader)
					calculatedSignature := createSignature(r.signingKey, stringToSign)
					isSignatureValid := subtle.ConstantTimeCompare([]byte(trailerSignature), []byte(calculatedSignature)) == 1
					if !isSignatureValid {
						slog.DebugContext(r.ctx, "Trailing header signature does not match calculated signature")
						return 0, ErrChunkSignatureMismatch
					}
				}
			}
			return 0, io.EOF // End of the chunked transfer
		}
	}

	if len(p) > int(r.chunkBytesRemaining) {
		p = p[:r.chunkBytesRemaining] // Limit the read to the remaining bytes in the chunk
	}
	n, err = io.ReadFull(r.innerBuf, p)
	if !r.skipChunkValidation {
		r.chunkHasher.Write(p[:n])
	}
	r.chunkBytesRemaining -= int64(n)
	if r.chunkBytesRemaining == 0 {
		_, err := r.innerBuf.Discard(2) // Discard the trailing \r\n
		if err != nil {
			return 0, err
		}
		if !r.skipChunkValidation {
			err = r.validateSignature()
			if err != nil {
				return 0, err
			}
		}
	}
	return n, err
}

func (r *awsChunkReadCloser) Close() error {
	err := r.innerCloser.Close()
	if err != nil {
		return err
	}
	return nil
}

type Credentials struct {
	AccessKeyId     string
	SecretAccessKey string
}

type IsAuthenticatedContextKey struct{}

func authTypeForRequest(r *http.Request) string {
	if isAnonymousRequest(r) {
		return "anonymous"
	}
	if r.Header.Get("Authorization") != "" {
		return "sigv4-header"
	}
	if r.URL.Query().Get("X-Amz-Credential") != "" {
		return "sigv4-presign"
	}
	return "anonymous"
}

func isAnonymousRequest(r *http.Request) bool {
	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader != "" {
		return false
	}
	query := r.URL.Query()
	credential := query.Get("X-Amz-Credential")
	if credential != "" {
		return false
	}
	return true
}

func MakeSignatureMiddleware(validCredentials []Credentials, region string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If the request has no authentication credentials at all,
		// let it through as an anonymous request. The server handlers
		// will check bucket policies to decide whether to allow access.
		if isAnonymousRequest(r) {
			ctx := context.WithValue(r.Context(), IsAuthenticatedContextKey{}, false)
			ctx = context.WithValue(ctx, AuthTypeContextKey{}, authTypeForRequest(r))
			r = r.Clone(ctx)
			next.ServeHTTP(w, r)
			return
		}

		usedAccessKeyId, isAuthenticated := checkAuthentication(validCredentials, region, r)
		if isAuthenticated {
			ctx := context.WithValue(r.Context(), AccessKeyIdContextKey{}, *usedAccessKeyId)
			ctx = context.WithValue(ctx, IsAuthenticatedContextKey{}, true)
			ctx = context.WithValue(ctx, AuthTypeContextKey{}, authTypeForRequest(r))
			r = r.Clone(ctx)
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(401)
		}
	})
}
