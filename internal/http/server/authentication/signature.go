package authentication

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ioutils"
)

const contentSHA256Header = "x-amz-content-sha256"
const contentSHA256UnsignedPayload = "UNSIGNED-PAYLOAD"
const contentSHA256StreamingUnsignedPayload = "STREAMING-UNSIGNED-PAYLOAD"
const contentSHA256StreamingUnsignedPayloadTrailing = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
const contentSHA256StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
const contentSHA256StreamingPayloadTrailing = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
const contentSHA256StreamingECDSAPayload = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
const contentSHA256StreamingECDSAPayloadTrailing = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"

const signatureAlgorithmV4 = "AWS4-HMAC-SHA256"
const signatureAlgorithmV4a = "AWS4-ECDSA-P256-SHA256"
const sigV4aSecretKeyPrefix = "AWS4A"
const expectedService = "s3"
const expectedRequest = "aws4_request"

const contentEncodingAwsChunked = "aws-chunked"

const trailerHeader = "x-amz-trailer"
const checksumTrailerPrefix = "x-amz-checksum-"

// maxMemoryCacheSize is the maximum size of a payload that will be cached in memory
// before switching to a disk-based cache.
const maxMemoryCacheSize = 10 * 1000 * 1000

var ErrChunkSignatureMismatch = errors.New("chunk signature mismatch")

// ErrTrailerChecksumMismatch is returned when the checksum declared in the
// trailer of an aws-chunked upload does not match the received payload.
// Its text is the S3 error code reported to the client.
var ErrTrailerChecksumMismatch = errors.New("BadDigest")

// ErrMalformedTrailer is returned when the trailer of an aws-chunked upload
// is missing, unsupported, or does not match the declared x-amz-trailer header.
// Its text is the S3 error code reported to the client.
var ErrMalformedTrailer = errors.New("MalformedTrailerError")

type AccessKeyIdContextKey struct{}
type AuthTypeContextKey struct{}
type RequestIDContextKey struct{}
type ClientIPContextKey struct{}

type signatureAlgorithm string

type credentialScope struct {
	date    string
	region  string
	service string
	request string
	value   string
}

type signatureVerifier struct {
	algorithm       signatureAlgorithm
	verifySignature func(stringToSign string, signature string) bool
}

func parseSignatureAlgorithm(value string) (signatureAlgorithm, bool) {
	algorithm := signatureAlgorithm(value)
	return algorithm, algorithm == signatureAlgorithmV4 || algorithm == signatureAlgorithmV4a
}

func (a signatureAlgorithm) parseCredentialScope(parts []string, expectedRegion string, regionSet string) (credentialScope, error) {
	switch a {
	case signatureAlgorithmV4:
		if len(parts) != 5 {
			return credentialScope{}, fmt.Errorf("SigV4 credential field must contain exactly 5 parts")
		}
		if parts[2] != expectedRegion {
			return credentialScope{}, fmt.Errorf("region in credential does not match expected region")
		}
		return credentialScope{date: parts[1], region: parts[2], service: parts[3], request: parts[4], value: strings.Join(parts[1:], "/")}, nil
	case signatureAlgorithmV4a:
		if len(parts) != 4 {
			return credentialScope{}, fmt.Errorf("SigV4a credential field must contain exactly 4 parts")
		}
		if !regionSetIncludes(regionSet, expectedRegion) {
			return credentialScope{}, fmt.Errorf("expected region is not included in X-Amz-Region-Set")
		}
		return credentialScope{date: parts[1], service: parts[2], request: parts[3], value: strings.Join(parts[1:], "/")}, nil
	default:
		return credentialScope{}, fmt.Errorf("unsupported signature algorithm")
	}
}

func (a signatureAlgorithm) newVerifier(accessKeyID string, secretAccessKey string, scope credentialScope) (signatureVerifier, error) {
	if a == signatureAlgorithmV4 {
		return newSigV4Verifier(createSigningKey(secretAccessKey, scope.date, scope.region, scope.service, scope.request)), nil
	}
	publicKey, err := deriveSigV4aPublicKey(accessKeyID, secretAccessKey)
	if err != nil {
		return signatureVerifier{}, err
	}
	return newSigV4aVerifier(publicKey), nil
}

func (a signatureAlgorithm) validateSignedHeaders(signedHeaders []string, isPresigned bool) error {
	if a == signatureAlgorithmV4a && !isPresigned && !slices.Contains(signedHeaders, "x-amz-region-set") {
		return fmt.Errorf("SigV4a signed headers do not include x-amz-region-set")
	}
	return nil
}

func (v signatureVerifier) verify(stringToSign string, signature string) bool {
	return v.verifySignature(stringToSign, signature)
}

func (v signatureVerifier) normalizeStreamingSignature(signature string) string {
	if v.algorithm == signatureAlgorithmV4a {
		return strings.TrimRight(signature, "*")
	}
	return signature
}

func (v signatureVerifier) acceptsStreamingPayload(contentSHA256 string) bool {
	if v.algorithm == signatureAlgorithmV4a {
		return contentSHA256 != contentSHA256StreamingPayload && contentSHA256 != contentSHA256StreamingPayloadTrailing
	}
	return contentSHA256 != contentSHA256StreamingECDSAPayload && contentSHA256 != contentSHA256StreamingECDSAPayloadTrailing
}

func newSigV4Verifier(signingKey []byte) signatureVerifier {
	return signatureVerifier{
		algorithm: signatureAlgorithmV4,
		verifySignature: func(stringToSign string, signature string) bool {
			calculatedSignature := createSignature(signingKey, stringToSign)
			return subtle.ConstantTimeCompare([]byte(signature), []byte(calculatedSignature)) == 1
		},
	}
}

func hmacSha256(secret []byte, data []byte) []byte {
	hmac := hmac.New(sha256.New, secret)
	hmac.Write(data)
	return hmac.Sum(nil)
}

func createSigningKey(secretAccessKey string, date string, region string, service string, request string) []byte {
	dateKey := hmacSha256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSha256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSha256(dateRegionKey, []byte(service))
	return hmacSha256(dateRegionServiceKey, []byte(request))
}

func createSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(hmacSha256(signingKey, []byte(stringToSign)))
}

func createScope(date string, region string, service string, request string) string {
	return date + "/" + region + "/" + service + "/" + request
}

func newSigV4aVerifier(publicKey *ecdsa.PublicKey) signatureVerifier {
	return signatureVerifier{
		algorithm: signatureAlgorithmV4a,
		verifySignature: func(stringToSign string, signature string) bool {
			return verifySigV4aSignature(publicKey, stringToSign, signature)
		},
	}
}

// deriveSigV4aPrivateKey deterministically derives the P-256 key pair used by
// SigV4a from an AWS access-key pair. The construction follows NIST SP 800-108
// counter-mode KDF and FIPS 186-4 Appendix B.4.2, as used by the AWS SDKs.
func deriveSigV4aPrivateKey(accessKeyID string, secretAccessKey string) (*ecdsa.PrivateKey, error) {
	curve := elliptic.P256()
	nMinusTwo := new(big.Int).Sub(curve.Params().N, big.NewInt(2))
	nMinusTwoBytes := nMinusTwo.FillBytes(make([]byte, sha256.Size))
	inputKey := []byte(sigV4aSecretKeyPrefix + secretAccessKey)

	for counter := 1; counter <= 0xff; counter++ {
		context := make([]byte, 0, len(accessKeyID)+1)
		context = append(context, accessKeyID...)
		context = append(context, byte(counter))

		candidate := sigV4aKDF(inputKey, []byte(signatureAlgorithmV4a), context)
		if constantTimeByteCompare(candidate, nMinusTwoBytes) >= 0 {
			continue
		}

		d := new(big.Int).SetBytes(candidate)
		d.Add(d, big.NewInt(1))
		x, y := curve.ScalarBaseMult(d.Bytes())
		return &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{Curve: curve, X: x, Y: y},
			D:         d,
		}, nil
	}

	return nil, fmt.Errorf("failed to derive SigV4a key after 255 attempts")
}

func deriveSigV4aPublicKey(accessKeyID string, secretAccessKey string) (*ecdsa.PublicKey, error) {
	privateKey, err := deriveSigV4aPrivateKey(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, err
	}
	return &privateKey.PublicKey, nil
}

// sigV4aKDF is the single-block NIST SP 800-108 HMAC-SHA256 counter-mode KDF
// used to produce a 256-bit P-256 private-key candidate.
func sigV4aKDF(key []byte, label []byte, context []byte) []byte {
	fixedInput := make([]byte, 0, 4+len(label)+1+len(context)+4)
	fixedInput = binary.BigEndian.AppendUint32(fixedInput, 1)
	fixedInput = append(fixedInput, label...)
	fixedInput = append(fixedInput, 0)
	fixedInput = append(fixedInput, context...)
	fixedInput = binary.BigEndian.AppendUint32(fixedInput, 256)
	return hmacSha256(key, fixedInput)
}

// constantTimeByteCompare compares equal-length, big-endian unsigned integers.
func constantTimeByteCompare(x []byte, y []byte) int {
	xLarger, yLarger := 0, 0
	for i := range x {
		xByte, yByte := int(x[i]), int(y[i])
		xGreater := ((yByte - xByte) >> 8) & 1
		yGreater := ((xByte - yByte) >> 8) & 1
		xLarger |= xGreater &^ yLarger
		yLarger |= yGreater &^ xLarger
	}
	return xLarger - yLarger
}

func verifySigV4aSignature(publicKey *ecdsa.PublicKey, stringToSign string, signature string) bool {
	decodedSignature, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(stringToSign))
	return ecdsa.VerifyASN1(publicKey, digest[:], decodedSignature)
}

func regionSetIncludes(regionSet string, expectedRegion string) bool {
	for regionPattern := range strings.SplitSeq(regionSet, ",") {
		regionPattern = strings.TrimSpace(regionPattern)
		if regionPattern != "" && wildcardMatch(regionPattern, expectedRegion) {
			return true
		}
	}
	return false
}

func createSigV4aScope(date string, service string, request string) string {
	return date + "/" + service + "/" + request
}

// wildcardMatch implements the '*' wildcard supported by X-Amz-Region-Set.
func wildcardMatch(pattern string, value string) bool {
	patternIndex, valueIndex := 0, 0
	starIndex, starValueIndex := -1, 0

	for valueIndex < len(value) {
		if patternIndex < len(pattern) && pattern[patternIndex] == value[valueIndex] {
			patternIndex++
			valueIndex++
			continue
		}
		if patternIndex < len(pattern) && pattern[patternIndex] == '*' {
			starIndex = patternIndex
			patternIndex++
			starValueIndex = valueIndex
			continue
		}
		if starIndex >= 0 {
			patternIndex = starIndex + 1
			starValueIndex++
			valueIndex = starValueIndex
			continue
		}
		return false
	}

	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}
	return patternIndex == len(pattern)
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

	var canonicalURI strings.Builder
	canonicalURI.Grow(len(escapedPath))
	for idx := 0; idx < len(escapedPath); idx++ {
		ch := escapedPath[idx]
		if ch == '/' {
			canonicalURI.WriteByte('/')
			continue
		}

		if ch == '%' && idx+2 < len(escapedPath) && isHexChar(escapedPath[idx+1]) && isHexChar(escapedPath[idx+2]) {
			canonicalURI.WriteByte('%')
			canonicalURI.WriteByte(upperHexChar(escapedPath[idx+1]))
			canonicalURI.WriteByte(upperHexChar(escapedPath[idx+2]))
			idx += 2
			continue
		}

		if isUnreservedChar(ch) {
			canonicalURI.WriteByte(ch)
			continue
		}

		fmt.Fprintf(&canonicalURI, "%%%02X", ch)
	}

	return canonicalURI.String()
}

func upperHexChar(ch byte) byte {
	if ch >= 'a' && ch <= 'f' {
		return ch - 'a' + 'A'
	}
	return ch
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

	var canonicalQueryString strings.Builder
	for idx, queryStringPair := range queryStrings {
		if idx > 0 {
			canonicalQueryString.WriteByte('&')
		}
		canonicalQueryString.WriteString(queryStringPair.key)
		canonicalQueryString.WriteByte('=')
		canonicalQueryString.WriteString(queryStringPair.val)
	}
	return canonicalQueryString.String()
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

// collectSignedHeaders returns the headers participating in the signature,
// lowercased and sorted by key, shared by the canonical-headers and
// signed-headers serializations.
func collectSignedHeaders(r *http.Request, headersToInclude []string) []pair {
	headers := make([]pair, 0, len(headersToInclude)+1)

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
	return headers
}

func generateCanonicalHeaders(r *http.Request, headersToInclude []string) string {
	var canonicalHeaders strings.Builder
	for _, header := range collectSignedHeaders(r, headersToInclude) {
		canonicalHeaders.WriteString(header.key)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(header.val)
		canonicalHeaders.WriteByte('\n')
	}
	return canonicalHeaders.String()
}

func generateSignedHeaders(r *http.Request, headersToInclude []string) string {
	var signedHeaders strings.Builder
	for idx, header := range collectSignedHeaders(r, headersToInclude) {
		if idx > 0 {
			signedHeaders.WriteByte(';')
		}
		signedHeaders.WriteString(header.key)
	}
	return signedHeaders.String()
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
	if isPresigned {
		canonicalRequest += contentSHA256UnsignedPayload
	} else {
		switch contentSHA256 {
		case contentSHA256UnsignedPayload,
			contentSHA256StreamingUnsignedPayload,
			contentSHA256StreamingUnsignedPayloadTrailing,
			contentSHA256StreamingPayload,
			contentSHA256StreamingPayloadTrailing,
			contentSHA256StreamingECDSAPayload,
			contentSHA256StreamingECDSAPayloadTrailing:
			canonicalRequest += contentSHA256
		default:
			hashedPayload, err := generateHashedPayload(r)
			if err != nil {
				return nil, err
			}
			canonicalRequest += *hashedPayload
		}
	}
	return &canonicalRequest, nil
}

func generateStringToSign(r *http.Request, timestamp string, scope string, headersToInclude []string, isPresigned bool, algorithm signatureAlgorithm) (*string, error) {
	canonicalRequest, err := generateCanonicalRequest(r, headersToInclude, isPresigned)
	if err != nil {
		return nil, err
	}
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(*canonicalRequest))
	dataSha256 := sha256Hash.Sum(nil)
	canonicalRequestHexSha256 := hex.EncodeToString(dataSha256)

	stringToSign := string(algorithm) + "\n" + timestamp + "\n" + scope + "\n" + canonicalRequestHexSha256
	return &stringToSign, nil
}

func generateStringToSignForChunk(algorithm signatureAlgorithm, timestamp string, scope string, previousSignature string, chunkHasher hash.Hash) string {
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(""))
	dataSha256 := sha256Hash.Sum(nil)
	emptyHashHex := hex.EncodeToString(dataSha256)

	return string(algorithm) + "-PAYLOAD" + "\n" + timestamp + "\n" + scope + "\n" + previousSignature + "\n" + emptyHashHex + "\n" + hex.EncodeToString(chunkHasher.Sum(nil))
}

func generateStringToSignForTrailerChunk(algorithm signatureAlgorithm, timestamp string, scope string, previousSignature string, trailingChecksumHeader string) string {
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(trailingChecksumHeader + "\n"))
	dataSha256 := sha256Hash.Sum(nil)
	hexHash := hex.EncodeToString(dataSha256)
	return string(algorithm) + "-TRAILER" + "\n" + timestamp + "\n" + scope + "\n" + previousSignature + "\n" + hexHash
}

func hasAwsChunkedContentEncoding(contentEncodingHeader string) bool {
	firstEncoding, _, _ := strings.Cut(contentEncodingHeader, ",")
	return strings.EqualFold(strings.TrimSpace(firstEncoding), contentEncodingAwsChunked)
}

func stripAwsChunkedContentEncoding(contentEncodingHeader string) string {
	encodings := strings.Split(contentEncodingHeader, ",")
	if len(encodings) == 0 || !strings.EqualFold(strings.TrimSpace(encodings[0]), contentEncodingAwsChunked) {
		return strings.TrimSpace(contentEncodingHeader)
	}

	remainingEncodings := make([]string, 0, len(encodings)-1)
	for _, encoding := range encodings[1:] {
		encoding = strings.TrimSpace(encoding)
		if encoding != "" {
			remainingEncodings = append(remainingEncodings, encoding)
		}
	}
	return strings.Join(remainingEncodings, ", ")
}

type signatureParameters struct {
	algorithm          signatureAlgorithm
	credential         string
	timestamp          string
	expirationDuration time.Duration
	signedHeaders      string
	signature          string
	isPresigned        bool
}

func parseSignatureParameters(r *http.Request) (signatureParameters, error) {
	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader == "" {
		slog.DebugContext(r.Context(), "Authorization header is missing checking for query parameters")
		query := r.URL.Query()
		algorithm, found := parseSignatureAlgorithm(query.Get("X-Amz-Algorithm"))
		if !found {
			return signatureParameters{}, fmt.Errorf("X-Amz-Algorithm is not supported")
		}

		expires, err := strconv.ParseInt(query.Get("X-Amz-Expires"), 10, 32)
		if err != nil {
			return signatureParameters{}, fmt.Errorf("failed to parse X-Amz-Expires: %w", err)
		}
		if expires < 1 || expires > 604800 {
			return signatureParameters{}, fmt.Errorf("X-Amz-Expires must be between 1 and 604800 seconds")
		}

		slog.DebugContext(r.Context(), "Using presigned auth query parameters")
		return signatureParameters{
			algorithm:          algorithm,
			credential:         query.Get("X-Amz-Credential"),
			timestamp:          query.Get("X-Amz-Date"),
			expirationDuration: time.Duration(expires) * time.Second,
			signedHeaders:      query.Get("X-Amz-SignedHeaders"),
			signature:          query.Get("X-Amz-Signature"),
			isPresigned:        true,
		}, nil
	}

	slog.DebugContext(r.Context(), "Authorization header is present")
	algorithm, fields, found := strings.Cut(authorizationHeader, " ")
	if !found {
		return signatureParameters{}, fmt.Errorf("Authorization header does not contain signature fields")
	}
	signatureAlgorithm, found := parseSignatureAlgorithm(algorithm)
	if !found {
		return signatureParameters{}, fmt.Errorf("Authorization header uses an unsupported signature algorithm")
	}

	authFields := strings.Split(fields, ",")
	if len(authFields) != 3 {
		return signatureParameters{}, fmt.Errorf("Authorization header does not contain exactly 3 fields")
	}
	credential, found := strings.CutPrefix(strings.TrimSpace(authFields[0]), "Credential=")
	if !found {
		return signatureParameters{}, fmt.Errorf("Authorization header does not contain Credential field")
	}
	signedHeaders, found := strings.CutPrefix(strings.TrimSpace(authFields[1]), "SignedHeaders=")
	if !found {
		return signatureParameters{}, fmt.Errorf("Authorization header does not contain SignedHeaders field")
	}
	signature, found := strings.CutPrefix(strings.TrimSpace(authFields[2]), "Signature=")
	if !found {
		return signatureParameters{}, fmt.Errorf("Authorization header does not contain Signature field")
	}

	timestamp := r.Header.Get("x-amz-date")
	if timestamp == "" {
		// Use the standard Date header if x-amz-date is not specified.
		timestamp = r.Header.Get("Date")
	}
	return signatureParameters{
		algorithm:          signatureAlgorithm,
		credential:         credential,
		timestamp:          timestamp,
		expirationDuration: 5 * time.Minute,
		signedHeaders:      signedHeaders,
		signature:          signature,
	}, nil
}

func checkAuthentication(validCredentials []Credentials, expectedRegion string, r *http.Request) (usedAccessKeyId *string, authenticated bool) {
	now := time.Now().UTC()
	contentEncodingHeader := r.Header.Get("Content-Encoding")
	isAwsChunked := hasAwsChunkedContentEncoding(contentEncodingHeader)

	parameters, err := parseSignatureParameters(r)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to parse signature parameters: "+err.Error())
		return nil, false
	}

	accessKeyIdAndScope := strings.Split(parameters.credential, "/")
	regionSet := r.Header.Get("x-amz-region-set")
	if parameters.isPresigned {
		regionSet = r.URL.Query().Get("X-Amz-Region-Set")
	}
	scope, err := parameters.algorithm.parseCredentialScope(accessKeyIdAndScope, expectedRegion, regionSet)
	if err != nil {
		slog.DebugContext(r.Context(), "Invalid credential scope: "+err.Error())
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
	if scope.service != expectedService {
		slog.DebugContext(r.Context(), "Service in credential does not match expected service")
		return nil, false
	}
	if scope.request != expectedRequest {
		slog.DebugContext(r.Context(), "Request in credential does not match expected request")
		return nil, false
	}

	parsedTimestamp, err := time.Parse("20060102T150405Z", parameters.timestamp)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to parse timestamp: "+err.Error())
		return nil, false
	}
	if scope.date != parsedTimestamp.Format("20060102") {
		slog.DebugContext(r.Context(), "Date in credential does not match signing timestamp")
		return nil, false
	}

	beforeTimestamp := parsedTimestamp.Add(-15 * time.Minute)
	expiredTimestamp := parsedTimestamp.Add(parameters.expirationDuration)
	if now.Before(beforeTimestamp) || now.After(expiredTimestamp) {
		slog.DebugContext(r.Context(), "Timestamp is not within the valid range ("+beforeTimestamp.Format(time.RFC3339)+" - "+expiredTimestamp.Format(time.RFC3339)+")")
		return nil, false
	}

	rawSignedHeadersArray := strings.Split(parameters.signedHeaders, ";")
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
	if err := parameters.algorithm.validateSignedHeaders(signedHeadersArray, parameters.isPresigned); err != nil {
		slog.DebugContext(r.Context(), "Invalid signed headers: "+err.Error())
		return nil, false
	}
	for headerKey := range r.Header {
		headerKey = strings.ToLower(headerKey)
		if mustBeSignedHeader(headerKey) && !slices.Contains(signedHeadersArray, headerKey) {
			slog.DebugContext(r.Context(), "Request contains unsigned security-sensitive header", "header", headerKey)
			return nil, false
		}
	}

	stringToSign, err := generateStringToSign(r, parameters.timestamp, scope.value, signedHeadersArray, parameters.isPresigned, parameters.algorithm)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to generate string to sign: "+err.Error())
		return nil, false
	}
	verifier, err := parameters.algorithm.newVerifier(accessKeyId, expectedCredentials.SecretAccessKey, scope)
	if err != nil {
		slog.DebugContext(r.Context(), "Failed to create signature verifier: "+err.Error())
		return nil, false
	}
	isSignatureValid := verifier.verify(*stringToSign, parameters.signature)
	if !isSignatureValid {
		slog.DebugContext(r.Context(), "Signature does not match calculated signature")
		return nil, false
	}

	if isAwsChunked {
		slog.DebugContext(r.Context(), "Request is using AWS Chunked Transfer Encoding")
		contentSHA256 := r.Header.Get(contentSHA256Header)
		if !verifier.acceptsStreamingPayload(contentSHA256) {
			slog.DebugContext(r.Context(), "Streaming payload algorithm does not match request signature algorithm")
			return nil, false
		}
		// aws-chunked is a transport encoding, not object metadata: strip it
		// whether it is the only encoding or the first of several.
		contentEncodingHeader = stripAwsChunkedContentEncoding(contentEncodingHeader)
		if contentEncodingHeader != "" {
			r.Header.Set("Content-Encoding", contentEncodingHeader)
		} else {
			r.Header.Del("Content-Encoding")
		}
		r.Header.Set("Content-Length", r.Header.Get("x-amz-decoded-content-length"))
		r.Header.Del("x-amz-decoded-content-length")
		trailingHeader := contentSHA256 == contentSHA256StreamingUnsignedPayloadTrailing || contentSHA256 == contentSHA256StreamingPayloadTrailing || contentSHA256 == contentSHA256StreamingECDSAPayloadTrailing
		hasTrailingHeaderWithSignature := contentSHA256 == contentSHA256StreamingPayloadTrailing || contentSHA256 == contentSHA256StreamingECDSAPayloadTrailing
		skipChunkValidation := contentSHA256 == contentSHA256StreamingUnsignedPayloadTrailing || contentSHA256 == contentSHA256StreamingUnsignedPayload
		trailerChecksumName := strings.ToLower(strings.TrimSpace(r.Header.Get(trailerHeader)))
		r.Body = newAwsChunkReadCloser(r.Context(), r.Body, parameters.timestamp, scope.value, parameters.signature, verifier, trailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation, trailerChecksumName)
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
	verifier                       signatureVerifier
	hasTrailingHeader              bool
	hasTrailingHeaderWithSignature bool
	skipChunkValidation            bool
	trailerChecksumName            string
	trailerHasher                  hash.Hash
}

func newAwsChunkReadCloser(ctx context.Context, inner io.ReadCloser, timestamp string, scope string, previousSignature string, verifier signatureVerifier, hasTrailingHeader bool, hasTrailingHeaderWithSignature bool, skipChunkValidation bool, trailerChecksumName string) *awsChunkReadCloser {
	var trailerHasher hash.Hash
	if hasTrailingHeader {
		trailerHasher, _ = checksumutils.NewChecksumTrailerHash(trailerChecksumName)
	}
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
		verifier:                       verifier,
		hasTrailingHeader:              hasTrailingHeader,
		hasTrailingHeaderWithSignature: hasTrailingHeaderWithSignature,
		skipChunkValidation:            skipChunkValidation,
		trailerChecksumName:            trailerChecksumName,
		trailerHasher:                  trailerHasher,
	}
}

func (r *awsChunkReadCloser) validateSignature() error {
	stringToSign := generateStringToSignForChunk(r.verifier.algorithm, r.timestamp, r.scope, r.previousSignature, r.chunkHasher)
	normalizedSignature := r.verifier.normalizeStreamingSignature(r.chunkSignature)
	isSignatureValid := r.verifier.verify(stringToSign, normalizedSignature)
	if !isSignatureValid {
		slog.DebugContext(r.ctx, "Chunk signature does not match calculated chunk signature")
		return ErrChunkSignatureMismatch
	}

	r.chunkHasher.Reset()
	r.previousSignature = normalizedSignature
	return nil
}

// maxTrailerLines bounds the trailer section: S3 allows a single checksum
// trailer, optionally followed by an x-amz-trailer-signature line.
const maxTrailerLines = 8

// readTrailerSection reads the trailer lines following the zero-length chunk
// and returns the checksum trailer line and the trailer signature (without its
// header name). The section is terminated by a blank line or EOF. A single
// blank line directly after the zero-length chunk is tolerated, because some
// clients terminate the zero-length chunk with its own \r\n before the
// trailers, mirroring the framing of the data chunks.
func (r *awsChunkReadCloser) readTrailerSection() (checksumHeader string, trailerSignature string) {
	for i := range maxTrailerLines {
		line, lineErr := r.innerBuf.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			if i == 0 && lineErr == nil {
				continue
			}
			break
		}
		if value, found := strings.CutPrefix(line, "x-amz-trailer-signature:"); found {
			trailerSignature = strings.TrimSpace(value)
		} else if checksumHeader == "" {
			checksumHeader = line
		}
		if lineErr != nil {
			break
		}
	}
	return checksumHeader, trailerSignature
}

// validateTrailerChecksum checks the x-amz-checksum-* trailer line against the
// checksum of the decoded payload. The trailer must match the algorithm the
// client declared in the x-amz-trailer header.
func (r *awsChunkReadCloser) validateTrailerChecksum(checksumHeader string) error {
	if r.trailerHasher == nil {
		if strings.HasPrefix(r.trailerChecksumName, checksumTrailerPrefix) {
			// A checksum trailer was declared, but with an algorithm we cannot
			// verify. Reject instead of storing unverified data.
			slog.DebugContext(r.ctx, "Unsupported checksum trailer declared", "trailer", r.trailerChecksumName)
			return ErrMalformedTrailer
		}
		return nil
	}
	name, value, found := strings.Cut(checksumHeader, ":")
	if !found || strings.ToLower(strings.TrimSpace(name)) != r.trailerChecksumName {
		slog.DebugContext(r.ctx, "Checksum trailer does not match declared x-amz-trailer header", "trailer", checksumHeader)
		return ErrMalformedTrailer
	}
	calculatedChecksum := base64.StdEncoding.EncodeToString(r.trailerHasher.Sum(nil))
	if strings.TrimSpace(value) != calculatedChecksum {
		slog.DebugContext(r.ctx, "Trailer checksum does not match calculated payload checksum", "trailer", r.trailerChecksumName)
		return ErrTrailerChecksumMismatch
	}
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
			if !r.skipChunkValidation {
				err = r.validateSignature()
				if err != nil {
					return 0, err
				}
			}
			if r.hasTrailingHeader {
				checksumHeader, trailerSignature := r.readTrailerSection()
				slog.DebugContext(r.ctx, "Validating trailing headers")

				if r.hasTrailingHeaderWithSignature {
					stringToSign := generateStringToSignForTrailerChunk(r.verifier.algorithm, r.timestamp, r.scope, r.previousSignature, checksumHeader)
					trailerSignature = r.verifier.normalizeStreamingSignature(trailerSignature)
					isSignatureValid := r.verifier.verify(stringToSign, trailerSignature)
					if !isSignatureValid {
						slog.DebugContext(r.ctx, "Trailing header signature does not match calculated signature")
						return 0, ErrChunkSignatureMismatch
					}
				}

				err = r.validateTrailerChecksum(checksumHeader)
				if err != nil {
					return 0, err
				}
			} else {
				_, err := r.innerBuf.Discard(2) // Discard the final \r\n
				if err != nil {
					return 0, err
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
	if r.trailerHasher != nil {
		r.trailerHasher.Write(p[:n])
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
