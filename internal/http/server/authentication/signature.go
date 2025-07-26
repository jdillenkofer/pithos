package authentication

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type AccessKeyIdContextKey struct{}

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
	return r.URL.EscapedPath()
}

func uriEncode(input string) string {
	output := url.QueryEscape(input)
	/* @TODO: make sure that the uriEncode follows AWS guidelines (non standard)
	   if ("+".equals(replacement)) {
	       replacement = "%20";
	   } else if ("*".equals(replacement)) {
	       replacement = "%2A";
	   } else if ("%7E".equals(replacement)) {
	       replacement = "~";
	   } else if (path && "%2F".equals(replacement)) {
	       replacement = "/";
	   }
	*/
	return output
}

func generateCanonicalQueryString(r *http.Request) string {
	queryStrings := []pair{}
	for queryKey, queryValues := range r.URL.Query() {
		if queryKey == "X-Amz-Signature" {
			continue
		}
		for _, queryVal := range queryValues {
			queryStrings = append(queryStrings, pair{
				key: queryKey,
				val: queryVal,
			})
		}
	}
	slices.SortFunc(queryStrings, func(a, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	canonicalQueryString := ""
	for idx, queryStringPair := range queryStrings {
		canonicalQueryString += uriEncode(queryStringPair.key) + "=" + uriEncode(queryStringPair.val)
		if idx < len(queryStrings)-1 {
			canonicalQueryString += "&"
		}
	}
	return canonicalQueryString
}

func includeInCanonicalHeaders(headerKey string, headersToInclude []string) bool {
	if slices.Contains(headersToInclude, headerKey) {
		return true
	}
	if headerKey == "content-type" {
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

func generateHashedPayload(r *http.Request) string {
	// @TODO: error handling
	// @TODO: cache request body to disk
	bodyBytes, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	sha256Hash := sha256.New()
	sha256Hash.Write(bodyBytes)
	dataSha256 := sha256Hash.Sum(nil)
	return hex.EncodeToString(dataSha256)
}

func generateCanonicalRequest(r *http.Request, headersToInclude []string, isPresigned bool) string {
	canonicalRequest := generateCanonicalHttpMethod(r) + "\n"
	canonicalRequest += generateCanonicalURI(r) + "\n"
	canonicalRequest += generateCanonicalQueryString(r) + "\n"
	canonicalRequest += generateCanonicalHeaders(r, headersToInclude) + "\n"
	canonicalRequest += generateSignedHeaders(r, headersToInclude) + "\n"
	if isPresigned || r.Header.Get("x-amz-content-sha256") == "UNSIGNED-PAYLOAD" {
		canonicalRequest += "UNSIGNED-PAYLOAD"
	} else {
		canonicalRequest += generateHashedPayload(r)
	}
	return canonicalRequest
}

func generateStringToSign(r *http.Request, timestamp string, scope string, headersToInclude []string, isPresigned bool) string {
	canonicalRequest := generateCanonicalRequest(r, headersToInclude, isPresigned)
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(canonicalRequest))
	dataSha256 := sha256Hash.Sum(nil)
	canonicalRequestHexSha256 := hex.EncodeToString(dataSha256)

	return "AWS4-HMAC-SHA256" + "\n" + timestamp + "\n" + scope + "\n" + canonicalRequestHexSha256
}

func createScope(date string, region string, service string, request string) string {
	return date + "/" + region + "/" + service + "/" + request
}

func checkAuthentication(validCredentials []Credentials, expectedRegion string, r *http.Request) (usedAccessKeyId *string, authenticated bool) {
	const signatureAlgorithm = "AWS4-HMAC-SHA256"
	const expectedService = "s3"
	const expectedRequest = "aws4_request"
	now := time.Now().UTC()
	expectedDate := now.Format("20060102")

	var credential string
	var timestamp string
	var expirationDuration time.Duration
	var signedHeaders string
	var signature string
	var isPresigned bool

	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader == "" {
		slog.Debug("Authorization header is missing checking for query parameters")
		isPresigned = true
		query := r.URL.Query()
		credential = query.Get("X-Amz-Credential")
		timestamp = query.Get("X-Amz-Date")
		expires := query.Get("X-Amz-Expires")
		slog.Debug("X-Amz-Credential: " + credential + " X-Amz-Date: " + timestamp + " X-Amz-Expires: " + expires)
		parsedExpired, err := strconv.ParseInt(expires, 10, 32)
		if err != nil {
			slog.Warn("Failed to parse X-Amz-Expires: " + err.Error())
			return nil, false
		}
		if parsedExpired < 1 || parsedExpired > 604800 {
			slog.Warn("X-Amz-Expires must be between 1 and 604800 seconds")
			return nil, false
		}
		expirationDuration = time.Duration(parsedExpired) * time.Second
		signedHeaders = query.Get("X-Amz-SignedHeaders")
		signature = query.Get("X-Amz-Signature")
	} else {
		slog.Debug("Authorization header: " + authorizationHeader)
		isPresigned = false
		authorizationHeader, found := strings.CutPrefix(authorizationHeader, signatureAlgorithm)
		if !found {
			slog.Warn("Authorization header does not start with " + signatureAlgorithm)
			return nil, false
		}
		authFields := strings.Split(authorizationHeader, ",")
		if len(authFields) != 3 {
			slog.Warn("Authorization header does not contain exactly 3 fields")
			return nil, false
		}

		credential = strings.TrimSpace(authFields[0])
		credential, found = strings.CutPrefix(credential, "Credential=")
		if !found {
			slog.Warn("Authorization header does not contain Credential field")
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
			slog.Warn("Authorization header does not contain SignedHeaders field")
			return nil, false
		}

		signature = strings.TrimSpace(authFields[2])
		signature, found = strings.CutPrefix(signature, "Signature=")
		if !found {
			slog.Warn("Authorization header does not contain Signature field")
			return nil, false
		}
	}

	accessKeyIdAndScope := strings.Split(credential, "/")
	if len(accessKeyIdAndScope) != 5 {
		slog.Warn("Credential field does not contain exactly 5 parts")
		return nil, false
	}
	accessKeyId := accessKeyIdAndScope[0]
	foundIndex := slices.IndexFunc(validCredentials, func(c Credentials) bool {
		return c.AccessKeyId == accessKeyId
	})
	if foundIndex < 0 {
		slog.Warn("Access key ID not found in valid credentials")
		return nil, false
	}
	expectedCredentials := validCredentials[foundIndex]
	date := accessKeyIdAndScope[1]
	if date != expectedDate {
		slog.Warn("Date in credential does not match expected date")
		return nil, false
	}
	region := accessKeyIdAndScope[2]
	if region != expectedRegion {
		slog.Warn("Region in credential does not match expected region")
		return nil, false
	}

	service := accessKeyIdAndScope[3]
	if service != expectedService {
		slog.Warn("Service in credential does not match expected service")
		return nil, false
	}

	request := accessKeyIdAndScope[4]
	if request != expectedRequest {
		slog.Warn("Request in credential does not match expected request")
		return nil, false
	}

	scope := createScope(expectedDate, region, service, request)

	parsedTimestamp, err := time.Parse("20060102T150405Z", timestamp)
	if err != nil {
		slog.Warn("Failed to parse timestamp: " + err.Error())
		return nil, false
	}
	beforeTimestamp := parsedTimestamp.Add(-15 * time.Minute)
	expiredTimestamp := parsedTimestamp.Add(expirationDuration)
	if now.Before(beforeTimestamp) || now.After(expiredTimestamp) {
		slog.Warn("Timestamp is not within the valid range (" + beforeTimestamp.Format(time.RFC3339) + " - " + expiredTimestamp.Format(time.RFC3339) + ")")
		return nil, false
	}

	signedHeadersArray := strings.Split(signedHeaders, ";")

	stringToSign := generateStringToSign(r, timestamp, scope, signedHeadersArray, isPresigned)
	signingKey := createSigningKey(expectedCredentials.SecretAccessKey, expectedDate, region, expectedService, expectedRequest)
	calculatedSignature := createSignature(signingKey, stringToSign)
	isSignatureValid := signature == calculatedSignature
	if !isSignatureValid {
		slog.Warn("Signature does not match calculated signature")
		slog.Debug("Expected signature: " + calculatedSignature)
		slog.Debug("Received signature: " + signature)
		return nil, false
	}
	return &accessKeyId, isSignatureValid
}

type Credentials struct {
	AccessKeyId     string
	SecretAccessKey string
}

func MakeSignatureMiddleware(validCredentials []Credentials, region string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		usedAccessKeyId, isAuthenticated := checkAuthentication(validCredentials, region, r)
		if isAuthenticated {
			r = r.Clone(context.WithValue(r.Context(), AccessKeyIdContextKey{}, *usedAccessKeyId))
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(401)
		}
	})
}
