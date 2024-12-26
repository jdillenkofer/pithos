package middlewares

import (
	"bytes"
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

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
	bodyBytes, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	sha256Hash := sha256.New()
	sha256Hash.Write(bodyBytes)
	dataSha256 := sha256Hash.Sum(nil)
	return hex.EncodeToString(dataSha256)
}

func generateCanonicalRequest(r *http.Request, headersToInclude []string) string {
	canonicalRequest := generateCanonicalHttpMethod(r) + "\n"
	canonicalRequest += generateCanonicalURI(r) + "\n"
	canonicalRequest += generateCanonicalQueryString(r) + "\n"
	canonicalRequest += generateCanonicalHeaders(r, headersToInclude) + "\n"
	canonicalRequest += generateSignedHeaders(r, headersToInclude) + "\n"
	if r.Header.Get("x-amz-content-sha256") == "UNSIGNED-PAYLOAD" {
		canonicalRequest += "UNSIGNED-PAYLOAD"
	} else {
		canonicalRequest += generateHashedPayload(r)
	}
	return canonicalRequest
}

func generateStringToSign(r *http.Request, timestamp string, scope string, headersToInclude []string) string {
	canonicalRequest := generateCanonicalRequest(r, headersToInclude)
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(canonicalRequest))
	dataSha256 := sha256Hash.Sum(nil)
	canonicalRequestHexSha256 := hex.EncodeToString(dataSha256)

	return "AWS4-HMAC-SHA256" + "\n" + timestamp + "\n" + scope + "\n" + canonicalRequestHexSha256
}

func createScope(date string, region string, service string, request string) string {
	return date + "/" + region + "/" + service + "/" + request
}

func checkAuthentication(expectedAccessKeyId string, expectedSecretAccessKey string, expectedRegion string, r *http.Request) bool {

	signatureAlgorithm := "AWS4-HMAC-SHA256"
	expectedService := "s3"
	expectedRequest := "aws4_request"
	expectedDate := time.Now().UTC().Format("20060102")
	var credential string
	var timestamp string
	var signedHeaders string
	var signature string

	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader == "" {
		query := r.URL.Query()
		credential = query.Get("X-Amz-Credential")
		timestamp = query.Get("X-Amz-Date")
		signedHeaders = query.Get("X-Amz-SignedHeaders")
		signature = query.Get("X-Amz-Signature")
	} else {
		authorizationHeader, found := strings.CutPrefix(authorizationHeader, signatureAlgorithm)
		if !found {
			return false
		}
		authFields := strings.Split(authorizationHeader, ",")
		if len(authFields) != 3 {
			return false
		}

		credential = strings.TrimSpace(authFields[0])
		credential, found = strings.CutPrefix(credential, "Credential=")
		if !found {
			return false
		}

		// @TODO: Use Date header (https://developer.mozilla.org/de/docs/Web/HTTP/Headers/Date) if x-amz-date is not specified
		// @TODO: check if the difference between timestamp and now is small enough
		timestamp = r.Header.Get("x-amz-date")

		signedHeaders = strings.TrimSpace(authFields[1])
		signedHeaders, found = strings.CutPrefix(signedHeaders, "SignedHeaders=")
		if !found {
			return false
		}

		signature = strings.TrimSpace(authFields[2])
		signature, found = strings.CutPrefix(signature, "Signature=")
		if !found {
			return false
		}
	}

	accessKeyIdAndScope := strings.Split(credential, "/")
	if len(accessKeyIdAndScope) != 5 {
		return false
	}
	accessKeyId := accessKeyIdAndScope[0]
	if accessKeyId != expectedAccessKeyId {
		return false
	}
	date := accessKeyIdAndScope[1]
	if date != expectedDate {
		return false
	}
	region := accessKeyIdAndScope[2]
	if region != expectedRegion {
		return false
	}

	service := accessKeyIdAndScope[3]
	if service != expectedService {
		return false
	}

	request := accessKeyIdAndScope[4]
	if request != expectedRequest {
		return false
	}

	scope := createScope(expectedDate, region, service, request)

	signedHeadersArray := strings.Split(signedHeaders, ";")

	stringToSign := generateStringToSign(r, timestamp, scope, signedHeadersArray)
	signingKey := createSigningKey(expectedSecretAccessKey, expectedDate, region, expectedService, expectedRequest)
	calculatedSignature := createSignature(signingKey, stringToSign)
	return signature == calculatedSignature
}

func MakeSignatureMiddleware(accessKeyId string, secretAccessKey string, region string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isAuthenticated := checkAuthentication(accessKeyId, secretAccessKey, region, r)
		if isAuthenticated {
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(401)
		}
	})
}
