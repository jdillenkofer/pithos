package middleware

import (
	"errors"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
)

const (
	originHeader                      = "Origin"
	accessControlRequestMethodHeader  = "Access-Control-Request-Method"
	accessControlRequestHeadersHeader = "Access-Control-Request-Headers"
	accessControlAllowOriginHeader    = "Access-Control-Allow-Origin"
	accessControlAllowMethodsHeader   = "Access-Control-Allow-Methods"
	accessControlAllowHeadersHeader   = "Access-Control-Allow-Headers"
	accessControlExposeHeadersHeader  = "Access-Control-Expose-Headers"
	accessControlMaxAgeHeader         = "Access-Control-Max-Age"
	accessControlAllowCredentials     = "Access-Control-Allow-Credentials"
	varyHeader                        = "Vary"
)

var validCORSMethods = []string{"GET", "PUT", "POST", "DELETE", "HEAD", "PATCH", "OPTIONS"}

type CORSRule = storage.CORSRule

type CORSRulesResolver func(r *http.Request) []CORSRule

func NormalizeAndValidateCORSRules(rules []CORSRule) ([]CORSRule, error) {
	normalized := make([]CORSRule, 0, len(rules))
	for idx, rule := range rules {
		origins := normalizeValues(rule.AllowedOrigins)
		if len(origins) == 0 {
			return nil, errors.New("cors rule " + strconv.Itoa(idx) + " has no allowedOrigins")
		}

		methods := normalizeMethods(rule.AllowedMethods)
		if len(methods) == 0 {
			return nil, errors.New("cors rule " + strconv.Itoa(idx) + " has no allowedMethods")
		}
		for _, method := range methods {
			if !slices.Contains(validCORSMethods, method) {
				return nil, errors.New("cors rule " + strconv.Itoa(idx) + " has invalid allowed method: " + method)
			}
		}

		if rule.AllowCredentials && slices.Contains(origins, "*") {
			return nil, errors.New("cors rule " + strconv.Itoa(idx) + " cannot combine allowCredentials with wildcard origin")
		}

		headers := normalizeValues(rule.AllowedHeaders)
		exposeHeaders := normalizeValues(rule.ExposeHeaders)

		normalized = append(normalized, CORSRule{
			AllowedOrigins:   origins,
			AllowedMethods:   methods,
			AllowedHeaders:   headers,
			ExposeHeaders:    exposeHeaders,
			MaxAgeSeconds:    rule.MaxAgeSeconds,
			AllowCredentials: rule.AllowCredentials,
		})
	}

	return normalized, nil
}

func MakeCORSMiddleware(rules []CORSRule, next http.Handler) http.Handler {
	return MakeCORSMiddlewareWithResolver(func(_ *http.Request) []CORSRule {
		return rules
	}, next)
}

func MakeCORSMiddlewareWithResolver(resolveRules CORSRulesResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get(originHeader))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		rules := resolveRules(r)
		if len(rules) == 0 {
			if isPreflightRequest(r) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if isPreflightRequest(r) {
			appendVary(w.Header(), originHeader)
			appendVary(w.Header(), accessControlRequestMethodHeader)
			appendVary(w.Header(), accessControlRequestHeadersHeader)
		} else {
			appendVary(w.Header(), originHeader)
		}

		requestedMethod := r.Method
		if isPreflightRequest(r) {
			requestedMethod = strings.TrimSpace(strings.ToUpper(r.Header.Get(accessControlRequestMethodHeader)))
		}

		requestedHeaders := parseHeaderList(r.Header.Get(accessControlRequestHeadersHeader))

		matchedRule, matchedOriginPattern, ok := findMatchingRule(rules, origin, requestedMethod, requestedHeaders, isPreflightRequest(r))
		if !ok {
			if isPreflightRequest(r) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		allowOriginValue := origin
		if matchedOriginPattern == "*" {
			allowOriginValue = "*"
		}

		headers := w.Header()
		headers.Set(accessControlAllowOriginHeader, allowOriginValue)
		if matchedRule.AllowCredentials {
			headers.Set(accessControlAllowCredentials, "true")
		}

		if isPreflightRequest(r) {
			headers.Set(accessControlAllowMethodsHeader, strings.Join(matchedRule.AllowedMethods, ", "))
			allowHeadersValue := preflightAllowHeadersValue(matchedRule.AllowedHeaders, requestedHeaders)
			if allowHeadersValue != "" {
				headers.Set(accessControlAllowHeadersHeader, allowHeadersValue)
			}
			if matchedRule.MaxAgeSeconds != nil {
				headers.Set(accessControlMaxAgeHeader, strconv.Itoa(*matchedRule.MaxAgeSeconds))
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		if len(matchedRule.ExposeHeaders) > 0 {
			headers.Set(accessControlExposeHeadersHeader, strings.Join(matchedRule.ExposeHeaders, ", "))
		}
		next.ServeHTTP(w, r)
	})
}

func isPreflightRequest(r *http.Request) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	return strings.TrimSpace(r.Header.Get(accessControlRequestMethodHeader)) != ""
}

func findMatchingRule(rules []CORSRule, origin string, method string, requestedHeaders []string, preflight bool) (*CORSRule, string, bool) {
	for idx := range rules {
		rule := &rules[idx]
		matchedOriginPattern, originMatches := matchOrigin(rule.AllowedOrigins, origin)
		if !originMatches {
			continue
		}
		if !matchMethod(rule.AllowedMethods, method) {
			continue
		}
		if preflight && !matchRequestedHeaders(rule.AllowedHeaders, requestedHeaders) {
			continue
		}
		return rule, matchedOriginPattern, true
	}
	return nil, "", false
}

func matchOrigin(allowedOrigins []string, origin string) (string, bool) {
	for _, allowedOrigin := range allowedOrigins {
		if wildcardMatch(strings.ToLower(allowedOrigin), strings.ToLower(origin)) {
			return allowedOrigin, true
		}
	}
	return "", false
}

func matchMethod(allowedMethods []string, method string) bool {
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	for _, allowedMethod := range allowedMethods {
		if allowedMethod == normalizedMethod {
			return true
		}
	}
	return false
}

func matchRequestedHeaders(allowedHeaders []string, requestedHeaders []string) bool {
	if len(requestedHeaders) == 0 {
		return true
	}
	if slices.Contains(allowedHeaders, "*") {
		return true
	}
	for _, requestedHeader := range requestedHeaders {
		matched := false
		for _, allowedHeader := range allowedHeaders {
			if wildcardMatch(strings.ToLower(allowedHeader), strings.ToLower(requestedHeader)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func preflightAllowHeadersValue(allowedHeaders []string, requestedHeaders []string) string {
	if len(allowedHeaders) == 0 {
		return ""
	}
	if slices.Contains(allowedHeaders, "*") {
		if len(requestedHeaders) == 0 {
			return "*"
		}
		return strings.Join(requestedHeaders, ", ")
	}
	return strings.Join(allowedHeaders, ", ")
}

func parseHeaderList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(strings.ToLower(part))
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func normalizeValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func normalizeMethods(methods []string) []string {
	normalized := normalizeValues(methods)
	for idx, method := range normalized {
		normalized[idx] = strings.ToUpper(method)
	}
	return normalized
}

func wildcardMatch(pattern string, value string) bool {
	if pattern == "*" {
		return true
	}
	matches, err := path.Match(pattern, value)
	if err != nil {
		return false
	}
	return matches
}

func appendVary(headers http.Header, varyValue string) {
	existing := headers.Values(varyHeader)
	for _, existingValue := range existing {
		for _, split := range strings.Split(existingValue, ",") {
			if strings.EqualFold(strings.TrimSpace(split), varyValue) {
				return
			}
		}
	}
	headers.Add(varyHeader, varyValue)
}
