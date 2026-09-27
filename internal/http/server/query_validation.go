package server

import (
	"net/http"
	"net/url"
	"strconv"
)

// Only an omitted limit selects the default. Invalid supplied values must not
// authorize as a small number and then execute with the larger default limit.
func parseMaxKeys(query url.Values) (int32, error) {
	if !query.Has(maxKeysQuery) {
		return int32(maxListLimit), nil
	}
	value, err := strconv.ParseInt(query.Get(maxKeysQuery), 10, 32)
	if err != nil || value < 0 || value > maxListLimit {
		return 0, ErrInvalidArgument
	}
	return int32(value), nil
}

// Validate before authentication and routing so signatures, authorizers, and
// handlers all see the same unambiguous query. Preserve RawQuery for SigV4.
func makeQueryValidationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeS3ErrorResponse(w, r, http.StatusBadRequest, "InvalidArgument", "Malformed query string", r.URL.Path)
			return
		}
		for _, values := range query {
			if len(values) != 1 {
				writeS3ErrorResponse(w, r, http.StatusBadRequest, "InvalidArgument", "Query parameters must not be repeated", r.URL.Path)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
