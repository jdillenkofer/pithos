package httputils

import "net/url"

// GetQueryParam returns a pointer to the query parameter value if it exists, otherwise nil.
func GetQueryParam(values url.Values, key string) *string {
	if values.Has(key) {
		val := values.Get(key)
		return &val
	}
	return nil
}
