package authentication

import (
	"encoding/xml"
	"net/http"
)

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// WriteRequestError renders an authentication failure as XML for tests; the
// server renders it with its S3 error writer.
func WriteRequestError(w http.ResponseWriter, r *http.Request, requestErr *RequestError) {
	body, err := xml.Marshal(errorResponse{Code: requestErr.Code, Message: requestErr.Message})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(requestErr.StatusCode)
	w.Write(body)
}
