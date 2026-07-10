package server

import (
	"encoding/xml"
	"log/slog"
	"net/http"

	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/oklog/ulid/v2"
)

func xmlMarshalWithDocType(v any) ([]byte, error) {
	xmlResponse, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), xmlResponse...), nil
}

func writeXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, response any) {
	body, err := xmlMarshalWithDocType(response)
	if err != nil {
		slog.ErrorContext(r.Context(), "Failed to marshal XML response", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(statusCode)
	if _, err := w.Write(body); err != nil {
		slog.DebugContext(r.Context(), "Failed to write XML response", "error", err)
	}
}

func writeS3ErrorResponse(w http.ResponseWriter, r *http.Request, statusCode int, code string, message string, resource string) {
	requestID, ok := httpmiddleware.RequestIDFromContext(r.Context())
	if !ok {
		requestID = ulid.Make().String()
	}
	writeXMLResponse(w, r, statusCode, ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestId: requestID,
	})
}
