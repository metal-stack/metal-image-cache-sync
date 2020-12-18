package utils

import (
	"net/http"
)

type HTTPStatusResponseWriter struct {
	http.ResponseWriter
	status int
}

func NewHTTPStatusResponseWriter(wrap http.ResponseWriter) *HTTPStatusResponseWriter {
	return &HTTPStatusResponseWriter{
		ResponseWriter: wrap,
	}
}

func (h *HTTPStatusResponseWriter) WriteHeader(code int) {
	h.status = code
	h.ResponseWriter.WriteHeader(code)
}

func (h *HTTPStatusResponseWriter) GetStatus() int {
	return h.status
}
