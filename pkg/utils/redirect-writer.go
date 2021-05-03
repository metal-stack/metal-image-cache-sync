package utils

import (
	"net/http"
	"strings"
)

const (
	notFoundResp = "404 page not found"
)

// HTTPRedirectResponseWriter redirects to the HTTPS address of the requested resource on 404.
type HTTPRedirectResponseWriter struct {
	http.ResponseWriter
	status int
	req    *http.Request
}

func NewHTTPRedirectResponseWriter(wrap http.ResponseWriter, req *http.Request) *HTTPRedirectResponseWriter {
	return &HTTPRedirectResponseWriter{
		ResponseWriter: wrap,
		req:            req,
	}
}

func (h *HTTPRedirectResponseWriter) WriteHeader(code int) {
	h.status = code
	if code != http.StatusNotFound {
		h.ResponseWriter.WriteHeader(code)
	}

	h.status = http.StatusTemporaryRedirect
	h.req.URL.Scheme = "https"
	h.req.URL.Host = h.req.Host
	h.ResponseWriter.Header().Add("Location", h.req.URL.String())
	h.ResponseWriter.WriteHeader(http.StatusTemporaryRedirect)
}

func (h *HTTPRedirectResponseWriter) Write(data []byte) (int, error) {
	resp := string(data)
	if strings.Contains(resp, notFoundResp) {
		mod := strings.Replace(resp, notFoundResp, "307 redirect due to cache miss", -1)
		return h.ResponseWriter.Write([]byte(mod))
	}
	return h.ResponseWriter.Write(data)
}

func (h *HTTPRedirectResponseWriter) GetStatus() int {
	return h.status
}
