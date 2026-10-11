package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
)

// Описывает ошибку HTTP-прокси для клиента.
type httpErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// Отправляет JSON-ошибку с идентификатором запроса.
func (h *HTTPProxy) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	response := httpErrorResponse{Code: code, Message: message, RequestID: w.Header().Get("X-Request-ID")}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("write proxy error response", "error", err, "request_id", response.RequestID)
	}
}

// Преобразует сбой соединения или таймаут в ответ прокси.
func (h *HTTPProxy) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(r.Context().Err(), context.Canceled) {
		return
	}
	status, code, message := http.StatusBadGateway, "bad_gateway", "catalog request failed"
	var networkError net.Error
	if errors.Is(r.Context().Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
		status, code, message = http.StatusGatewayTimeout, "gateway_timeout", "catalog request timed out"
	}
	h.logger.Warn("catalog HTTP proxy failed", "request_id", w.Header().Get("X-Request-ID"), "status", status)
	h.writeError(w, status, code, message)
}
