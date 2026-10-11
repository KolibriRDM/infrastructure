package balancer

import (
	"encoding/json"
	"net/http"
)

// Описывает состояние процесса или готовность балансировщика.
type probeResponse struct {
	Status string `json:"status"`
}

// Отправляет короткий служебный ответ без кэширования.
func writeProbeResponse(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	// После начала ответа ошибка записи не позволяет отправить другой статус.
	_ = json.NewEncoder(w).Encode(probeResponse{Status: value})
}

// Отклоняет неподдерживаемые методы служебного маршрута.
func probeMethodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Allow", "GET, HEAD")
	writeProbeResponse(w, http.StatusMethodNotAllowed, "method_not_allowed")
}
