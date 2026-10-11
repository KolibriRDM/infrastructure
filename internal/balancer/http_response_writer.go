package balancer

import "net/http"

// Запоминает HTTP-статус, сохраняя доступ к возможностям исходного writer.
type proxyResponseWriter struct {
	http.ResponseWriter
	status int
}

// Передаёт промежуточные ответы и запоминает первый окончательный статус.
func (w *proxyResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Устанавливает статус 200 перед первой записью тела.
func (w *proxyResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

// Позволяет ReverseProxy использовать Flush и Hijack через ResponseController.
func (w *proxyResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
