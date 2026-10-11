package balancer

import (
	"fmt"
	"net/http"
)

// Объединяет служебные маршруты и HTTP-прокси каталога.
func NewRouter(pool *Pool, proxy *HTTPProxy, metrics *Metrics) (http.Handler, error) {
	if pool == nil || len(pool.backends) == 0 {
		return nil, fmt.Errorf("create balancer router: backend pool is required: %w", ErrInvalidConfig)
	}
	if proxy == nil || proxy.pool != pool {
		return nil, fmt.Errorf("create balancer router: HTTP proxy must use the same pool: %w", ErrInvalidConfig)
	}
	if metrics == nil || metrics.pool != pool || proxy.metrics != metrics {
		return nil, fmt.Errorf("create balancer router: metrics must be shared with HTTP proxy: %w", ErrInvalidConfig)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeProbeResponse(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !pool.Ready() {
			writeProbeResponse(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		writeProbeResponse(w, http.StatusOK, "ready")
	})
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("/healthz", probeMethodNotAllowed)
	mux.HandleFunc("/readyz", probeMethodNotAllowed)
	mux.HandleFunc("/metrics", probeMethodNotAllowed)
	mux.Handle("/", proxy)
	return mux, nil
}
