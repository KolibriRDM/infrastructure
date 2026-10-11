package balancer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"
)

// Распределяет HTTP-запросы между доступными экземплярами каталога.
type HTTPProxy struct {
	pool      *Pool
	transport *http.Transport
	proxies   map[*Backend]*httputil.ReverseProxy
	logger    *slog.Logger
	metrics   *Metrics
}

const httpRequestTimeout = 2 * time.Second

// Создаёт HTTP-прокси с общим пулом соединений.
func NewHTTPProxy(pool *Pool, logger *slog.Logger, metrics *Metrics) (*HTTPProxy, error) {
	if pool == nil || len(pool.backends) == 0 {
		return nil, fmt.Errorf("create HTTP proxy: backend pool is required: %w", ErrInvalidConfig)
	}
	if logger == nil {
		return nil, fmt.Errorf("create HTTP proxy: logger is required: %w", ErrInvalidConfig)
	}
	if metrics == nil || metrics.pool != pool {
		return nil, fmt.Errorf("create HTTP proxy: metrics must use the same pool: %w", ErrInvalidConfig)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	dialer := &net.Dialer{
		Timeout:   300 * time.Millisecond,
		KeepAlive: 30 * time.Second,
	}
	transport.DialContext = dialer.DialContext
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 20
	transport.MaxConnsPerHost = 50
	transport.IdleConnTimeout = 60 * time.Second
	transport.TLSHandshakeTimeout = time.Second
	transport.ResponseHeaderTimeout = time.Second
	transport.ExpectContinueTimeout = time.Second
	transport.DisableCompression = true
	h := &HTTPProxy{
		pool:      pool,
		transport: transport,
		proxies:   make(map[*Backend]*httputil.ReverseProxy, len(pool.backends)),
		logger:    logger,
		metrics:   metrics,
	}
	for _, backend := range pool.backends {
		target := backend.HTTPURL()
		h.proxies[backend] = &httputil.ReverseProxy{
			Transport:    transport,
			ErrorHandler: h.handleProxyError,
			ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(&target)
				pr.SetXForwarded()
				pr.Out.Header.Set("X-Request-ID", pr.In.Header.Get("X-Request-ID"))
				// Каталог не поддерживает ключи идемпотентности PATCH: не разрешаем повтор записи.
				if pr.Out.Method == http.MethodPatch {
					pr.Out.Header.Del("Idempotency-Key")
					pr.Out.Header.Del("X-Idempotency-Key")
					pr.Out.GetBody = nil
				}
			},
			ModifyResponse: func(resp *http.Response) error {
				// ID уже установлен балансировщиком: исключаем дублирование заголовка.
				resp.Header.Del("X-Request-ID")
				return nil
			},
		}
	}
	return h, nil
}

// Выбирает экземпляр и передаёт запрос с ограничением времени.
func (h *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var selected *Backend
	response := &proxyResponseWriter{ResponseWriter: w}
	w = response
	// Записываем результат и длительность при любом завершении запроса.
	defer func() {
		panicValue := recover()
		var code string
		if panicValue != nil {
			code = "aborted"
		} else {
			statusCode := response.status
			if statusCode == 0 {
				switch r.Context().Err() {
				case context.Canceled:
					statusCode = 499
				case context.DeadlineExceeded:
					statusCode = http.StatusGatewayTimeout
				default:
					statusCode = http.StatusOK
				}
			}
			code = strconv.Itoa(statusCode)
		}
		h.metrics.ObserveRequest("http", selected, code, time.Since(started))
		// Сохраняем прерывание ответа, которое выполняет ReverseProxy.
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	requestID := rand.Text()
	w.Header().Set("X-Request-ID", requestID)
	ctx, cancel := context.WithTimeout(r.Context(), httpRequestTimeout)
	defer cancel()
	request := r.Clone(ctx)
	request.Header.Set("X-Request-ID", requestID)
	if err := ctx.Err(); err != nil {
		h.handleProxyError(w, request, err)
		return
	}
	backend, err := h.pool.NextHTTP()
	if errors.Is(err, ErrNoHealthyBackends) {
		h.writeError(w, http.StatusServiceUnavailable, "catalog_unavailable", "no healthy catalog backends")
		return
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	selected = backend
	h.metrics.ObserveSelection("http", backend)
	h.proxies[backend].ServeHTTP(w, request)
}

// Закрывает свободные соединения с экземплярами каталога.
func (h *HTTPProxy) CloseIdleConnections() {
	h.transport.CloseIdleConnections()
}
