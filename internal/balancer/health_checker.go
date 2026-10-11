package balancer

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"
)

// Задаёт период проверки каждого экземпляра каталога.
const healthCheckInterval = time.Second

// Ограничивает время одной проверки доступности.
const healthCheckTimeout = 300 * time.Millisecond

// Ограничивает чтение тела ответа служебного маршрута.
const maxHealthResponseBytes = 4 << 10

// Проверяет готовность каталога и доступность его gRPC-подключения.
type HealthChecker struct {
	pool       *Pool
	grpcProxy  *GRPCProxy
	httpClient *http.Client
}

// Проверяет зависимости и создаёт отдельный HTTP-клиент служебных проверок.
func NewHealthChecker(pool *Pool, grpcProxy *GRPCProxy) (*HealthChecker, error) {
	if pool == nil || len(pool.backends) == 0 {
		return nil, fmt.Errorf("create health checker: backend pool is required: %w", ErrInvalidConfig)
	}
	if grpcProxy == nil || grpcProxy.pool != pool {
		return nil, fmt.Errorf("create health checker: gRPC proxy must use the same pool: %w", ErrInvalidConfig)
	}
	for _, backend := range pool.backends {
		if grpcProxy.connections[backend] == nil {
			return nil, fmt.Errorf("create health checker: gRPC connection for %q is required: %w", backend.Name(), ErrInvalidConfig)
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	dialer := &net.Dialer{Timeout: healthCheckTimeout, KeepAlive: 30 * time.Second}
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = healthCheckTimeout
	transport.ResponseHeaderTimeout = healthCheckTimeout
	transport.MaxIdleConns = len(pool.backends)
	transport.MaxIdleConnsPerHost = 1
	transport.MaxConnsPerHost = 1
	transport.IdleConnTimeout = 30 * time.Second
	transport.DisableCompression = true
	return &HealthChecker{
		pool:      pool,
		grpcProxy: grpcProxy,
		httpClient: &http.Client{
			Transport:     transport,
			Timeout:       healthCheckTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// Закрывает свободные HTTP-соединения после остановки проверок.
func (h *HealthChecker) CloseIdleConnections() {
	h.httpClient.CloseIdleConnections()
}

// Проверяет готовность каталога через HTTP с ограничением времени и тела.
func (h *HealthChecker) checkHTTP(ctx context.Context, backend *Backend) bool {
	probeCtx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	target := backend.HTTPURL()
	target.Path = "/readyz"
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false
	}
	response, err := h.httpClient.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	// Дополнительный байт позволяет обнаружить превышение лимита.
	reader := io.LimitReader(response.Body, maxHealthResponseBytes+1)
	size, err := io.Copy(io.Discard, reader)
	return err == nil && size <= maxHealthResponseBytes && probeCtx.Err() == nil && response.StatusCode == http.StatusOK
}

// Ожидает готовности существующего gRPC-подключения в пределах таймаута.
func (h *HealthChecker) checkGRPC(ctx context.Context, backend *Backend) bool {
	probeCtx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	conn := h.grpcProxy.connections[backend]
	for {
		if probeCtx.Err() != nil {
			return false
		}
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return true
		case connectivity.Shutdown:
			return false
		case connectivity.Idle:
			conn.Connect()
		}
		// Ждём изменения состояния без активного опроса в цикле.
		if !conn.WaitForStateChange(probeCtx, state) {
			return false
		}
	}
}

// Обновляет доступность HTTP и gRPC по результатам проверок экземпляра.
func (h *HealthChecker) checkBackend(ctx context.Context, backend *Backend) {
	if ctx.Err() != nil {
		return
	}
	httpReady := h.checkHTTP(ctx, backend)
	if ctx.Err() != nil {
		return
	}
	backend.SetHTTPHealthy(httpReady)
	// Неготовый каталог исключается и из gRPC, даже если его порт доступен.
	if !httpReady {
		backend.SetGRPCHealthy(false)
		return
	}
	grpcReady := h.checkGRPC(ctx, backend)
	if ctx.Err() != nil {
		return
	}
	backend.SetGRPCHealthy(grpcReady)
}

// Проверяет один экземпляр сразу и затем по таймеру до отмены.
func (h *HealthChecker) runBackend(ctx context.Context, backend *Backend) {
	defer backend.SetHTTPHealthy(false)
	defer backend.SetGRPCHealthy(false)
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	h.checkBackend(ctx, backend)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkBackend(ctx, backend)
		}
	}
}

// Запускает по одной горутине на экземпляр и ждёт их завершения; вызывается один раз.
func (h *HealthChecker) Run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	defer h.CloseIdleConnections()
	var workers sync.WaitGroup
	for _, backend := range h.pool.backends {
		workers.Add(1)
		go func(backend *Backend) {
			defer workers.Done()
			h.runBackend(ctx, backend)
		}(backend)
	}
	// Run возвращается только после остановки всех проверяющих горутин.
	workers.Wait()
}
