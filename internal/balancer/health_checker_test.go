package balancer

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Создаёт проверяющий компонент и освобождает его сетевые ресурсы после теста.
func testHealthChecker(t *testing.T, backends ...*Backend) *HealthChecker {
	t.Helper()
	pool, err := NewPool(backends)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := NewGRPCProxy(pool, NewMetrics(pool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, conn := range proxy.connections {
			conn.Close()
		}
	})
	h, err := NewHealthChecker(pool, proxy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.CloseIdleConnections)
	return h
}

// Создаёт экземпляр каталога с адресами тестовых серверов.
func healthBackend(t *testing.T, httpAddress, grpcAddress string) *Backend {
	t.Helper()
	b, err := NewBackend("catalog-1", httpAddress, grpcAddress)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Отклоняет отсутствующие зависимости, чужой пул и неполный набор соединений.
func TestNewHealthCheckerInvalidConfig(t *testing.T) {
	pool, _ := testPool(t, 1)
	other, _ := testPool(t, 1)
	for _, tc := range []struct {
		pool  *Pool
		proxy *GRPCProxy
	}{
		{nil, nil}, {&Pool{}, nil}, {pool, nil}, {pool, &GRPCProxy{pool: other}}, {pool, &GRPCProxy{pool: pool}},
	} {
		h, err := NewHealthChecker(tc.pool, tc.proxy)
		if h != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("checker=%v err=%v", h, err)
		}
	}
}

// Принимает только успешный ограниченный ответ без редиректов и ошибок чтения.
func TestHealthCheckHTTP(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status, size int
		truncated    bool
		want         bool
	}{
		{"ready", 200, 12, false, true}, {"empty", 200, 0, false, true}, {"max", 200, maxHealthResponseBytes, false, true},
		{"too large", 200, maxHealthResponseBytes + 1, false, false}, {"not ready", 503, 0, false, false},
		{"no content", 204, 0, false, false}, {"redirect", 302, 0, false, false}, {"truncated", 200, 2, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.RequestURI() != "/readyz" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				if tc.truncated {
					w.Header().Set("Content-Length", "100")
				}
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(tc.status)
				io.WriteString(w, strings.Repeat("x", tc.size))
			}))
			defer upstream.Close()
			b := healthBackend(t, upstream.URL, "localhost:9082")
			h := testHealthChecker(t, b)
			if got := h.checkHTTP(context.Background(), b); got != tc.want {
				t.Fatalf("ready=%v want=%v", got, tc.want)
			}
			if calls.Load() != 1 {
				t.Fatalf("request count=%d", calls.Load())
			}
			if b.HTTPURL().Path != "" {
				t.Fatal("backend URL modified")
			}
		})
	}
}

// Таймаут ограничивает ожидание заголовков и чтение незавершённого тела.
func TestHealthCheckHTTPTimeout(t *testing.T) {
	for _, bodyStarted := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[bodyStarted], func(t *testing.T) {
			stopped := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(stopped)
				if bodyStarted {
					w.Header().Set("Content-Length", "100")
					w.WriteHeader(200)
					io.WriteString(w, "x")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer upstream.Close()
			b := healthBackend(t, upstream.URL, "localhost:9082")
			h := testHealthChecker(t, b)
			started := time.Now()
			if h.checkHTTP(context.Background(), b) {
				t.Fatal("hung server marked ready")
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("probe exceeded timeout: %s", elapsed)
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("server request not cancelled")
			}
		})
	}
}

// Уже отменённый HTTP-контекст не обращается к серверу.
func TestHealthCheckHTTPCancelled(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer upstream.Close()
	b := healthBackend(t, upstream.URL, "localhost:9082")
	h := testHealthChecker(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h.checkHTTP(ctx, b) || calls.Load() != 0 {
		t.Fatal("cancelled probe reached server")
	}
}

// Поднимает gRPC-транспорт, допускающий перезапуск на прежнем порту.
func startHealthGRPC(t *testing.T, address string) (*grpc.Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		t.Error("health check invoked business RPC")
		return nil, errors.New("unexpected RPC")
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	return server, listener.Addr().String()
}

// Готовность БД и gRPC проверяется отдельно, отключённый сервер возвращается после рестарта.
func TestHealthCheckBackendRecovery(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(503)
		}
	}))
	defer httpServer.Close()
	server, address := startHealthGRPC(t, "127.0.0.1:0")
	b := healthBackend(t, httpServer.URL, address)
	h := testHealthChecker(t, b)
	h.checkBackend(context.Background(), b)
	if !b.HTTPHealthy() || !b.GRPCHealthy() {
		t.Fatal("healthy catalog not enabled")
	}
	ready.Store(false)
	h.checkBackend(context.Background(), b)
	if b.HTTPHealthy() || b.GRPCHealthy() {
		t.Fatal("unready catalog still enabled")
	}
	if h.grpcProxy.connections[b].GetState() != connectivity.Ready {
		t.Fatal("test requires ready gRPC transport")
	}
	ready.Store(true)
	h.checkBackend(context.Background(), b)
	if !b.HTTPHealthy() || !b.GRPCHealthy() {
		t.Fatal("readiness recovery failed")
	}
	server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn := h.grpcProxy.connections[b]
	for conn.GetState() == connectivity.Ready {
		if !conn.WaitForStateChange(ctx, connectivity.Ready) {
			t.Fatal("connection did not notice stopped server")
		}
	}
	h.checkBackend(ctx, b)
	if !b.HTTPHealthy() || b.GRPCHealthy() {
		t.Fatal("HTTP readiness masked gRPC failure")
	}
	_, _ = startHealthGRPC(t, address)
	for !b.GRPCHealthy() {
		if ctx.Err() != nil {
			t.Fatal("gRPC did not recover")
		}
		h.checkBackend(ctx, b)
	}
	if !b.HTTPHealthy() {
		t.Fatal("HTTP readiness lost")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if h.checkGRPC(context.Background(), b) {
		t.Fatal("shutdown connection marked ready")
	}
}

// Ожидание подключения ограничено таймаутом пробы и отменой родительского контекста.
func TestHealthCheckGRPCDeadline(t *testing.T) {
	b := healthBackend(t, "http://localhost:8082", "localhost:9082")
	h := testHealthChecker(t, b)
	h.grpcProxy.connections[b].Close()
	conn, err := grpc.NewClient("passthrough:///unused:9082", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }))
	if err != nil {
		t.Fatal(err)
	}
	h.grpcProxy.connections[b] = conn
	started := time.Now()
	if h.checkGRPC(context.Background(), b) {
		t.Fatal("unconnected channel marked ready")
	}
	if elapsed := time.Since(started); elapsed < healthCheckTimeout || elapsed > 2*time.Second {
		t.Fatalf("elapsed=%s", elapsed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h.checkGRPC(ctx, b) {
		t.Fatal("cancelled check succeeded")
	}
}

// Подменяет HTTP-проверку и отслеживает закрытие свободных соединений.
type healthTestTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
	closed    atomic.Int64
}

// Выполняет управляемую проверку без реальной сети.
func (t *healthTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.roundTrip(r)
}

// Фиксирует завершение работы HTTP-клиента.
func (t *healthTestTransport) CloseIdleConnections() { t.closed.Add(1) }

// Проверяет немедленный запуск, периодичность, ограничение конкурентности и полную остановку.
func TestHealthCheckerRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, backends := testPool(t, 2)
		h := testHealthChecker(t, backends...)
		var calls [2]atomic.Int64
		var active [2]atomic.Int64
		transport := &healthTestTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			index := 0
			if r.URL.Hostname() == "catalog-2" {
				index = 1
			}
			calls[index].Add(1)
			if active[index].Add(1) != 1 {
				t.Error("overlapping probes for one backend")
			}
			defer active[index].Add(-1)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}}
		h.httpClient.Transport = transport
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		for _, b := range backends {
			b.SetHTTPHealthy(true)
			b.SetGRPCHealthy(true)
		}
		go func() { defer close(done); h.Run(ctx) }()
		synctest.Wait()
		for i := range calls {
			if calls[i].Load() != 1 || active[i].Load() != 1 {
				t.Fatalf("backend=%d calls=%d active=%d", i, calls[i].Load(), active[i].Load())
			}
		}
		time.Sleep(healthCheckTimeout)
		synctest.Wait()
		for i, b := range backends {
			if b.HTTPHealthy() || b.GRPCHealthy() || active[i].Load() != 0 {
				t.Fatal("failed probe did not disable backend")
			}
		}
		time.Sleep(healthCheckInterval - healthCheckTimeout)
		synctest.Wait()
		for i := range calls {
			if calls[i].Load() != 2 || active[i].Load() != 1 {
				t.Fatal("periodic probes not started")
			}
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run returned before workers stopped or is still blocked")
		}
		for i, b := range backends {
			if active[i].Load() != 0 || b.HTTPHealthy() || b.GRPCHealthy() {
				t.Fatal("worker remains active after Run")
			}
		}
		if transport.closed.Load() != 1 {
			t.Fatal("idle connections not closed")
		}
		time.Sleep(2 * healthCheckInterval)
		synctest.Wait()
		for i := range calls {
			if calls[i].Load() != 2 {
				t.Fatal("probe started after shutdown")
			}
		}
	})
}

// Run с отменённым контекстом не запускает проверяющие горутины.
func TestHealthCheckerRunCancelled(t *testing.T) {
	_, backends := testPool(t, 1)
	h := testHealthChecker(t, backends...)
	h.httpClient.Transport = proxyRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("unexpected probe")
		return nil, errors.New("unexpected")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.Run(ctx)
	if backends[0].HTTPHealthy() || backends[0].GRPCHealthy() {
		t.Fatal("cancelled checker changed health")
	}
}
