package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/balancer"
	"github.com/KolibriRDM/food-delivery-infrastructure/internal/platform/config"
	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// Передаёт события приложения тесту без чтения stdout и гонок буфера.
type logEvents struct{ events chan map[string]any }

// Разбирает одну запись JSON-логгера.
func (l *logEvents) Write(p []byte) (int, error) {
	var event map[string]any
	if err := json.Unmarshal(p, &event); err != nil {
		return 0, err
	}
	l.events <- event
	return len(p), nil
}

// Дожидается события в ограниченное время.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
	var zero T
	return zero
}

// Создаёт конфигурацию с выделяемыми ОС портами для проверки приложения.
func testConfig() config.BalancerConfig {
	return config.BalancerConfig{HTTPAddr: "127.0.0.1:0", GRPCAddr: "127.0.0.1:0",
		Backends: []config.BackendConfig{{Name: "catalog-1", HTTPURL: "http://127.0.0.1:1", GRPCAddr: "127.0.0.1:1"}},
	}
}

// Проверяет ошибки до запуска, отмену и освобождение первого занятого приложением порта.
func TestServeStartup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := serve(ctx, config.BalancerConfig{}, logger); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("invalid backend", func(t *testing.T) {
		cfg := testConfig()
		cfg.Backends[0].HTTPURL = "invalid"
		if err := serve(context.Background(), cfg, logger); !errors.Is(err, balancer.ErrInvalidConfig) {
			t.Fatalf("error = %v", err)
		}
	})
	for _, protocol := range []string{"HTTP", "gRPC"} {
		t.Run(protocol+" port busy", func(t *testing.T) {
			busy, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer busy.Close()
			cfg := testConfig()
			if protocol == "HTTP" {
				cfg.HTTPAddr = busy.Addr().String()
			} else {
				// Занятый вторым сервером адрес должен освободить уже открытый HTTP-порт.
				available, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				cfg.HTTPAddr = available.Addr().String()
				_ = available.Close()
				cfg.GRPCAddr = busy.Addr().String()
			}
			if err := serve(context.Background(), cfg, logger); err == nil || !strings.Contains(err.Error(), "listen "+protocol) {
				t.Fatalf("error = %v", err)
			}
			if protocol == "gRPC" {
				listener, err := net.Listen("tcp", cfg.HTTPAddr)
				if err != nil {
					t.Fatal("HTTP listener leaked:", err)
				}
				_ = listener.Close()
			}
		})
	}
}

// Запускает приложение и возвращает фактически выделенные порты из его журнала.
func startApp(t *testing.T, cfg config.BalancerConfig) (string, string, context.CancelFunc, <-chan error, *logEvents) {
	t.Helper()
	events := &logEvents{events: make(chan map[string]any, 32)}
	logger := slog.New(slog.NewJSONHandler(events, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, logger) }()
	t.Cleanup(cancel)
	addresses := make(map[string]string)
	for len(addresses) < 2 {
		event := await(t, events.events)
		if name, ok := event["msg"].(string); ok && strings.HasSuffix(name, "server started") {
			addresses[name] = event["addr"].(string)
		}
	}
	return addresses["HTTP server started"], addresses["gRPC server started"], cancel, done, events
}

// Читает ответ полностью и закрывает тело.
func get(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

// Проверяет освобождение слушающих портов после завершения приложения.
func assertPortsReleased(t *testing.T, addresses ...string) {
	t.Helper()
	for _, addr := range addresses {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("listener %s leaked: %v", addr, err)
			continue
		}
		_ = listener.Close()
	}
}

// Подключает тестового клиента к настоящему gRPC-порту приложения.
func appClient(t *testing.T, addr string) catalogv1.CatalogServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return catalogv1.NewCatalogServiceClient(conn)
}

// Проверяет доступность диагностики при отказе всех каталогов и штатную остановку.
func TestServeUnavailable(t *testing.T) {
	httpAddr, grpcAddr, cancel, done, _ := startApp(t, testConfig())
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 503, "/metrics": 200, "/v1/menu": 503} {
		code, body := get(t, client, "http://"+httpAddr+path)
		if code != want {
			t.Errorf("%s status=%d, want %d", path, code, want)
		}
		if path == "/metrics" && !strings.Contains(body, "food_balancer_backend_up") {
			t.Error("missing backend metrics")
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	_, err := appClient(t, grpcAddr).ResolveItems(ctx, &catalogv1.ResolveItemsRequest{RestaurantId: "r"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("RPC error=%v", err)
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	assertPortsReleased(t, httpAddr, grpcAddr)
}

// Удерживает RPC до разрешения теста и наблюдает закрытие исходящих соединений.
type lifecycleCatalog struct {
	catalogv1.UnimplementedCatalogServiceServer
	started     chan struct{}
	release     chan struct{}
	connections atomic.Int64
}

// Возвращает результат только после разрешения, учитывая отмену запроса.
func (s *lifecycleCatalog) ResolveItems(ctx context.Context, req *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
	close(s.started)
	select {
	case <-s.release:
		return &catalogv1.ResolveItemsResponse{RestaurantId: req.RestaurantId}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// Сохраняет контекст статистики RPC.
func (s *lifecycleCatalog) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

// Прикладная статистика не нужна проверке закрытия каналов.
func (s *lifecycleCatalog) HandleRPC(context.Context, stats.RPCStats) {}

// Сохраняет контекст статистики соединения.
func (s *lifecycleCatalog) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// Считает активные подключения балансировщика к каталогу.
func (s *lifecycleCatalog) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.connections.Add(1)
	case *stats.ConnEnd:
		s.connections.Add(-1)
	}
}

// Ждёт внешнее сетевое состояние с ограниченным сроком.
func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("condition did not become true")
		}
	}
}

// Проверяет сохранение активных HTTP/RPC при отмене и закрытие исходящих подключений.
func TestServeGracefulShutdown(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	httpStarted := make(chan struct{})
	backendHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		close(httpStarted)
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backendHTTP.Close)
	stub := &lifecycleCatalog{started: make(chan struct{}), release: release}
	backendGRPC := grpc.NewServer(grpc.StatsHandler(stub))
	catalogv1.RegisterCatalogServiceServer(backendGRPC, stub)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backendDone := make(chan error, 1)
	go func() { backendDone <- backendGRPC.Serve(listener) }()
	t.Cleanup(func() {
		backendGRPC.Stop()
		_ = listener.Close()
		if err := await(t, backendDone); err != nil {
			t.Error(err)
		}
	})
	cfg := testConfig()
	cfg.Backends[0].HTTPURL = backendHTTP.URL
	cfg.Backends[0].GRPCAddr = listener.Addr().String()
	httpAddr, grpcAddr, cancel, done, events := startApp(t, cfg)
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	eventually(t, func() bool { code, _ := get(t, client, "http://"+httpAddr+"/readyz"); return code == 200 })
	if stub.connections.Load() != 1 {
		t.Fatal("expected one outgoing gRPC connection")
	}
	grpcClient := appClient(t, grpcAddr)
	httpResult, grpcResult := make(chan error, 1), make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + httpAddr + "/v1/menu")
		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil && (resp.StatusCode != 200 || string(body) != "finished") {
				err = fmt.Errorf("HTTP result: %d %q", resp.StatusCode, body)
			}
		}
		httpResult <- err
	}()
	go func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		resp, err := grpcClient.ResolveItems(ctx, &catalogv1.ResolveItemsRequest{RestaurantId: "restaurant-1"})
		if err == nil && resp.GetRestaurantId() != "restaurant-1" {
			err = fmt.Errorf("unexpected RPC result: %v", resp)
		}
		grpcResult <- err
	}()
	await(t, httpStarted)
	await(t, stub.started)
	cancel()
	if event := await(t, events.events); event["msg"] != "servers stopping" {
		t.Fatalf("unexpected event: %v", event)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before requests completed: %v", err)
	default:
	}
	unblock()
	if err := await(t, httpResult); err != nil {
		t.Fatal("HTTP:", err)
	}
	if err := await(t, grpcResult); err != nil {
		t.Fatal("RPC:", err)
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	assertPortsReleased(t, httpAddr, grpcAddr)
	eventually(t, func() bool { return stub.connections.Load() == 0 })
}
