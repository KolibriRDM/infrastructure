package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Подменяет один RPC для проверки контекста без сетевых задержек.
type grpcClientFunc func(context.Context, *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error)

// Выполняет тестовый RPC вместо обращения к серверу.
func (f grpcClientFunc) ResolveItems(ctx context.Context, req *catalogv1.ResolveItemsRequest, _ ...grpc.CallOption) (*catalogv1.ResolveItemsResponse, error) {
	return f(ctx, req)
}

// Позволяет запускать настоящий сервер каталога с заданным поведением.
type catalogServerStub struct {
	catalogv1.UnimplementedCatalogServiceServer
	resolve grpcClientFunc
}

// Передаёт вызов тестовому обработчику каталога.
func (s *catalogServerStub) ResolveItems(ctx context.Context, req *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
	return s.resolve(ctx, req)
}

// Запускает локальный gRPC-сервер и останавливает его после теста.
func serveTestGRPC(t *testing.T, server *grpc.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	return listener.Addr().String()
}

// Запускает независимые экземпляры каталога и создаёт настоящий прокси.
func testGRPCProxy(t *testing.T, handlers ...grpcClientFunc) (*GRPCProxy, []*Backend) {
	t.Helper()
	backends := make([]*Backend, len(handlers))
	for i, handler := range handlers {
		server := grpc.NewServer()
		catalogv1.RegisterCatalogServiceServer(server, &catalogServerStub{resolve: handler})
		address := serveTestGRPC(t, server)
		backend, err := NewBackend(fmt.Sprintf("catalog-%d", i+1), "http://localhost:8082", address)
		if err != nil {
			t.Fatal(err)
		}
		backends[i] = backend
	}
	pool, err := NewPool(backends)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := NewGRPCProxy(pool, NewMetrics(pool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := proxy.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, backend := range backends {
		backend.SetGRPCHealthy(true)
	}
	return proxy, backends
}

// Подключает одного клиента к балансировщику для нескольких независимых RPC.
func proxyGRPCClient(t *testing.T, proxy *GRPCProxy) catalogv1.CatalogServiceClient {
	t.Helper()
	server, err := NewGRPCServer(proxy)
	if err != nil {
		t.Fatal(err)
	}
	address := serveTestGRPC(t, server)
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return catalogv1.NewCatalogServiceClient(conn)
}

// Создаёт запрос с несколькими блюдами для проверки передачи без преобразований.
func grpcTestRequest() *catalogv1.ResolveItemsRequest {
	return &catalogv1.ResolveItemsRequest{RestaurantId: "restaurant-1", ItemIds: []string{"item-b", "item-a"}}
}

// Создаёт ответ с признаком экземпляра и сохраняет порядок блюд.
func grpcTestResponse(req *catalogv1.ResolveItemsRequest, name string) *catalogv1.ResolveItemsResponse {
	response := &catalogv1.ResolveItemsResponse{RestaurantId: req.RestaurantId}
	for _, id := range req.ItemIds {
		response.Items = append(response.Items, &catalogv1.ResolvedItem{Id: id, Name: name, PriceKopecks: 12345})
	}
	return response
}

// Проверяет конфигурацию, адреса каналов и отсутствие преждевременной готовности.
func TestNewGRPCProxy(t *testing.T) {
	for _, pool := range []*Pool{nil, {}} {
		h, err := NewGRPCProxy(pool, NewMetrics(pool))
		if h != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("proxy=%v err=%v", h, err)
		}
	}
	if server, err := NewGRPCServer(nil); server != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("server=%v err=%v", server, err)
	}
	pool, backends := testPool(t, 2)
	h, err := NewGRPCProxy(pool, NewMetrics(pool))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for _, b := range backends {
		conn := h.connections[b]
		if conn == nil || h.clients[b] == nil || conn.Target() != "passthrough:///"+b.GRPCAddress() || conn.GetState() != connectivity.Idle || b.GRPCHealthy() {
			t.Fatalf("unexpected connection for %s", b.Name())
		}
	}
	if h.connections[backends[0]] == h.connections[backends[1]] {
		t.Fatal("backends share connection")
	}
}

// Ошибка закрытия не мешает обработать остальные подключения и собрать все ошибки.
func TestGRPCProxyCloseAll(t *testing.T) {
	pool, backends := testPool(t, 5)
	h, err := NewGRPCProxy(pool, NewMetrics(pool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, conn := range h.connections {
			conn.Close()
		}
	})
	for _, b := range backends[:3] {
		if err := h.connections[b].Close(); err != nil {
			t.Fatal(err)
		}
	}
	err = h.Close()
	if err == nil {
		t.Fatal("missing close errors")
	}
	for _, b := range backends[:3] {
		if !strings.Contains(err.Error(), b.Name()) {
			t.Errorf("missing error for %s: %v", b.Name(), err)
		}
	}
	for _, b := range backends {
		if h.connections[b].GetState() != connectivity.Shutdown {
			t.Errorf("connection %s not closed", b.Name())
		}
	}
	empty := &GRPCProxy{}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
}

// Распределяет отдельные RPC одного клиента, исключает и возвращает экземпляр.
func TestGRPCProxyRoundRobin(t *testing.T) {
	req := grpcTestRequest()
	makeHandler := func(name string) grpcClientFunc {
		return func(_ context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
			if !proto.Equal(r, req) {
				t.Error("request changed")
			}
			return grpcTestResponse(r, name), nil
		}
	}
	h, backends := testGRPCProxy(t, makeHandler("one"), makeHandler("two"))
	client := proxyGRPCClient(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check := func(want string) {
		t.Helper()
		response, err := client.ResolveItems(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(response, grpcTestResponse(req, want)) {
			t.Fatalf("response=%v", response)
		}
	}
	for _, want := range []string{"one", "two", "one", "two"} {
		check(want)
	}
	backends[0].SetGRPCHealthy(false)
	check("two")
	check("two")
	backends[0].SetGRPCHealthy(true)
	check("one")
	check("two")
	for _, b := range backends {
		b.SetGRPCHealthy(false)
		b.SetHTTPHealthy(true)
	}
	response, err := client.ResolveItems(ctx, req)
	if response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

// Параллельные RPC равномерно распределяются через одно клиентское соединение.
func TestGRPCProxyConcurrentCalls(t *testing.T) {
	var counts [2]atomic.Int64
	handlers := make([]grpcClientFunc, 2)
	for i := range handlers {
		handlers[i] = func(_ context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
			counts[i].Add(1)
			return grpcTestResponse(r, "ok"), nil
		}
	}
	h, _ := testGRPCProxy(t, handlers...)
	client := proxyGRPCClient(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Прогреваем оба подключения перед параллельной проверкой.
	for i := 0; i < 2; i++ {
		if _, err := client.ResolveItems(ctx, grpcTestRequest()); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.ResolveItems(ctx, grpcTestRequest()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if counts[0].Load() != 21 || counts[1].Load() != 21 {
		t.Fatalf("counts=%d/%d", counts[0].Load(), counts[1].Load())
	}
}

// Сохраняет код, сообщение и details ошибки без повторного вызова каталога.
func TestGRPCProxyPreservesStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.NotFound, codes.Unavailable, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			want, err := status.New(code, "catalog error").WithDetails(&errdetails.ErrorInfo{Reason: "MENU_ITEM_ERROR", Domain: "catalog"})
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int64
			h, _ := testGRPCProxy(t, func(context.Context, *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
				calls.Add(1)
				return nil, want.Err()
			})
			client := proxyGRPCClient(t, h)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			response, err := client.ResolveItems(ctx, grpcTestRequest())
			if response != nil || !proto.Equal(status.Convert(err).Proto(), want.Proto()) || calls.Load() != 1 {
				t.Fatalf("response=%v err=%v calls=%d", response, err, calls.Load())
			}
		})
	}
}

// Передаёт разрешённую metadata через два настоящих gRPC-соединения.
func TestGRPCProxyMetadata(t *testing.T) {
	h, _ := testGRPCProxy(t, func(ctx context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if got := md.Get("x-request-id"); len(got) != 1 || got[0] != "req-123" {
			t.Errorf("request ID=%v", got)
		}
		for _, key := range []string{"authorization", "private", "private-bin"} {
			if len(md.Get(key)) != 0 {
				t.Errorf("metadata leaked: %s", key)
			}
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > grpcRequestTimeout {
			t.Error("RPC deadline missing")
		}
		return grpcTestResponse(r, "ok"), nil
	})
	client := proxyGRPCClient(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-request-id", "req-123", "authorization", "secret", "private", "secret", "private-bin", "secret"))
	if _, err := client.ResolveItems(ctx, grpcTestRequest()); err != nil {
		t.Fatal(err)
	}
}

// Проверяет дедлайн прокси и сохранение более короткого клиентского ограничения.
func TestGRPCProxyDeadlines(t *testing.T) {
	for _, budget := range []time.Duration{0, 50 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pool, b := testPool(t, 1)
				b[0].SetGRPCHealthy(true)
				h := &GRPCProxy{pool: pool, metrics: NewMetrics(pool), clients: map[*Backend]catalogv1.CatalogServiceClient{b[0]: grpcClientFunc(func(ctx context.Context, _ *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				})}}
				ctx := context.Background()
				want := grpcRequestTimeout
				if budget != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, budget)
					defer cancel()
					want = budget
				}
				started := time.Now()
				response, err := h.ResolveItems(ctx, grpcTestRequest())
				if response != nil || status.Code(err) != codes.DeadlineExceeded || time.Since(started) != want {
					t.Fatalf("response=%v err=%v elapsed=%s", response, err, time.Since(started))
				}
			})
		})
	}
}

// Неверный запрос и завершённый контекст не вызывают клиент каталога.
func TestGRPCProxyEarlyErrors(t *testing.T) {
	pool, b := testPool(t, 1)
	b[0].SetGRPCHealthy(true)
	h := &GRPCProxy{pool: pool, metrics: NewMetrics(pool), clients: map[*Backend]catalogv1.CatalogServiceClient{b[0]: grpcClientFunc(func(context.Context, *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
		t.Fatal("unexpected RPC")
		return nil, nil
	})}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		ctx  context.Context
		req  *catalogv1.ResolveItemsRequest
		code codes.Code
	}{{ctx, grpcTestRequest(), codes.Canceled}, {expired, grpcTestRequest(), codes.DeadlineExceeded}, {context.Background(), nil, codes.InvalidArgument}} {
		response, err := h.ResolveItems(tc.ctx, tc.req)
		if response != nil || status.Code(err) != tc.code {
			t.Fatalf("response=%v err=%v want=%v", response, err, tc.code)
		}
	}
}

// Отмена клиента достигает каталога через балансировщик.
func TestGRPCProxyCancellation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	h, _ := testGRPCProxy(t, func(ctx context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	client := proxyGRPCClient(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.ResolveItems(ctx, grpcTestRequest()); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("RPC did not start")
	}
	cancel()
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not stop")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("catalog did not stop")
	}
}

// Лимиты клиента и сервера отклоняют сообщения больше 64 КиБ.
func TestGRPCProxyMessageLimits(t *testing.T) {
	var calls atomic.Int64
	h, _ := testGRPCProxy(t, func(_ context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
		calls.Add(1)
		if r.RestaurantId == "large-response" {
			return &catalogv1.ResolveItemsResponse{RestaurantId: strings.Repeat("x", maxGRPCMessageBytes+1)}, nil
		}
		return grpcTestResponse(r, "ok"), nil
	})
	client := proxyGRPCClient(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	huge := &catalogv1.ResolveItemsRequest{RestaurantId: strings.Repeat("x", maxGRPCMessageBytes+1)}
	for _, invoke := range []func(context.Context, *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error){h.ResolveItems, func(ctx context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
		return client.ResolveItems(ctx, r)
	}} {
		response, err := invoke(ctx, huge)
		if response != nil || status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("large request: response=%v err=%v", response, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("oversized request reached catalog")
	}
	response, err := client.ResolveItems(ctx, &catalogv1.ResolveItemsRequest{RestaurantId: "large-response"})
	if response != nil || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("large response: response=%v err=%v", response, err)
	}
}
