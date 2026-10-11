package balancer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Считывает счётчик или число наблюдений гистограммы с указанными метками.
func metricCount(t *testing.T, m *Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var result float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, sample := range f.Metric {
			got := map[string]string{}
			for _, label := range sample.Label {
				got[label.GetName()] = label.GetValue()
			}
			match := true
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if sample.Counter != nil {
				result += sample.GetCounter().GetValue()
			}
			if sample.Histogram != nil {
				result += float64(sample.GetHistogram().GetSampleCount())
			}
		}
	}
	return result
}

// Проверяет все комбинации доступности без сдвига round-robin.
func TestPoolReady(t *testing.T) {
	p, b := testPool(t, 2)
	p.nextHTTP.Store(13)
	p.nextGRPC.Store(17)
	for bits := 0; bits < 16; bits++ {
		b[0].SetHTTPHealthy(bits&1 != 0)
		b[0].SetGRPCHealthy(bits&2 != 0)
		b[1].SetHTTPHealthy(bits&4 != 0)
		b[1].SetGRPCHealthy(bits&8 != 0)
		want := bits&5 != 0 && bits&10 != 0
		if got := p.Ready(); got != want {
			t.Errorf("bits=%04b ready=%v want=%v", bits, got, want)
		}
	}
	if p.nextHTTP.Load() != 13 || p.nextGRPC.Load() != 17 {
		t.Fatal("readiness changed round-robin")
	}
	if new(Pool).Ready() {
		t.Fatal("empty pool ready")
	}
}

// Роутер отклоняет отсутствующие зависимости и несовпадающий реестр.
func TestNewRouterInvalidConfig(t *testing.T) {
	h, _ := testHTTPProxy(t, "http://unused:8082")
	other, _ := testPool(t, 1)
	for _, tc := range []struct {
		pool    *Pool
		proxy   *HTTPProxy
		metrics *Metrics
	}{
		{nil, h, h.metrics}, {&Pool{}, h, h.metrics}, {h.pool, nil, h.metrics}, {other, h, h.metrics},
		{h.pool, h, nil}, {h.pool, h, NewMetrics(other)}, {h.pool, h, NewMetrics(h.pool)},
	} {
		router, err := NewRouter(tc.pool, tc.proxy, tc.metrics)
		if router != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("router=%v err=%v", router, err)
		}
	}
}

// Служебные маршруты не вызывают каталог и не увеличивают метрики прокси.
func TestBalancerRouterProbes(t *testing.T) {
	h, b := testHTTPProxy(t, "http://unused:8082")
	calls := 0
	h.proxies[b[0]].Transport = proxyRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 418, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("catalog response")), Request: r}, nil
	})
	router, err := NewRouter(h.pool, h, h.metrics)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		http, grpc bool
		want       int
	}{{false, false, 503}, {true, false, 503}, {false, true, 503}, {true, true, 200}} {
		b[0].SetHTTPHealthy(tc.http)
		b[0].SetGRPCHealthy(tc.grpc)
		for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
			want := 200
			if path == "/readyz" {
				want = tc.want
			}
			for _, method := range []string{"GET", "HEAD"} {
				w := httptest.NewRecorder()
				router.ServeHTTP(w, httptest.NewRequest(method, path, nil))
				if w.Code != want {
					t.Fatalf("%s %s status=%d want=%d", method, path, w.Code, want)
				}
				if path != "/metrics" && (w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store") {
					t.Fatal("missing probe headers")
				}
				if path == "/metrics" && !strings.Contains(w.Body.String(), "food_balancer_backend_up") {
					t.Fatal("missing metrics")
				}
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
			if w.Code != 405 || w.Header().Get("Allow") != "GET, HEAD" {
				t.Fatalf("POST %s: %d", path, w.Code)
			}
		}
	}
	if calls != 0 || h.pool.nextHTTP.Load() != 0 || h.pool.nextGRPC.Load() != 0 {
		t.Fatal("probes selected catalog")
	}
	if metricCount(t, h.metrics, "food_balancer_requests_total", nil) != 0 || metricCount(t, h.metrics, "food_balancer_request_duration_seconds", nil) != 0 {
		t.Fatal("probes counted as proxy traffic")
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/v1/restaurants?private-id=x", nil))
	if w.Code != 418 || w.Body.String() != "catalog response" || calls != 1 {
		t.Fatal("business request not proxied")
	}
	if metricCount(t, h.metrics, "food_balancer_requests_total", map[string]string{"protocol": "http", "backend": b[0].Name(), "code": "418"}) != 1 {
		t.Fatal("proxied response not counted")
	}
}

// HEAD на служебном маршруте не передаёт тело по реальному HTTP-соединению.
func TestBalancerRouterHEAD(t *testing.T) {
	h, _ := testHTTPProxy(t, "http://unused:8082")
	router, err := NewRouter(h.pool, h, h.metrics)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		response, err := server.Client().Head(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || len(body) != 0 {
			t.Fatalf("HEAD %s body=%q err=%v", path, body, err)
		}
	}
}

// HTTP-метрики учитывают ранние ошибки, ответы каталога и сетевые сбои ровно один раз.
func TestHTTPProxyRequestMetrics(t *testing.T) {
	for _, kind := range []string{"ok", "upstream error", "unavailable", "network", "timeout", "cancelled", "expired"} {
		t.Run(kind, func(t *testing.T) {
			h, b := testHTTPProxy(t, "http://unused:8082")
			calls := 0
			h.proxies[b[0]].Transport = proxyRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if kind == "network" {
					return nil, errors.New("secret")
				}
				if kind == "timeout" {
					return nil, context.DeadlineExceeded
				}
				code := 200
				if kind == "upstream error" {
					code = 409
				}
				return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("response")), Request: r}, nil
			})
			ctx := context.Background()
			code := "200"
			backend := b[0].Name()
			selections := float64(1)
			switch kind {
			case "upstream error":
				code = "409"
			case "unavailable":
				b[0].SetHTTPHealthy(false)
				code = "503"
				backend = ""
				selections = 0
			case "network":
				code = "502"
			case "timeout":
				code = "504"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				code = "499"
				backend = ""
				selections = 0
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				code = "504"
				backend = ""
				selections = 0
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
			if metricCount(t, h.metrics, "food_balancer_requests_total", map[string]string{"protocol": "http", "backend": backend, "code": code}) != 1 {
				t.Fatal("wrong HTTP result metric")
			}
			if metricCount(t, h.metrics, "food_balancer_requests_total", nil) != 1 || metricCount(t, h.metrics, "food_balancer_request_duration_seconds", nil) != 1 {
				t.Fatal("request counted more than once")
			}
			if metricCount(t, h.metrics, "food_balancer_selections_total", nil) != selections || calls != int(selections) {
				t.Fatal("wrong selection count")
			}
		})
	}
}

// Имитирует обрыв тела после уже отправленных заголовков.
type brokenMetricBody struct{}

// Возвращает ошибку чтения тела ответа.
func (brokenMetricBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// Завершает тестовое чтение без дополнительных ресурсов.
func (brokenMetricBody) Close() error { return nil }

// Прерывание начатого ответа учитывается как aborted и сохраняет исходный panic.
func TestHTTPProxyAbortedMetric(t *testing.T) {
	h, b := testHTTPProxy(t, "http://unused:8082")
	h.proxies[b[0]].Transport = proxyRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: brokenMetricBody{}, Request: r}, nil
	})
	ctx := context.WithValue(context.Background(), http.ServerContextKey, &http.Server{})
	w := httptest.NewRecorder()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	}()
	if recovered != http.ErrAbortHandler || w.Code != 200 {
		t.Fatalf("panic=%v status=%d", recovered, w.Code)
	}
	if metricCount(t, h.metrics, "food_balancer_requests_total", map[string]string{"code": "aborted", "backend": b[0].Name()}) != 1 || metricCount(t, h.metrics, "food_balancer_requests_total", nil) != 1 {
		t.Fatal("aborted request counted incorrectly")
	}
}

// Именованная ошибка отражает итоговый gRPC-код, включая ранние возвраты.
func TestGRPCProxyRequestMetrics(t *testing.T) {
	for _, kind := range []string{"ok", "catalog error", "nil request", "unavailable", "cancelled", "expired", "panic"} {
		t.Run(kind, func(t *testing.T) {
			pool, b := testPool(t, 1)
			b[0].SetGRPCHealthy(true)
			m := NewMetrics(pool)
			calls := 0
			h := &GRPCProxy{pool: pool, metrics: m, clients: map[*Backend]catalogv1.CatalogServiceClient{b[0]: grpcClientFunc(func(_ context.Context, r *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
				calls++
				if kind == "catalog error" {
					return nil, status.Error(codes.NotFound, "missing item")
				}
				if kind == "panic" {
					panic("test panic")
				}
				return grpcTestResponse(r, "ok"), nil
			})}}
			ctx := context.Background()
			req := grpcTestRequest()
			want := codes.OK
			backend := b[0].Name()
			selections := float64(1)
			switch kind {
			case "catalog error":
				want = codes.NotFound
			case "nil request":
				req = nil
				want = codes.InvalidArgument
				backend = ""
				selections = 0
			case "unavailable":
				b[0].SetGRPCHealthy(false)
				want = codes.Unavailable
				backend = ""
				selections = 0
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = codes.Canceled
				backend = ""
				selections = 0
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				want = codes.DeadlineExceeded
				backend = ""
				selections = 0
			case "panic":
				want = codes.Internal
			}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, err := h.ResolveItems(ctx, req)
				if status.Code(err) != want {
					t.Errorf("err=%v want=%v", err, want)
				}
			}()
			if kind == "panic" && recovered != "test panic" || kind != "panic" && recovered != nil {
				t.Fatalf("panic=%v", recovered)
			}
			if metricCount(t, m, "food_balancer_requests_total", map[string]string{"protocol": "grpc", "backend": backend, "code": want.String()}) != 1 {
				t.Fatal("wrong gRPC metric")
			}
			if metricCount(t, m, "food_balancer_requests_total", nil) != 1 || metricCount(t, m, "food_balancer_request_duration_seconds", nil) != 1 || metricCount(t, m, "food_balancer_selections_total", nil) != selections || calls != int(selections) {
				t.Fatal("wrong observation count")
			}
		})
	}
}

// Запоминает промежуточные статусы для проверки обёртки HTTP-ответа.
type statusWriter struct {
	header http.Header
	codes  []int
}

// Возвращает тестовый набор заголовков.
func (w *statusWriter) Header() http.Header { return w.header }

// Запоминает отправленный HTTP-статус.
func (w *statusWriter) WriteHeader(code int) { w.codes = append(w.codes, code) }

// Принимает тело без сетевой записи.
func (w *statusWriter) Write(body []byte) (int, error) { return len(body), nil }

// Промежуточный 103 не заменяет итоговый статус, а повторный окончательный игнорируется.
func TestProxyResponseWriter(t *testing.T) {
	original := &statusWriter{header: make(http.Header)}
	w := &proxyResponseWriter{ResponseWriter: original}
	w.WriteHeader(103)
	w.WriteHeader(201)
	w.WriteHeader(500)
	if w.status != 201 || len(original.codes) != 2 || original.codes[0] != 103 || original.codes[1] != 201 || w.Unwrap() != original {
		t.Fatal("status or unwrap incorrect")
	}
	recorder := httptest.NewRecorder()
	bodyWriter := &proxyResponseWriter{ResponseWriter: recorder}
	if _, err := bodyWriter.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if bodyWriter.status != 200 {
		t.Fatal("implicit status missing")
	}
	if err := http.NewResponseController(bodyWriter).Flush(); err != nil || !recorder.Flushed {
		t.Fatalf("Flush unavailable: %v", err)
	}
}
