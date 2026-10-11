package balancer

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
	"testing/synctest"
	"time"
)

// Подменяет транспорт для детерминированных ошибок и дедлайнов.
type proxyRoundTripFunc func(*http.Request) (*http.Response, error)

// Выполняет подменённый сетевой запрос.
func (f proxyRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Собирает прокси с готовыми экземплярами для тестовых HTTP-серверов.
func testHTTPProxy(t *testing.T, addresses ...string) (*HTTPProxy, []*Backend) {
	t.Helper()
	backends := make([]*Backend, len(addresses))
	for i, address := range addresses {
		b, err := NewBackend(fmt.Sprintf("catalog-%d", i+1), address, "localhost:9082")
		if err != nil {
			t.Fatal(err)
		}
		b.SetHTTPHealthy(true)
		backends[i] = b
	}
	pool, err := NewPool(backends)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHTTPProxy(pool, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(pool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.CloseIdleConnections)
	return h, backends
}

// Проверяет статус, безопасное содержимое и корреляцию JSON-ошибки.
func assertProxyError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var response httpErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || response.Code != code || response.Message == "" || response.RequestID == "" || response.RequestID != w.Header().Get("X-Request-ID") {
		t.Fatalf("status=%d response=%+v", w.Code, response)
	}
	if w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing error headers")
	}
	if strings.Contains(w.Body.String(), "secret-canary") {
		t.Fatal("internal error leaked")
	}
}

// Конструктор отклоняет отсутствующие зависимости.
func TestNewHTTPProxyInvalidConfig(t *testing.T) {
	pool, _ := testPool(t, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		pool   *Pool
		logger *slog.Logger
	}{{nil, logger}, {&Pool{}, logger}, {pool, nil}} {
		h, err := NewHTTPProxy(tc.pool, tc.logger, NewMetrics(tc.pool))
		if h != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("proxy=%v error=%v", h, err)
		}
	}
}

// Передаёт тело, путь, query и прикладные заголовки, сохраняя исходный запрос.
func TestHTTPProxyForwardRequest(t *testing.T) {
	type observed struct {
		method, uri, host, body string
		header                  http.Header
	}
	requests := make(chan observed, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests <- observed{r.Method, r.RequestURI, r.Host, string(body), r.Header.Clone()}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, `{"code":"conflict","request_id":%q}`, r.Header.Get("X-Request-ID"))
	}))
	defer upstream.Close()
	h, backends := testHTTPProxy(t, upstream.URL)
	body := `{"price_kopecks":12000,"expected_version":2}`
	request := httptest.NewRequest("PATCH", "http://balancer.local/v1/menu-items/a%2Fb?tag=one&tag=two&q=a%20b", strings.NewReader(body))
	request.RemoteAddr = "192.0.2.10:1234"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Demo-Role", "staff")
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("X-Request-ID", "client-id")
	request.Header.Set("X-Forwarded-For", "spoofed")
	request.Header.Set("X-Forwarded-Host", "spoofed")
	request.Header.Set("X-Forwarded-Proto", "spoofed")
	request.Header.Set("Forwarded", "for=spoofed")
	request.Header.Set("Connection", "X-Hop, X-Request-ID")
	request.Header.Set("X-Hop", "removed")
	request.Header.Set("Idempotency-Key", "do-not-retry")
	request.Header.Set("X-Idempotency-Key", "do-not-retry")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request)
	got := <-requests
	if got.method != "PATCH" || got.uri != "/v1/menu-items/a%2Fb?tag=one&tag=two&q=a%20b" || got.body != body || got.host != backends[0].HTTPURL().Host {
		t.Fatalf("forwarded request=%+v", got)
	}
	for name, want := range map[string]string{"Content-Type": "application/json", "Accept": "application/json", "X-Demo-Role": "staff", "Authorization": "Bearer test-token", "X-Forwarded-For": "192.0.2.10", "X-Forwarded-Host": "balancer.local", "X-Forwarded-Proto": "http"} {
		if got.header.Get(name) != want {
			t.Errorf("%s=%q want %q", name, got.header.Get(name), want)
		}
	}
	for _, name := range []string{"Connection", "X-Hop", "Forwarded", "Idempotency-Key", "X-Idempotency-Key", "Accept-Encoding"} {
		if got.header.Get(name) != "" {
			t.Errorf("unexpected %s", name)
		}
	}
	id := w.Header().Get("X-Request-ID")
	if id == "" || id == "client-id" || got.header.Get("X-Request-ID") != id || len(w.Header().Values("X-Request-ID")) != 1 {
		t.Fatal("request ID mismatch")
	}
	if w.Code != 409 || !strings.Contains(w.Body.String(), id) || len(w.Header().Values("Set-Cookie")) != 2 {
		t.Fatalf("response=%v %s", w.Result(), w.Body)
	}
	if request.Header.Get("X-Request-ID") != "client-id" || request.Header.Get("Idempotency-Key") != "do-not-retry" || request.URL.Host != "balancer.local" {
		t.Fatal("incoming request changed")
	}
}

// Параллельные HTTP-запросы равномерно распределяются между экземплярами.
func TestHTTPProxyConcurrentDistribution(t *testing.T) {
	var counts [2]atomic.Int64
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { counts[i].Add(1); w.WriteHeader(204) }))
		defer servers[i].Close()
	}
	h, backends := testHTTPProxy(t, servers[0].URL, servers[1].URL)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/restaurants", nil))
			if w.Code != 204 {
				t.Errorf("status=%d", w.Code)
			}
		}()
	}
	wg.Wait()
	if counts[0].Load() != 20 || counts[1].Load() != 20 {
		t.Fatal("uneven distribution")
	}
	backends[0].SetHTTPHealthy(false)
	for i := 0; i < 4; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}
	if counts[0].Load() != 20 || counts[1].Load() != 24 {
		t.Fatal("unhealthy backend selected")
	}
	backends[0].SetHTTPHealthy(true)
	for i := 0; i < 2; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}
	if counts[0].Load() != 21 || counts[1].Load() != 25 {
		t.Fatal("backend not restored")
	}
}

// Отсутствие доступных экземпляров даёт 503 без сетевого вызова.
func TestHTTPProxyNoHealthyBackends(t *testing.T) {
	h, backends := testHTTPProxy(t, "http://unused:8082")
	backends[0].SetHTTPHealthy(false)
	h.proxies[backends[0]].Transport = proxyRoundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected network call"); return nil, nil })
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	assertProxyError(t, w, 503, "catalog_unavailable")
}

// Сетевой сбой и ошибка таймаута получают разные безопасные ответы.
func TestHTTPProxyTransportErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"connection", errors.New("secret-canary"), 502, "bad_gateway"},
		{"timeout", &net.DNSError{Err: "secret-canary", IsTimeout: true}, 504, "gateway_timeout"},
		{"deadline", fmt.Errorf("secret-canary: %w", context.DeadlineExceeded), 504, "gateway_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, b := testHTTPProxy(t, "http://unused:8082")
			h.proxies[b[0]].Transport = proxyRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, tc.err })
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
			assertProxyError(t, w, tc.status, tc.code)
		})
	}
}

// Более короткий дедлайн клиента сохраняется, собственный ограничивает ожидание двумя секундами.
func TestHTTPProxyDeadlines(t *testing.T) {
	for _, budget := range []time.Duration{0, 100 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, b := testHTTPProxy(t, "http://unused:8082")
				h.proxies[b[0]].Transport = proxyRoundTripFunc(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
				ctx := context.Background()
				want := httpRequestTimeout
				if budget != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, budget)
					defer cancel()
					want = budget
				}
				started := time.Now()
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
				assertProxyError(t, w, 504, "gateway_timeout")
				if elapsed := time.Since(started); elapsed != want {
					t.Fatalf("elapsed=%s want=%s", elapsed, want)
				}
			})
		})
	}
}

// Завершённый контекст не приводит к обращению в каталог.
func TestHTTPProxyAlreadyDone(t *testing.T) {
	for _, cancelled := range []bool{true, false} {
		h, b := testHTTPProxy(t, "http://unused:8082")
		h.proxies[b[0]].Transport = proxyRoundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("unexpected request")
			return nil, errors.New("unexpected")
		})
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		if cancelled {
			cancel()
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
		cancel()
		if cancelled {
			if w.Body.Len() != 0 || w.Flushed {
				t.Fatal("response written after cancellation")
			}
		} else {
			assertProxyError(t, w, 504, "gateway_timeout")
		}
	}
}

// Отмена клиента прерывает реальный запрос к серверу без JSON-ответа.
func TestHTTPProxyCancellation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(stopped) }))
	defer upstream.Close()
	h, _ := testHTTPProxy(t, upstream.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() { defer close(done); h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx)) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not stop")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not stop")
	}
	if w.Body.Len() != 0 {
		t.Fatal("response written after cancellation")
	}
}

// Ожидание заголовков ограничено таймаутом транспорта.
func TestHTTPProxyResponseHeaderTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer upstream.Close()
	h, _ := testHTTPProxy(t, upstream.URL)
	h.transport.ResponseHeaderTimeout = 20 * time.Millisecond
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	assertProxyError(t, w, 504, "gateway_timeout")
}

// Обрыв ответа после PATCH не вызывает повтор на том же или другом экземпляре.
func TestHTTPProxyDoesNotRetryPATCH(t *testing.T) {
	for _, body := range []string{"", `{"price_kopecks":123}`} {
		t.Run(fmt.Sprintf("body-%d", len(body)), func(t *testing.T) {
			var patches, otherCalls atomic.Int64
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.WriteHeader(204)
					return
				}
				patches.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { otherCalls.Add(1); w.WriteHeader(204) }))
			defer second.Close()
			h, b := testHTTPProxy(t, first.URL, second.URL)
			b[1].SetHTTPHealthy(false)
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/warm", nil))
			b[1].SetHTTPHealthy(true)
			h.pool.nextHTTP.Store(0)
			req := httptest.NewRequest("PATCH", "/v1/menu-items/one", strings.NewReader(body))
			req.Header.Set("Idempotency-Key", "key")
			req.Header.Set("X-Idempotency-Key", "key")
			req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(body)), nil }
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			assertProxyError(t, w, 502, "bad_gateway")
			if patches.Load() != 1 || otherCalls.Load() != 0 {
				t.Fatalf("patches=%d failover=%d", patches.Load(), otherCalls.Load())
			}
		})
	}
}

// Повторные запросы используют соединение, закрытие idle заставляет открыть новое.
func TestHTTPProxyConnectionReuse(t *testing.T) {
	var connections atomic.Int64
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()
	h, _ := testHTTPProxy(t, upstream.URL)
	for i := 0; i < 2; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}
	if connections.Load() != 1 {
		t.Fatalf("connections=%d", connections.Load())
	}
	h.CloseIdleConnections()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if connections.Load() != 2 {
		t.Fatalf("connections after close=%d", connections.Load())
	}
}

// Ответы каталога передаются без преобразования ошибок и перехода по редиректу.
func TestHTTPProxyPreservesResponses(t *testing.T) {
	for _, status := range []int{200, 400, 404, 409, 500, 503, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/other")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				io.WriteString(w, `{"upstream":true}`)
			}))
			defer upstream.Close()
			h, _ := testHTTPProxy(t, upstream.URL)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
			if w.Code != status || w.Body.String() != `{"upstream":true}` || w.Header().Get("Location") != "/other" || calls.Load() != 1 {
				t.Fatalf("unexpected response: %d %s", w.Code, w.Body)
			}
		})
	}
}
