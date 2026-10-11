package balancer

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Доступность читается из текущих флагов, реестры экземпляров изолированы.
func TestBalancerMetricsHealth(t *testing.T) {
	pool, b := testPool(t, 2)
	m := NewMetrics(pool)
	check := func(want map[string]float64) {
		t.Helper()
		families, err := m.registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, family := range families {
			if family.GetName() == "food_balancer_backend_up" {
				for _, metric := range family.Metric {
					labels := map[string]string{}
					for _, label := range metric.Label {
						labels[label.GetName()] = label.GetValue()
					}
					got[labels["protocol"]+"/"+labels["backend"]] = metric.GetGauge().GetValue()
				}
			}
		}
		if len(got) != len(want) {
			t.Fatalf("gauges=%v", got)
		}
		for key, value := range want {
			if got[key] != value {
				t.Errorf("%s=%v want=%v", key, got[key], value)
			}
		}
	}
	check(map[string]float64{"http/catalog-1": 0, "grpc/catalog-1": 0, "http/catalog-2": 0, "grpc/catalog-2": 0})
	b[0].SetHTTPHealthy(true)
	b[1].SetGRPCHealthy(true)
	check(map[string]float64{"http/catalog-1": 1, "grpc/catalog-1": 0, "http/catalog-2": 0, "grpc/catalog-2": 1})
	// Повторное создание не использует глобальный реестр Prometheus.
	other := NewMetrics(pool)
	if m.registry == other.registry {
		t.Fatal("shared registry")
	}
}

// Счётчики потокобезопасны, а произвольные значения не попадают в метки.
func TestBalancerMetricsRequests(t *testing.T) {
	pool, b := testPool(t, 1)
	m := NewMetrics(pool)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.ObserveSelection("http", b[0])
			m.ObserveRequest("http", b[0], "200", 10*time.Millisecond)
		}()
	}
	wg.Wait()
	m.ObserveRequest("grpc", nil, "Unavailable", time.Millisecond)
	m.ObserveRequest("http", b[0], "aborted", time.Millisecond)
	m.ObserveRequest("http", &Backend{name: "private-id"}, "private-status", time.Millisecond)
	m.ObserveRequest("grpc", nil, "private-status", time.Millisecond)
	m.ObserveRequest("private-protocol", b[0], "200", time.Millisecond)
	m.ObserveSelection("http", &Backend{name: "private-id"})
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "private-") {
		t.Fatalf("status=%d unexpected metric label", w.Code)
	}
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var selections, requests float64
	var durations uint64
	for _, f := range families {
		for _, metric := range f.Metric {
			switch f.GetName() {
			case "food_balancer_selections_total":
				selections += metric.GetCounter().GetValue()
			case "food_balancer_requests_total":
				requests += metric.GetCounter().GetValue()
			case "food_balancer_request_duration_seconds":
				durations += metric.GetHistogram().GetSampleCount()
			}
		}
	}
	if selections != 20 || requests != 24 || durations != 24 {
		t.Fatalf("selections=%v requests=%v durations=%v", selections, requests, durations)
	}
}

// Прокси отклоняют отсутствующие метрики и реестр другого пула.
func TestProxyMetricsConfiguration(t *testing.T) {
	pool, _ := testPool(t, 1)
	other, _ := testPool(t, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, m := range []*Metrics{nil, NewMetrics(other)} {
		if h, err := NewHTTPProxy(pool, logger, m); h != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("HTTP proxy=%v err=%v", h, err)
		}
		if h, err := NewGRPCProxy(pool, m); h != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("gRPC proxy=%v err=%v", h, err)
		}
	}
}
