package devcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMetricsRejectMissing отличает отсутствие статистики от пустой очереди.
func TestMetricsRejectMissing(t *testing.T) {
	for _, text := range []string{"", "food_outbox_pending_events_broken 0", "food_outbox_pending_events NaN", "food_outbox_pending_events{service=\"x\"}"} {
		if _, err := metricTotal(text, "food_outbox_pending_events"); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
	got, err := metricTotal("food_outbox_pending_events{service=\"a\"} 2\nfood_outbox_pending_events{service=\"b\"} 3", "food_outbox_pending_events")
	if err != nil || got != 5 {
		t.Fatalf("got %v, %v", got, err)
	}
}

// TestSelectionsAndPercentile проверяет распределение и ближайший ранг без изменения исходных данных.
func TestSelectionsAndPercentile(t *testing.T) {
	got, err := selections("food_balancer_selections_total{backend=\"catalog-1\",protocol=\"grpc\",service=\"catalog-balancer\"} 7")
	if err != nil || got["grpc/catalog-1"] != 7 {
		t.Fatalf("got %v, %v", got, err)
	}
	values := []float64{30, 10, 20}
	if p := percentile(values, .95); p == nil || *p != 30 {
		t.Fatalf("wrong percentile: %v", p)
	}
	if values[0] != 30 {
		t.Fatal("input mutated")
	}
	if percentile(nil, .95) != nil {
		t.Fatal("empty percentile must be null")
	}
}

// TestDeliveryDetectsLossAndDuplicates допускает commit без 201, но запрещает потери и дубли.
func TestDeliveryDetectsLossAndDuplicates(t *testing.T) {
	load := deliveryLoad{RunID: strings.Repeat("a", 32), Requests: 3, StatusCounts: map[string]int{"201": 1}}
	orders := deliveryOrders{Created: 2, Published: 2, EventIDs: []string{"a", "b"}}
	for _, tc := range []struct {
		name  string
		ids   []string
		valid bool
	}{{"ambiguous commit", []string{"a", "b", "unrelated"}, true}, {"missing", []string{"a"}, false}, {"duplicate", []string{"a", "b", "a"}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDelivery(load, orders, tc.ids)
			if (err == nil) != tc.valid {
				t.Fatalf("got %v", err)
			}
		})
	}
}

// TestLoadWorkerAccountsResponses проверяет ответы, уникальные ключи и диагностические примеры.
func TestLoadWorkerAccountsResponses(t *testing.T) {
	var requests atomic.Int32
	keys := map[string]bool{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" || keys[key] {
			t.Error("missing or duplicate key")
		}
		keys[key] = true
		n := requests.Add(1)
		switch n {
		case 1:
			w.WriteHeader(201)
			fmt.Fprint(w, `{"id":"order-1"}`)
		case 2:
			w.WriteHeader(503)
			fmt.Fprint(w, `{"code":"unavailable"}`)
		case 3:
			fmt.Fprint(w, `{}`)
		default:
			cancel()
			w.WriteHeader(429)
		}
	}))
	defer server.Close()
	r := newRunner()
	r.orders = server.URL
	result := r.loadWorker(ctx, loadOptions{scenario: "orders", clients: 1}, strings.Repeat("b", 32), []string{"user-1"}, 0, time.Now().Add(time.Second))
	if result.codes["201"] != 1 || result.codes["503"] != 1 || result.codes["transport_or_validation_error"] < 1 {
		t.Fatalf("codes: %v", result.codes)
	}
	if len(result.successful) != 1 || result.sample == nil || result.sample.OrderID != "order-1" {
		t.Fatalf("result: %+v", result)
	}
	if len(result.examples) < 2 || result.examples[0]["idempotency_key"] == "" {
		t.Fatal("missing error trace")
	}
}

// TestHTTPBoundAndCancellation проверяет лимит тела и остановку зависшего запроса.
func TestHTTPBoundAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/large" {
			fmt.Fprint(w, strings.Repeat("x", (8<<20)+1))
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	r := newRunner()
	if _, _, _, err := r.request(context.Background(), "GET", server.URL+"/large", nil, nil); err == nil {
		t.Fatal("oversized response accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, _, err := r.request(ctx, "GET", server.URL+"/slow", nil, nil); err == nil {
		t.Fatal("expected cancellation")
	}
}

// TestMonitorStops проверяет завершение фоновой горутины по контексту.
func TestMonitorStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan outboxStats, 1)
	go func() { done <- newRunner().monitorOutbox(ctx) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor leaked")
	}
}

// TestMenuValidation отвергает ошибочные ответы даже с HTTP 200.
func TestMenuValidation(t *testing.T) {
	for _, body := range []string{`{}`, `[]`, `[{"price_kopecks":10}]`, `null`} {
		if decodeMenu([]byte(body)) == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	if err := decodeMenu([]byte(`[{"id":"dish","price_kopecks":10}]`)); err != nil {
		t.Fatal(err)
	}
}

// TestReportRoundTrip сохраняет исходный формат отчёта для проверки доставки.
func TestReportRoundTrip(t *testing.T) {
	path := t.TempDir() + "/report.json"
	data := map[string]any{
		"run_id":        strings.Repeat("c", 32),
		"requests":      2,
		"status_counts": map[string]int{"201": 1},
		"sample_orders": []orderSample{{OrderID: "order", UserID: "user"}},
	}
	if err := saveJSON(path, data); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var load deliveryLoad
	if err = json.Unmarshal(encoded, &load); err != nil {
		t.Fatal(err)
	}
	if load.Samples[0].UserID != "user" || load.StatusCounts["201"] != 1 {
		t.Fatalf("bad report: %+v", load)
	}
}

// TestPageAcceptsHTML допускает HTML и JavaScript вместо требования JSON от GUI.
func TestPageAcceptsHTML(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "*/*" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		fmt.Fprint(w, "<html>GUI</html>")
	}))
	defer server.Close()
	data, err := newRunner().page(context.Background(), server.URL)
	if err != nil || !strings.Contains(string(data), "<html>") {
		t.Fatalf("page: %s, %v", data, err)
	}
}
