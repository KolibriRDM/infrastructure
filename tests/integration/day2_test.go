//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const restaurantID = "00000000-0000-4000-8000-000000000001"
const userID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const otherUserID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const pizzaID = "00000000-0000-4000-8000-000000000101"
const drinkID = "00000000-0000-4000-8000-000000000103"

// Проверяет всю цепочку двух процессов с PostgreSQL и отказами.
func TestDay2Processes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ordersPool := testPool(t, ctx, "DAY2_ORDERS_DATABASE_URL", "ORDERS_DB_PASSWORD", "orders_test_day2_")
	catalogPool := testPool(t, ctx, "DAY2_CATALOG_DATABASE_URL", "CATALOG_DB_PASSWORD", "catalog_test_day2_")
	defer ordersPool.Close()
	defer catalogPool.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	catalogAddress := freeAddress(t)
	catalogGRPCAddress := freeAddress(t)
	_, stopCatalog := startProcess(t, ctx, client, "DAY2_CATALOG_BINARY", map[string]string{
		"CATALOG_HTTP_ADDR":    catalogAddress,
		"CATALOG_GRPC_ADDR":    catalogGRPCAddress,
		"CATALOG_DATABASE_URL": os.Getenv("DAY2_CATALOG_DATABASE_URL"),
	})
	defer stopCatalog()
	startOrders := func() (string, func()) {
		return startProcess(t, ctx, client, "DAY2_ORDERS_BINARY", map[string]string{
			"ORDERS_HTTP_ADDR":    freeAddress(t),
			"ORDERS_DATABASE_URL": os.Getenv("DAY2_ORDERS_DATABASE_URL"),
			"CATALOG_GRPC_ADDR":   catalogGRPCAddress,
		})
	}
	ordersURL, stopOrders := startOrders()
	defer stopOrders()
	counts := func() [4]int {
		t.Helper()
		var n [4]int
		if err := ordersPool.QueryRow(ctx, `SELECT (SELECT count(*) FROM orders), (SELECT count(*) FROM order_items), (SELECT count(*) FROM idempotency_keys), (SELECT count(*) FROM outbox_events)`).Scan(&n[0], &n[1], &n[2], &n[3]); err != nil {
			t.Fatal(err)
		}
		return n
	}
	assertDelta := func(before [4]int, orderCount int) {
		t.Helper()
		want := before
		for i, delta := range [4]int{orderCount, orderCount * 2, orderCount, orderCount} {
			want[i] += delta
		}
		if got := counts(); got != want {
			t.Fatalf("table counts=%v want=%v", got, want)
		}
	}
	request := CreateOrderRequest{RestaurantID: restaurantID, DeliveryAddress: " Москва, Тестовая, 1 ", Items: []CreateOrderItemRequest{{MenuItemID: pizzaID, Quantity: 2}, {MenuItemID: drinkID, Quantity: 1}}}
	post := func(key, user string, body CreateOrderRequest, status int) apiResponse {
		t.Helper()
		response, err := apiRequest(ctx, client, http.MethodPost, ordersURL+"/v1/orders", user, key, body)
		if err != nil {
			t.Fatal(err)
		}
		if response.status != status {
			t.Fatalf("POST status=%d want=%d body=%s", response.status, status, response.body)
		}
		return response
	}
	get := func(path, user string, status int) apiResponse {
		t.Helper()
		response, err := apiRequest(ctx, client, http.MethodGet, ordersURL+path, user, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if response.status != status {
			t.Fatalf("GET status=%d want=%d body=%s", response.status, status, response.body)
		}
		return response
	}
	decodeOrder := func(response apiResponse) OrderResponse {
		t.Helper()
		var order OrderResponse
		if err := json.Unmarshal(response.body, &order); err != nil {
			t.Fatal(err)
		}
		return order
	}

	firstResponse := post("first", userID, request, 201)
	first := decodeOrder(firstResponse)
	if first.ID == "" || first.TotalKopecks != 79900 || first.Status != "created" || first.Version != 1 || first.DeliveryAddress != "Москва, Тестовая, 1" || firstResponse.location != "/v1/orders/"+first.ID {
		t.Fatalf("created order=%+v", first)
	}
	assertDelta([4]int{}, 1)
	t.Run("read and canonical replay", func(t *testing.T) {
		read := decodeOrder(get("/v1/orders/"+first.ID, userID, 200))
		if !reflect.DeepEqual(read, first) {
			t.Fatalf("read=%+v first=%+v", read, first)
		}
		reordered := request
		reordered.Items = []CreateOrderItemRequest{request.Items[1], request.Items[0]}
		reordered.DeliveryAddress = strings.TrimSpace(request.DeliveryAddress)
		replay := decodeOrder(post("first", strings.ToUpper(userID), reordered, 200))
		if !reflect.DeepEqual(replay, first) {
			t.Fatalf("replay=%+v first=%+v", replay, first)
		}
		assertDelta([4]int{}, 1)
	})
	t.Run("foreign and missing orders are indistinguishable", func(t *testing.T) {
		for _, response := range []apiResponse{get("/v1/orders/"+first.ID, otherUserID, 404), get("/v1/orders/ffffffff-ffff-4fff-8fff-ffffffffffff", userID, 404)} {
			var body ErrorResponse
			if err := json.Unmarshal(response.body, &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != "not_found" || strings.Contains(string(response.body), first.DeliveryAddress) {
				t.Fatalf("response=%s", response.body)
			}
		}
	})
	t.Run("changed body conflicts", func(t *testing.T) {
		before := counts()
		changed := request
		changed.DeliveryAddress = "Другой адрес"
		post("first", userID, changed, 409)
		assertDelta(before, 0)
	})
	t.Run("key scoped by user", func(t *testing.T) {
		before := counts()
		other := decodeOrder(post("first", otherUserID, request, 201))
		if other.ID == first.ID || other.UserID != otherUserID {
			t.Fatalf("other order=%+v", other)
		}
		assertDelta(before, 1)
	})
	t.Run("menu updates preserve snapshots", func(t *testing.T) {
		if _, err := catalogPool.Exec(ctx, "UPDATE menu_items SET price_kopecks=30000 WHERE id=$1::uuid", pizzaID); err != nil {
			t.Fatal(err)
		}
		fresh := decodeOrder(post("new-price", userID, request, 201))
		if fresh.TotalKopecks != 89900 || fresh.Items[0].UnitPriceKopecks != 30000 {
			t.Fatalf("new order=%+v", fresh)
		}
		replay := decodeOrder(post("first", userID, request, 200))
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("old snapshot changed: %+v", replay)
		}
	})
	t.Run("outbox stores contract and stays pending", func(t *testing.T) {
		var raw []byte
		var eventID, eventType string
		var pending bool
		var attempts int
		if err := ordersPool.QueryRow(ctx, `SELECT event_id::text,type,payload,published_at IS NULL,attempts FROM outbox_events WHERE order_id=$1::uuid`, first.ID).Scan(&eventID, &eventType, &raw, &pending, &attempts); err != nil {
			t.Fatal(err)
		}
		var payload struct {
			EventID string    `json:"event_id"`
			Type    string    `json:"type"`
			Version int       `json:"schema_version"`
			At      time.Time `json:"occurred_at"`
			OrderID string    `json:"order_id"`
			UserID  string    `json:"user_id"`
			Total   int64     `json:"total_kopecks"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if eventType != "order.created.v1" || payload.Type != eventType || payload.EventID != eventID || payload.Version != 1 || !payload.At.Equal(first.CreatedAt) || payload.OrderID != first.ID || payload.UserID != userID || payload.Total != 79900 || !pending || attempts != 0 || strings.Contains(string(raw), "delivery_address") {
			t.Fatalf("event=%s pending=%v attempts=%d", raw, pending, attempts)
		}
	})
	t.Run("ten concurrent repeats", func(t *testing.T) {
		before := counts()
		type result struct {
			response apiResponse
			err      error
		}
		results := make(chan result, 10)
		start := make(chan struct{})
		for range 10 {
			go func() {
				<-start
				response, err := apiRequest(ctx, client, http.MethodPost, ordersURL+"/v1/orders", userID, "concurrent", request)
				results <- result{response, err}
			}()
		}
		// Закрытие канала одновременно разрешает всем горутинам начать работу.
		close(start)
		created := 0
		winner := ""
		for range 10 {
			r := <-results
			if r.err != nil {
				t.Error(r.err)
				continue
			}
			if r.response.status == 201 {
				created++
			} else if r.response.status != 200 {
				t.Errorf("concurrent status=%d body=%s", r.response.status, r.response.body)
				continue
			}
			order := decodeOrder(r.response)
			if winner == "" {
				winner = order.ID
			}
			if order.ID != winner {
				t.Errorf("multiple orders: %s and %s", winner, order.ID)
			}
		}
		if created != 1 {
			t.Fatalf("created=%d want=1", created)
		}
		assertDelta(before, 1)
	})
	t.Run("outbox failure rolls back complete order", func(t *testing.T) {
		before := counts()
		if _, err := ordersPool.Exec(ctx, `ALTER TABLE outbox_events ADD CONSTRAINT day2_reject_event CHECK (false) NOT VALID`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := ordersPool.Exec(ctx, `ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS day2_reject_event`); err != nil {
				t.Error(err)
			}
		}()
		post("rollback", userID, request, 500)
		assertDelta(before, 0)
		if _, err := ordersPool.Exec(ctx, `ALTER TABLE outbox_events DROP CONSTRAINT day2_reject_event`); err != nil {
			t.Fatal(err)
		}
		post("rollback", userID, request, 201)
		assertDelta(before, 1)
	})
	t.Run("unavailable dish leaves no writes", func(t *testing.T) {
		before := counts()
		if _, err := catalogPool.Exec(ctx, "UPDATE menu_items SET is_available=false WHERE id=$1::uuid", pizzaID); err != nil {
			t.Fatal(err)
		}
		post("unavailable-dish", userID, request, 400)
		assertDelta(before, 0)
		post("first", userID, request, 200)
	})
	t.Run("restart orders while catalog is down", func(t *testing.T) {
		stopCatalog()
		stopOrders()
		ordersURL, stopOrders = startOrders()
		defer stopOrders()
		before := counts()
		get("/healthz", "", 200)
		get("/readyz", "", 200)
		read := decodeOrder(get("/v1/orders/"+first.ID, userID, 200))
		if !reflect.DeepEqual(read, first) {
			t.Fatalf("order lost after restart: %+v", read)
		}
		replay := decodeOrder(post("first", userID, request, 200))
		if !reflect.DeepEqual(replay, first) {
			t.Fatalf("replay changed after restart: %+v", replay)
		}
		post("catalog-down", userID, request, 503)
		assertDelta(before, 0)
	})
}

// Статус, тело и Location ответа в сквозных тестах.
type apiResponse struct {
	status   int
	body     []byte
	location string
}

// Выполняет HTTP-запрос и проверяет служебные заголовки.
func apiRequest(ctx context.Context, client *http.Client, method, url, user, key string, payload any) (apiResponse, error) {
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return apiResponse{}, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return apiResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Demo-User-ID", user)
	req.Header.Set("Idempotency-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 128<<10))
	if err != nil {
		return apiResponse{}, err
	}
	if resp.Header.Get("X-Request-ID") == "" || resp.Header.Get("Content-Type") != "application/json" {
		return apiResponse{}, fmt.Errorf("missing HTTP headers, status %d", resp.StatusCode)
	}
	return apiResponse{resp.StatusCode, body, resp.Header.Get("Location")}, nil
}

// Подключается только к изолированной тестовой базе.
func testPool(t *testing.T, ctx context.Context, dsnKey, passwordKey, prefix string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv(dsnKey))
	if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, prefix) {
		t.Fatal("use scripts/test-day2.sh with isolated test databases")
	}
	cfg.ConnConfig.Password = os.Getenv(passwordKey)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("create test database pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal("connect to test database")
	}
	return pool
}

// Выбирает свободный локальный порт для тестового процесса.
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// Запускает сервис, ждёт readiness и готовит его остановку.
func startProcess(t *testing.T, ctx context.Context, client *http.Client, binaryKey string, settings map[string]string) (string, func()) {
	t.Helper()
	binary := os.Getenv(binaryKey)
	if binary == "" {
		t.Fatal("use scripts/test-day2.sh to build test processes")
	}
	cmd := exec.CommandContext(ctx, binary)
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	for key, value := range settings {
		env[key] = value
	}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	logFile, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal("start test process", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			select {
			case <-done:
			default:
				_ = cmd.Process.Signal(os.Interrupt)
			}
			select {
			case <-done:
			case <-time.After(7 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("process did not shut down gracefully")
			}
			_ = logFile.Close()
			if waitErr != nil {
				t.Errorf("%s exited with error: %v", binaryKey, waitErr)
			}
		})
	}
	t.Cleanup(stop)
	address := settings["ORDERS_HTTP_ADDR"]
	if address == "" {
		address = settings["CATALOG_HTTP_ADDR"]
	}
	url := "http://" + address
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			t.Fatal("test process exited before readiness", binaryKey, waitErr)
		case <-deadline.C:
			t.Fatal("test process readiness timeout", binaryKey)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
			response, err := apiRequest(ctx, client, http.MethodGet, url+"/readyz", "", "", nil)
			if err == nil && response.status == 200 {
				return url, stop
			}
		}
	}
}
