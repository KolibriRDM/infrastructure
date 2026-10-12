package devcheck

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const restaurantID = "00000000-0000-4000-8000-000000000001"
const itemID = "00000000-0000-4000-8000-000000000101"
const menuPath = "/v1/restaurants/" + restaurantID + "/menu"
const cacheKey = "menu:v1:" + restaurantID

// runner содержит адреса локального стенда и клиент с ограничением времени.
type runner struct {
	client        *http.Client
	catalog       string
	orders        string
	notifications string
	balancer      string
	elastic       string
	kafkaUI       string
	redisUI       string
}

// newRunner задаёт адреса опубликованных портов Compose.
func newRunner() *runner {
	return &runner{
		client:        &http.Client{Timeout: 10 * time.Second},
		catalog:       "http://127.0.0.1:8082",
		orders:        "http://127.0.0.1:8080",
		notifications: "http://127.0.0.1:8081",
		balancer:      "http://127.0.0.1:8084",
		elastic:       "http://127.0.0.1:9200",
		kafkaUI:       "http://127.0.0.1:8083",
		redisUI:       "http://127.0.0.1:5540",
	}
}

// Run выбирает проверку; запускать команду нужно из корня infrastructure.
func Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: devcheck demo-order|check-menu-cache|check-order-rate-limit|check-redis|check-logs|check-guis|check-load-delivery|check-faults|logs-setup|check-kibana|check-acceptance|check-analytics|load menu|orders|free-port")
	}
	r := newRunner()
	switch args[0] {
	case "check-analytics":
		return r.checkAnalytics(ctx)
	case "check-acceptance":
		return r.checkAcceptance(ctx)
	case "logs-setup":
		return r.setupLogs(ctx)
	case "check-kibana":
		return r.checkKibana(ctx)
	case "check-faults":
		return r.checkFaults(ctx)
	case "demo-order":
		return r.demoOrder(ctx)
	case "check-menu-cache":
		return r.checkMenuCache(ctx)
	case "check-order-rate-limit":
		return r.checkRateLimit(ctx)
	case "check-redis":
		return r.checkRedis(ctx)
	case "check-logs":
		return r.checkLogs(ctx)
	case "check-guis":
		return r.checkGUIs(ctx)
	case "check-load-delivery":
		return r.checkDelivery(ctx, args[1:])
	case "load":
		return r.load(ctx, args[1:])
	case "free-port":
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return err
		}
		fmt.Println(listener.Addr().(*net.TCPAddr).Port)
		return listener.Close()
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// request возвращает тело и статус, включая HTTP-ошибки приложения.
func (r *runner) request(ctx context.Context, method, url string, payload any, headers map[string]string) ([]byte, int, http.Header, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, nil, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err == nil && len(data) > 8<<20 {
		err = errors.New("HTTP response exceeds 8 MiB")
	}
	return data, resp.StatusCode, resp.Header, err
}

// getJSON требует успешный ответ и разбирает его в заданную модель.
func (r *runner) getJSON(ctx context.Context, method, url string, payload any, headers map[string]string, target any) error {
	data, status, _, err := r.request(ctx, method, url, payload, headers)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %.250s", method, url, status, data)
	}
	if target == nil {
		return nil
	}
	return json.Unmarshal(data, target)
}

// command ограничивает время внешнего процесса и сохраняет stderr при ошибке.
func command(ctx context.Context, input string, name string, args ...string) (string, error) {
	return commandWithTimeout(ctx, 90*time.Second, input, name, args...)
}

// commandWithTimeout задаёт отдельный срок для длительного запуска Elastic Stack.
func commandWithTimeout(ctx context.Context, timeout time.Duration, input string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

// compose выполняет команду в локальном проекте Compose.
func compose(ctx context.Context, input string, args ...string) (string, error) {
	return command(ctx, input, "docker", append([]string{"compose", "--env-file", ".env", "-f", "deploy/compose/compose.yaml"}, args...)...)
}

// redis выполняет диагностическую команду Redis внутри контейнера.
func redis(ctx context.Context, args ...string) (string, error) {
	return compose(ctx, "", append([]string{"exec", "-T", "redis", "redis-cli", "--raw"}, args...)...)
}

// wait приостанавливает цикл с немедленной реакцией на отмену.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// randomID создаёт случайный идентификатор без дополнительных библиотек.
func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// uuid форматирует случайные байты как UUID версии 4.
func uuid() (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	b, _ := hex.DecodeString(id)
	// Старшие биты задают версию UUID и стандартный вариант.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// printJSON выводит результат проверки в читаемом виде.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// saveJSON сохраняет отчёт через переименование полностью записанного файла.
func saveJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".report-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(append(data, '\n'))
	closeErr := f.Close()
	if err = errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// orderPayload возвращает небольшой заказ на демонстрационное блюдо.
func orderPayload() any {
	return map[string]any{"restaurant_id": restaurantID, "delivery_address": "Москва, Учебная, 1", "items": []any{map[string]any{"menu_item_id": itemID, "quantity": 1}}}
}

// menuItem описывает необходимые проверке поля публичного меню.
type menuItem struct {
	ID    string `json:"id"`
	Price int    `json:"price_kopecks"`
}

// notification описывает связь уведомления с заказом и событием.
type notification struct {
	ID      string `json:"id"`
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	EventID string `json:"event_id"`
}

// notificationList соответствует HTTP-ответу сервиса уведомлений.
type notificationList struct {
	Items []notification `json:"items"`
}

// orderSample сохраняет пару заказа и пользователя для последующей сверки.
type orderSample struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

// demoOrder создаёт заказ и ждёт его уведомление из Kafka.
func (r *runner) demoOrder(ctx context.Context) error {
	user := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	key, err := randomID()
	if err != nil {
		return err
	}
	var order struct {
		ID string `json:"id"`
	}
	if err = r.getJSON(ctx, "POST", r.orders+"/v1/orders", orderPayload(), map[string]string{"X-Demo-User-ID": user, "Idempotency-Key": "compose-demo-" + key}, &order); err != nil {
		return err
	}
	if order.ID == "" {
		return errors.New("missing order ID")
	}
	fmt.Println("Создан заказ:", order.ID)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		var list notificationList
		if err = r.getJSON(ctx, "GET", r.notifications+"/v1/orders/"+order.ID+"/notifications", nil, map[string]string{"X-Demo-User-ID": user}, &list); err != nil {
			return err
		}
		if len(list.Items) > 0 {
			if len(list.Items) != 1 || list.Items[0].OrderID != order.ID || list.Items[0].UserID != user {
				return errors.New("notification does not match order")
			}
			n := list.Items[0]
			return printJSON(map[string]any{
				"order_id":        order.ID,
				"event_id":        n.EventID,
				"notification_id": n.ID,
				"topic":           "food.order-created.v1",
				"consumer_group":  "notifications-v1",
				"kafka_ui":        "http://localhost:8083",
			})
		}
		if err = wait(ctx, 250*time.Millisecond); err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
	}
}
