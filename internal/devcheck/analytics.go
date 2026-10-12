package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// analyticsSnapshot сравнивает сырые доставки с итогами после исключения дублей.
type analyticsSnapshot struct {
	Raw     uint64  `json:"raw_events"`
	Orders  uint64  `json:"orders_created"`
	Total   int64   `json:"sum_kopecks"`
	Average float64 `json:"average_kopecks"`
}

// clickhouseSQL выполняет локальный SQL без передачи пароля в аргументах хоста.
func clickhouseSQL(ctx context.Context, query string) (string, error) {
	return compose(ctx, query, "exec", "-T", "clickhouse", "sh", "-ec", `exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery`)
}

// readAnalytics получает число доставок и агрегаты одного неизменяемого события.
func readAnalytics(ctx context.Context, eventID string) (analyticsSnapshot, error) {
	query := fmt.Sprintf(`SELECT
 (SELECT count() FROM analytics.order_events_raw WHERE event_id='%s') AS raw_events,
 count() AS orders_created, sum(total_kopecks) AS sum_kopecks,
 if(count()=0, 0, avg(total_kopecks)) AS average_kopecks
 FROM analytics.orders_created WHERE event_id='%s'
 SETTINGS output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow;`, eventID, eventID)
	data, err := clickhouseSQL(ctx, query)
	var result analyticsSnapshot
	if err == nil {
		err = json.Unmarshal([]byte(data), &result)
	}
	return result, err
}

// checkAnalytics проверяет реальные повторы Kafka и сохранность аналитики при рестарте.
func (r *runner) checkAnalytics(ctx context.Context) (err error) {
	fmt.Fprintln(os.Stderr, "[1/7] Создаю заказ и жду уведомление (до 90 секунд)...")
	order, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	if err = r.waitFaultDelivery(ctx, order); err != nil {
		return err
	}
	payload, err := sql(ctx, "orders_db", fmt.Sprintf("SELECT payload::text FROM outbox_events WHERE event_id='%s'::uuid;", order.EventID))
	if err != nil {
		return err
	}
	var event struct {
		Total int64 `json:"total_kopecks"`
	}
	if err = json.Unmarshal([]byte(payload), &event); err != nil {
		return err
	}
	var before, after analyticsSnapshot
	fmt.Fprintln(os.Stderr, "[2/7] Жду событие в ClickHouse (до 90 секунд)...")
	if err = eventually(ctx, "first analytics event", 90*time.Second, func(ctx context.Context) (bool, error) {
		var e error
		before, e = readAnalytics(ctx, order.EventID)
		return before.Raw >= 1 && before.Orders == 1 && before.Total == event.Total && before.Average == float64(event.Total), e
	}); err != nil {
		return err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"), kgo.RecordDeliveryTimeout(15*time.Second))
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Fprintln(os.Stderr, "[3/7] Публикую дубль и проверяю неизменность агрегатов (до 60 секунд)...")
	// Повторяем тот же event_id и payload, как при повторной доставке outbox.
	if err = client.ProduceSync(ctx, &kgo.Record{Topic: "food.order-created.v1", Key: []byte(order.ID), Value: []byte(payload)}).FirstErr(); err != nil {
		return err
	}
	if err = eventually(ctx, "duplicate excluded from analytics", 60*time.Second, func(ctx context.Context) (bool, error) {
		var e error
		after, e = readAnalytics(ctx, order.EventID)
		return after.Raw > before.Raw && after.Orders == 1 && after.Total == before.Total && after.Average == before.Average, e
	}); err != nil {
		return err
	}
	stopped := true
	defer func() {
		if stopped {
			fmt.Fprintln(os.Stderr, "Восстанавливаю ClickHouse после прерванной проверки...")
			err = errors.Join(err, startServices("clickhouse"))
		}
	}()
	fmt.Fprintln(os.Stderr, "[4/7] Останавливаю ClickHouse и проверяю заказ/уведомление без аналитики...")
	if _, err = compose(ctx, "", "stop", "clickhouse"); err != nil {
		return err
	}
	backlog, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	if err = r.waitFaultDelivery(ctx, backlog); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "[5/7] Запускаю ClickHouse и жду готовность контейнера...")
	if err = startServices("clickhouse"); err != nil {
		return err
	}
	stopped = false
	fmt.Fprintln(os.Stderr, "[6/7] Жду возобновление Kafka consumer и догрузку заказа (до 90 секунд)...")
	if err = eventually(ctx, "analytics backlog after restart", 90*time.Second, func(ctx context.Context) (bool, error) {
		snapshot, e := readAnalytics(ctx, backlog.EventID)
		return snapshot.Orders == 1 && snapshot.Raw >= 1, e
	}); err != nil {
		return err
	}
	persisted, err := readAnalytics(ctx, order.EventID)
	if err != nil {
		return err
	}
	if persisted.Raw < after.Raw || persisted.Orders != 1 || persisted.Total != before.Total || persisted.Average != before.Average {
		return fmt.Errorf("analytics changed after restart: %+v", persisted)
	}
	if err = r.waitFaultDelivery(ctx, order); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "[7/7] Проверяю SQL-запросы, consumer group и SQL-интерфейс...")
	queries, err := os.ReadFile("deploy/compose/clickhouse/queries.sql")
	if err != nil {
		return err
	}
	results, err := clickhouseSQL(ctx, string(queries))
	if err != nil {
		return err
	}
	groups, err := compose(ctx, "", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092", "--describe", "--group", "analytics-v1")
	if err != nil {
		return err
	}
	if !strings.Contains(groups, "food.order-created.v1") {
		return errors.New("analytics consumer group has no source topic")
	}
	page, code, _, err := r.request(ctx, "GET", "http://127.0.0.1:18123/play", nil, nil)
	if err != nil {
		return err
	}
	if code != 200 || !strings.Contains(strings.ToLower(string(page)), "<html") {
		return errors.New("ClickHouse SQL UI is unavailable")
	}
	result := map[string]any{
		"checked_at": time.Now().UTC(), "order": order, "backlog_order": backlog,
		"before_duplicate": before, "after_duplicate": after, "after_restart": persisted,
		"consumer_group": groups, "example_queries": results, "sql_ui": "http://localhost:18123/play",
		"notifications_independent": true,
	}
	if err = saveJSON("reports/stage9/analytics.json", result); err != nil {
		return err
	}
	if err = printJSON(result); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "PASS: ClickHouse — доставка, дедупликация и восстановление проверены.")
	return nil
}
