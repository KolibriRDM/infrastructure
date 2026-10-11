package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// deliveryLoad содержит поля нагрузочного отчёта, нужные для сверки доставки.
type deliveryLoad struct {
	RunID        string         `json:"run_id"`
	Requests     int            `json:"requests"`
	StatusCounts map[string]int `json:"status_counts"`
	Samples      []orderSample  `json:"sample_orders"`
}

// deliveryOrders описывает события заказов одного нагрузочного запуска.
type deliveryOrders struct {
	Created   int      `json:"created"`
	Published int      `json:"published"`
	EventIDs  []string `json:"event_ids"`
}

// sql выполняет диагностический запрос через psql внутри PostgreSQL-контейнера.
func sql(ctx context.Context, db, query string) (string, error) {
	return compose(ctx, query, "exec", "-T", "postgres", "sh", "-ec", `export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U "$POSTGRES_USER" -d "$1" -At -v ON_ERROR_STOP=1`, "sh", db)
}

// validateDelivery учитывает подтверждённые и неоднозначно завершившиеся HTTP-запросы.
func validateDelivery(load deliveryLoad, orders deliveryOrders, received []string) error {
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(load.RunID) {
		return errors.New("invalid load run ID")
	}
	if load.Requests <= 0 || orders.Created <= 0 || load.StatusCounts["201"] > orders.Created || orders.Created > load.Requests || orders.Published != orders.Created {
		return fmt.Errorf("delivery counts do not match: acknowledged=%d created=%d published=%d requests=%d", load.StatusCounts["201"], orders.Created, orders.Published, load.Requests)
	}
	expected := map[string]int{}
	for _, id := range orders.EventIDs {
		if id == "" {
			return errors.New("empty event ID")
		}
		if _, exists := expected[id]; exists {
			return errors.New("duplicate outbox event")
		}
		expected[id] = 0
	}
	if len(expected) != orders.Created {
		return errors.New("outbox event count does not match created orders")
	}
	for _, id := range received {
		if _, exists := expected[id]; exists {
			expected[id]++
		}
	}
	for id, count := range expected {
		if count != 1 {
			return fmt.Errorf("event %s has %d notifications, expected 1", id, count)
		}
	}
	return nil
}

// checkDelivery сверяет PostgreSQL и HTTP-уведомления для выбранного нагрузочного отчёта.
func (r *runner) checkDelivery(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("check-load-delivery", flag.ContinueOnError)
	input := flags.String("input", "reports/stage8/orders-load.json", "load report")
	output := flags.String("output", "reports/stage8/delivery-check.json", "delivery report")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected delivery arguments")
	}
	data, err := os.ReadFile(*input)
	if err != nil {
		return err
	}
	var load deliveryLoad
	if err = json.Unmarshal(data, &load); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(load.RunID) {
		return errors.New("invalid load run ID")
	}
	// Строгая проверка hex-идентификатора делает SQL-префикс безопасным.
	query := fmt.Sprintf("SELECT json_build_object('created',count(*),'published',count(*) FILTER (WHERE e.published_at IS NOT NULL),'event_ids',json_agg(e.event_id::text)) FROM idempotency_keys k JOIN outbox_events e USING(order_id) WHERE k.key LIKE 'load-%s-%%';", load.RunID)
	raw, err := sql(ctx, "orders_db", query)
	if err != nil {
		return err
	}
	var orders deliveryOrders
	if err = json.Unmarshal([]byte(raw), &orders); err != nil {
		return err
	}
	raw, err = sql(ctx, "notifications_db", "SELECT event_id::text FROM notifications;")
	if err != nil {
		return err
	}
	if err = validateDelivery(load, orders, strings.Fields(raw)); err != nil {
		return err
	}
	for _, sample := range load.Samples {
		if sample.OrderID == "" || sample.UserID == "" {
			return errors.New("invalid order sample")
		}
		var list notificationList
		if err = r.getJSON(ctx, "GET", r.notifications+"/v1/orders/"+sample.OrderID+"/notifications", nil, map[string]string{"X-Demo-User-ID": sample.UserID}, &list); err != nil {
			return err
		}
		if len(list.Items) != 1 || list.Items[0].OrderID != sample.OrderID || list.Items[0].UserID != sample.UserID {
			return errors.New("HTTP notification does not match order")
		}
	}
	result := map[string]any{
		"run_id":                    load.RunID,
		"created":                   orders.Created,
		"published":                 orders.Published,
		"http_created_acknowledged": load.StatusCounts["201"],
		"committed_without_201":     orders.Created - load.StatusCounts["201"],
		"notifications":             orders.Created,
		"missing":                   0,
		"duplicate_events":          0,
		"http_samples_checked":      len(load.Samples),
		"verified_at":               float64(time.Now().UnixNano()) / 1e9,
	}
	if err = saveJSON(*output, result); err != nil {
		return err
	}
	return printJSON(result)
}
