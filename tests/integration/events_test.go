//go:build integration && e2e

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Представляет уведомление публичного HTTP API без импорта кода сервиса.
type eventNotification struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Kind      string    `json:"kind"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Проверяет три процесса, реальную Kafka, дедупликацию и доставку после простоя.
func TestOrderNotificationEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	ordersPool := testPool(t, ctx, "EVENTS_ORDERS_DATABASE_URL", "ORDERS_DB_PASSWORD", "orders_test_events_")
	defer ordersPool.Close()
	notificationsPool := testPool(t, ctx, "EVENTS_NOTIFICATIONS_DATABASE_URL", "NOTIFICATIONS_DB_PASSWORD", "notifications_test_events_")
	defer notificationsPool.Close()
	brokers := os.Getenv("EVENTS_KAFKA_BROKERS")
	if brokers == "" {
		t.Fatal("use scripts/test-events-e2e.sh")
	}
	topic := "food-events-test-" + strings.ToLower(rand.Text())
	group := topic + "-notifications"
	kafka, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	create := kmsg.NewPtrCreateTopicsRequest()
	create.TimeoutMillis = 10000
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, kafka)
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Topics) != 1 {
		t.Fatalf("create topic response=%+v", created)
	}
	if err := kerr.ErrorForCode(created.Topics[0].ErrorCode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deleteEventResources(t, kafka, topic, group) })
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	grpcAddress := freeAddress(t)
	_, stopCatalog := startProcess(t, ctx, client, "EVENTS_CATALOG_BINARY", map[string]string{
		"CATALOG_HTTP_ADDR": freeAddress(t), "CATALOG_GRPC_ADDR": grpcAddress,
		"CATALOG_DATABASE_URL": os.Getenv("EVENTS_CATALOG_DATABASE_URL"), "LOG_LEVEL": "info",
	})
	defer stopCatalog()
	ordersURL, stopOrders := startProcess(t, ctx, client, "EVENTS_ORDERS_BINARY", map[string]string{
		"ORDERS_HTTP_ADDR": freeAddress(t), "ORDERS_DATABASE_URL": os.Getenv("EVENTS_ORDERS_DATABASE_URL"),
		"CATALOG_GRPC_ADDR": grpcAddress, "KAFKA_BROKERS": brokers, "ORDER_CREATED_TOPIC": topic, "LOG_LEVEL": "info",
	})
	defer stopOrders()
	startNotifications := func() (string, func()) {
		return startProcess(t, ctx, client, "EVENTS_NOTIFICATIONS_BINARY", map[string]string{
			"NOTIFICATIONS_HTTP_ADDR": freeAddress(t), "NOTIFICATIONS_DATABASE_URL": os.Getenv("EVENTS_NOTIFICATIONS_DATABASE_URL"),
			"KAFKA_BROKERS": brokers, "ORDER_CREATED_TOPIC": topic, "KAFKA_CONSUMER_GROUP": group, "LOG_LEVEL": "info",
		})
	}
	notificationsURL, stopNotifications := startNotifications()
	defer stopNotifications()
	request := CreateOrderRequest{RestaurantID: restaurantID, DeliveryAddress: "Москва, Сквозная, 1", Items: []CreateOrderItemRequest{{MenuItemID: pizzaID, Quantity: 2}, {MenuItemID: drinkID, Quantity: 1}}}
	postOrder := func(key string, status int) OrderResponse {
		t.Helper()
		response, err := apiRequest(ctx, client, http.MethodPost, ordersURL+"/v1/orders", userID, key, request)
		if err != nil {
			t.Fatal(err)
		}
		if response.status != status {
			t.Fatalf("POST status=%d want=%d body=%s", response.status, status, response.body)
		}
		var order OrderResponse
		if err := json.Unmarshal(response.body, &order); err != nil {
			t.Fatal(err)
		}
		if order.ID == "" || order.UserID != userID || order.TotalKopecks != 79900 {
			t.Fatalf("order=%+v", order)
		}
		return order
	}
	waitPublished := func(orderID string) (string, []byte) {
		t.Helper()
		var eventID string
		var payload []byte
		awaitEvent(t, ctx, "outbox publication", func() (bool, error) {
			var published bool
			err := ordersPool.QueryRow(ctx, `SELECT event_id::text,payload,published_at IS NOT NULL FROM outbox_events WHERE order_id=$1::uuid`, orderID).Scan(&eventID, &payload, &published)
			return published, err
		})
		return eventID, payload
	}
	readNotifications := func(orderID, user string) []eventNotification {
		t.Helper()
		response, err := apiRequest(ctx, client, http.MethodGet, notificationsURL+"/v1/orders/"+orderID+"/notifications", user, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if response.status != 200 {
			t.Fatalf("notifications status=%d body=%s", response.status, response.body)
		}
		var list struct {
			Items []eventNotification `json:"items"`
		}
		if err := json.Unmarshal(response.body, &list); err != nil {
			t.Fatal(err)
		}
		return list.Items
	}
	waitNotification := func(orderID, eventID string) eventNotification {
		t.Helper()
		var notification eventNotification
		awaitEvent(t, ctx, "notification HTTP response", func() (bool, error) {
			items := readNotifications(orderID, userID)
			if len(items) == 0 {
				return false, nil
			}
			if len(items) != 1 {
				return false, fmt.Errorf("notifications count=%d", len(items))
			}
			notification = items[0]
			if notification.ID == "" || notification.EventID != eventID || notification.OrderID != orderID || notification.UserID != userID || notification.Kind != "order_created" || notification.Status != "simulated" || notification.CreatedAt.IsZero() {
				return false, fmt.Errorf("notification=%+v", notification)
			}
			return true, nil
		})
		var saved eventNotification
		err := notificationsPool.QueryRow(ctx, `SELECT id::text,event_id::text,order_id::text,user_id::text,kind,status,created_at FROM notifications WHERE event_id=$1::uuid`, eventID).Scan(&saved.ID, &saved.EventID, &saved.OrderID, &saved.UserID, &saved.Kind, &saved.Status, &saved.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		if saved.ID != notification.ID || saved.OrderID != notification.OrderID || saved.UserID != notification.UserID || saved.Kind != notification.Kind || saved.Status != notification.Status || !saved.CreatedAt.Equal(notification.CreatedAt) {
			t.Fatal("HTTP response differs from stored notification")
		}
		return notification
	}
	first := postOrder("events-first", 201)
	firstEvent, payload := waitPublished(first.ID)
	firstNotification := waitNotification(first.ID, firstEvent)
	t.Log("HTTP order -> catalog gRPC -> PostgreSQL outbox -> Kafka -> notifications PostgreSQL -> HTTP passed")
	if replay := postOrder("events-first", 200); replay.ID != first.ID {
		t.Fatal("idempotent replay created another order")
	}
	if items := readNotifications(first.ID, otherUserID); len(items) != 0 {
		t.Fatal("another user can read notification")
	}

	// Повторяем именно событие Kafka и ждём commit, прежде чем проверять дедупликацию.
	duplicate := &kgo.Record{Topic: topic, Key: []byte(first.ID), Value: payload}
	if err := kafka.ProduceSync(ctx, duplicate).FirstErr(); err != nil {
		t.Fatal(err)
	}
	awaitEvent(t, ctx, "duplicate offset commit", func() (bool, error) { return eventOffsetCommitted(ctx, kafka, group, duplicate) })
	items := readNotifications(first.ID, userID)
	if len(items) != 1 || items[0].ID != firstNotification.ID {
		t.Fatalf("duplicate changed notification: %+v", items)
	}
	var count int
	if err := notificationsPool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE event_id=$1::uuid`, firstEvent).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate rows=%d error=%v", count, err)
	}
	t.Log("repeated Kafka event committed without creating another notification")

	stopNotifications()
	second := postOrder("events-offline", 201)
	secondEvent, _ := waitPublished(second.ID)
	if err := notificationsPool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE event_id=$1::uuid`, secondEvent).Scan(&count); err != nil || count != 0 {
		t.Fatalf("offline notifications=%d error=%v", count, err)
	}
	notificationsURL, stopNotifications = startNotifications()
	defer stopNotifications()
	waitNotification(second.ID, secondEvent)
	if items := readNotifications(first.ID, userID); len(items) != 1 || items[0].ID != firstNotification.ID {
		t.Fatal("first notification changed after restart")
	}
	var orders, outbox, notifications int
	if err := ordersPool.QueryRow(ctx, `SELECT (SELECT count(*) FROM orders),(SELECT count(*) FROM outbox_events)`).Scan(&orders, &outbox); err != nil {
		t.Fatal(err)
	}
	if err := notificationsPool.QueryRow(ctx, `SELECT count(*) FROM notifications`).Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if orders != 2 || outbox != 2 || notifications != 2 {
		t.Fatalf("orders=%d outbox=%d notifications=%d", orders, outbox, notifications)
	}
	t.Log("notifications restart consumed backlog with the same group; two orders, two events, two notifications")
}

// Ожидает наблюдаемое состояние с ограниченным временем.
func awaitEvent(t *testing.T, ctx context.Context, name string, check func() (bool, error)) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := check()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", name, ctx.Err())
		case <-deadline.C:
			t.Fatalf("timeout waiting for %s", name)
		case <-tick.C:
		}
	}
}

// Проверяет сохранённый Kafka offset после обработки повторного события.
func eventOffsetCommitted(ctx context.Context, client *kgo.Client, group string, record *kgo.Record) (bool, error) {
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: record.Topic, Partitions: []int32{record.Partition}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: record.Topic, Partitions: []int32{record.Partition}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{g}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return false, err
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return false, err
	}
	for _, groupResponse := range response.Groups {
		if err := kerr.ErrorForCode(groupResponse.ErrorCode); err != nil {
			return false, err
		}
		for _, topic := range groupResponse.Topics {
			for _, partition := range topic.Partitions {
				if topic.Topic == record.Topic && partition.Partition == record.Partition {
					return partition.Offset >= record.Offset+1, kerr.ErrorForCode(partition.ErrorCode)
				}
			}
		}
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if topic.Topic == record.Topic && partition.Partition == record.Partition {
				return partition.Offset >= record.Offset+1, kerr.ErrorForCode(partition.ErrorCode)
			}
		}
	}
	return false, nil
}

// Удаляет только созданные тестом группу и топик после остановки сервисов.
func deleteEventResources(t *testing.T, client *kgo.Client, topic, group string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	groups := kmsg.NewPtrDeleteGroupsRequest()
	groups.Groups = []string{group}
	response, err := groups.RequestWith(ctx, client)
	if err != nil {
		t.Error(err)
	} else {
		for _, result := range response.Groups {
			if err := kerr.ErrorForCode(result.ErrorCode); err != nil && result.ErrorCode != kerr.GroupIDNotFound.Code {
				t.Error(err)
			}
		}
	}
	topics := kmsg.NewPtrDeleteTopicsRequest()
	topics.TimeoutMillis = 10000
	topics.TopicNames = []string{topic}
	topics.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
	deleted, err := topics.RequestWith(ctx, client)
	if err != nil {
		t.Error(err)
	} else {
		for _, result := range deleted.Topics {
			if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
				t.Error(err)
			}
		}
	}
}
