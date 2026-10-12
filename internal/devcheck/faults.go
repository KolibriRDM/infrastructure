package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// faultOrder сохраняет идентификаторы запроса для повторов при отказах.
type faultOrder struct {
	ID      string `json:"order_id"`
	User    string `json:"user_id"`
	Key     string `json:"idempotency_key"`
	EventID string `json:"event_id"`
}

// newFaultOrder создаёт уникальный заказ на рабочем учебном стенде.
func (r *runner) newFaultOrder(ctx context.Context) (faultOrder, error) {
	user, err := uuid()
	if err != nil {
		return faultOrder{}, err
	}
	id, err := randomID()
	if err != nil {
		return faultOrder{}, err
	}
	order := faultOrder{User: user, Key: "stage9-" + id}
	data, status, _, err := r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), map[string]string{"X-Demo-User-ID": user, "Idempotency-Key": order.Key})
	if err != nil {
		return order, err
	}
	if status != 201 {
		return order, fmt.Errorf("create fault order: HTTP %d: %.250s", status, data)
	}
	var reply orderReply
	if err = json.Unmarshal(data, &reply); err != nil {
		return order, err
	}
	if reply.ID == "" {
		return order, errors.New("missing order ID")
	}
	order.ID = reply.ID
	raw, err := sql(ctx, "orders_db", fmt.Sprintf("SELECT event_id::text FROM outbox_events WHERE order_id='%s'::uuid;", order.ID))
	order.EventID = raw
	return order, err
}

// eventually повторяет наблюдение до успеха, сохраняя последнюю причину ожидания.
func eventually(ctx context.Context, name string, timeout time.Duration, check func(ctx context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	for {
		ok, err := check(ctx)
		if ok && err == nil {
			return nil
		}
		last = err
		if err = wait(ctx, 250*time.Millisecond); err != nil {
			return fmt.Errorf("%s: %w; last check: %v", name, err, last)
		}
	}
}

// waitFaultDelivery сверяет публикацию outbox и единственное уведомление через HTTP.
func (r *runner) waitFaultDelivery(ctx context.Context, order faultOrder) error {
	return eventually(ctx, "order delivery", 90*time.Second, func(ctx context.Context) (bool, error) {
		published, err := sql(ctx, "orders_db", fmt.Sprintf("SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id='%s'::uuid;", order.EventID))
		if err != nil || published != "t" {
			return false, err
		}
		var list notificationList
		err = r.getJSON(ctx, "GET", r.notifications+"/v1/orders/"+order.ID+"/notifications", nil, map[string]string{"X-Demo-User-ID": order.User}, &list)
		if err != nil {
			return false, err
		}
		if len(list.Items) == 0 {
			return false, nil
		}
		if len(list.Items) != 1 || list.Items[0].EventID != order.EventID || list.Items[0].UserID != order.User {
			return false, errors.New("notification mismatch or duplicate")
		}
		return true, nil
	})
}

// startServices восстанавливает только указанные существующие контейнеры.
func startServices(services ...string) error {
	timeout, waitSeconds := 90*time.Second, "60"
	for _, name := range services {
		if name == "elasticsearch" || name == "kibana" {
			timeout, waitSeconds = 420*time.Second, "360"
		}
	}
	args := []string{"compose", "--env-file", ".env", "-f", "deploy/compose/compose.yaml", "up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "--wait-timeout", waitSeconds}
	_, err := commandWithTimeout(context.Background(), timeout, "", "docker", append(args, services...)...)
	return err
}

// kafkaPosition возвращает commit группы или конец каждой партиции DLQ.
func kafkaPosition(ctx context.Context, client *kgo.Client, committed bool) (string, error) {
	values := make([]string, 3)
	if committed {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "notifications-v1"
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "food.order-created.v1", Partitions: []int32{0, 1, 2}}}
		req.Groups = []kmsg.OffsetFetchRequestGroup{{Group: req.Group, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "food.order-created.v1", Partitions: []int32{0, 1, 2}}}}}
		res, err := req.RequestWith(ctx, client)
		if err != nil {
			return "", err
		}
		if err = kerr.ErrorForCode(res.ErrorCode); err != nil {
			return "", err
		}
		for _, topic := range res.Topics {
			for _, p := range topic.Partitions {
				if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
					return "", err
				}
				if p.Partition >= 0 && p.Partition < 3 {
					values[p.Partition] = strconv.FormatInt(p.Offset, 10)
				}
			}
		}
		for _, group := range res.Groups {
			if err = kerr.ErrorForCode(group.ErrorCode); err != nil {
				return "", err
			}
			for _, topic := range group.Topics {
				for _, p := range topic.Partitions {
					if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
						return "", err
					}
					if p.Partition >= 0 && p.Partition < 3 {
						values[p.Partition] = strconv.FormatInt(p.Offset, 10)
					}
				}
			}
		}
	} else {
		req := kmsg.NewPtrListOffsetsRequest()
		req.ReplicaID = -1
		topic := kmsg.ListOffsetsRequestTopic{Topic: "food.order-created.dlq.v1"}
		for p := int32(0); p < 3; p++ {
			part := kmsg.NewListOffsetsRequestTopicPartition()
			part.Partition = p
			part.Timestamp = -1
			topic.Partitions = append(topic.Partitions, part)
		}
		req.Topics = []kmsg.ListOffsetsRequestTopic{topic}
		res, err := req.RequestWith(ctx, client)
		if err != nil {
			return "", err
		}
		for _, topic := range res.Topics {
			for _, p := range topic.Partitions {
				if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
					return "", err
				}
				if p.Partition >= 0 && p.Partition < 3 {
					values[p.Partition] = strconv.FormatInt(p.Offset, 10)
				}
			}
		}
	}
	for _, value := range values {
		if value == "" {
			return "", errors.New("missing Kafka partition offset")
		}
	}
	return strings.Join(values, ","), nil
}

// checkFaults проверяет реальные отказы и восстанавливает стенд через defer.
func (r *runner) checkFaults(ctx context.Context) (err error) {
	for _, base := range []string{r.orders, r.notifications, r.balancer} {
		if err = r.ready(ctx, base); err != nil {
			return err
		}
	}
	results := []string{}
	orders := []faultOrder{}
	var notificationsLimit string
	defer func() {
		if notificationsLimit != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, e := sql(cleanup, "postgres", "ALTER DATABASE notifications_db CONNECTION LIMIT "+notificationsLimit+";")
			cancel()
			err = errors.Join(err, e)
		}
		err = errors.Join(err, startServices("kafka", "redis", "catalog", "catalog-2", "balancer", "orders", "notifications", "prometheus", "grafana", "elasticsearch", "kibana", "filebeat"))
		result := map[string]any{"checked_at": time.Now().UTC(), "checks": results, "orders": orders, "passed": err == nil}
		if err != nil {
			result["error"] = err.Error()
		}
		err = errors.Join(err, saveJSON("reports/stage9/faults.json", result), printJSON(result))
	}()
	baseline, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	orders = append(orders, baseline)
	if err = r.waitFaultDelivery(ctx, baseline); err != nil {
		return err
	}
	// При отключённой Kafka HTTP сохраняет заказ вместе с pending outbox.
	if _, err = compose(ctx, "", "stop", "kafka"); err != nil {
		return err
	}
	for range 2 {
		order, e := r.newFaultOrder(ctx)
		if e != nil {
			return e
		}
		orders = append(orders, order)
	}
	if err = wait(ctx, 6*time.Second); err != nil {
		return err
	}
	for _, order := range orders[1:] {
		pending, e := sql(ctx, "orders_db", fmt.Sprintf("SELECT published_at IS NULL FROM outbox_events WHERE event_id='%s'::uuid;", order.EventID))
		if e != nil {
			return e
		}
		if pending != "t" {
			return errors.New("Kafka outage did not retain pending event")
		}
	}
	if err = startServices("kafka"); err != nil {
		return err
	}
	for _, order := range orders[1:] {
		if err = r.waitFaultDelivery(ctx, order); err != nil {
			return err
		}
	}
	results = append(results, "Kafka outage: HTTP 201, pending outbox, complete recovery")
	fmt.Println("PASS:", results[len(results)-1])
	// Запрещаем новые подключения только к notifications_db и закрываем старые.
	notificationsLimit, err = sql(ctx, "postgres", "SELECT datconnlimit FROM pg_database WHERE datname='notifications_db';")
	if err != nil {
		return err
	}
	if _, err = strconv.Atoi(notificationsLimit); err != nil {
		return err
	}
	if _, err = sql(ctx, "postgres", "ALTER DATABASE notifications_db CONNECTION LIMIT 0; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='notifications_db';"); err != nil {
		return err
	}
	if err = eventually(ctx, "notifications DB unavailable", 10*time.Second, func(ctx context.Context) (bool, error) {
		_, status, _, e := r.request(ctx, "GET", r.notifications+"/readyz", nil, nil)
		return status == 503, e
	}); err != nil {
		return err
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"), kgo.DialTimeout(2*time.Second))
	if err != nil {
		return err
	}
	defer kafka.Close()
	offsets, err := kafkaPosition(ctx, kafka, true)
	if err != nil {
		return err
	}
	dlq, err := kafkaPosition(ctx, kafka, false)
	if err != nil {
		return err
	}
	dbOrder, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	orders = append(orders, dbOrder)
	if err = eventually(ctx, "outbox published while notifications DB is down", 20*time.Second, func(ctx context.Context) (bool, error) {
		raw, e := sql(ctx, "orders_db", fmt.Sprintf("SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id='%s'::uuid;", dbOrder.EventID))
		return raw == "t", e
	}); err != nil {
		return err
	}
	if err = wait(ctx, 6*time.Second); err != nil {
		return err
	}
	after, err := kafkaPosition(ctx, kafka, true)
	if err != nil {
		return err
	}
	afterDLQ, err := kafkaPosition(ctx, kafka, false)
	if err != nil {
		return err
	}
	if after != offsets || afterDLQ != dlq {
		return fmt.Errorf("DB outage advanced offsets/DLQ: %s -> %s; %s -> %s", offsets, after, dlq, afterDLQ)
	}
	if _, err = sql(ctx, "postgres", "ALTER DATABASE notifications_db CONNECTION LIMIT "+notificationsLimit+";"); err != nil {
		return err
	}
	notificationsLimit = ""
	if err = r.waitFaultDelivery(ctx, dbOrder); err != nil {
		return err
	}
	results = append(results, "notifications DB outage: no commit, no DLQ, exactly one notification after recovery")
	fmt.Println("PASS:", results[len(results)-1])
	// Сохранённый заказ и идемпотентный повтор не зависят от доступности каталога.
	if _, err = compose(ctx, "", "stop", "catalog", "catalog-2"); err != nil {
		return err
	}
	if err = eventually(ctx, "catalogs unavailable", 10*time.Second, func(ctx context.Context) (bool, error) {
		_, status, _, e := r.request(ctx, "GET", r.balancer+"/readyz", nil, nil)
		return status == 503, e
	}); err != nil {
		return err
	}
	headers := map[string]string{"X-Demo-User-ID": baseline.User, "Idempotency-Key": baseline.Key}
	if err = r.getJSON(ctx, "GET", r.orders+"/v1/orders/"+baseline.ID, nil, headers, nil); err != nil {
		return err
	}
	data, status, _, err := r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), headers)
	if err != nil {
		return err
	}
	var replay orderReply
	if err = json.Unmarshal(data, &replay); err != nil {
		return err
	}
	if status != 200 || replay.ID != baseline.ID {
		return errors.New("catalog outage broke idempotent replay")
	}
	headers["Idempotency-Key"] = baseline.Key + "-unavailable"
	_, status, _, err = r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), headers)
	if err != nil {
		return err
	}
	if status != 503 {
		return fmt.Errorf("new order without catalog: HTTP %d", status)
	}
	count, err := sql(ctx, "orders_db", fmt.Sprintf("SELECT count(*) FROM idempotency_keys WHERE key='%s';", headers["Idempotency-Key"]))
	if err != nil || count != "0" {
		return fmt.Errorf("failed create left idempotency row: %s, %v", count, err)
	}
	if err = startServices("catalog", "catalog-2"); err != nil {
		return err
	}
	if err = eventually(ctx, "balancer recovered", 15*time.Second, func(ctx context.Context) (bool, error) { e := r.ready(ctx, r.balancer); return e == nil, e }); err != nil {
		return err
	}
	results = append(results, "catalog outage: new create 503 without saved key, existing order/replay available")
	fmt.Println("PASS:", results[len(results)-1])
	if err = r.checkMenuCache(ctx); err != nil {
		return err
	}
	if err = r.checkRateLimit(ctx); err != nil {
		return err
	}
	results = append(results, "Redis outage: cache fallback, limiter fail-open, key/TTL restored")
	if _, err = compose(ctx, "", "stop", "notifications"); err != nil {
		return err
	}
	offline, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	orders = append(orders, offline)
	if err = r.getJSON(ctx, "GET", r.orders+"/v1/orders/"+offline.ID, nil, map[string]string{"X-Demo-User-ID": offline.User}, nil); err != nil {
		return err
	}
	if err = startServices("notifications"); err != nil {
		return err
	}
	if err = r.waitFaultDelivery(ctx, offline); err != nil {
		return err
	}
	results = append(results, "notifications restart: orders remain available, backlog delivered")
	if _, err = compose(ctx, "", "stop", "prometheus", "grafana", "filebeat", "kibana", "elasticsearch"); err != nil {
		return err
	}
	telemetry, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	orders = append(orders, telemetry)
	if err = r.waitFaultDelivery(ctx, telemetry); err != nil {
		return err
	}
	results = append(results, "telemetry outage: order and notification succeed")
	return nil
}
