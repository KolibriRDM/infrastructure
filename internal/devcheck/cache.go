package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// checkTTL требует положительный срок жизни ключа в заданных пределах.
func checkTTL(ctx context.Context, key string, max int) error {
	raw, err := redis(ctx, "PTTL", key)
	if err != nil {
		return err
	}
	ttl, err := strconv.Atoi(raw)
	if err != nil {
		return err
	}
	if ttl <= 0 || ttl > max {
		return fmt.Errorf("unexpected TTL for %s: %d ms", key, ttl)
	}
	return nil
}

// menuPrice извлекает цену демонстрационного блюда из публичного меню.
func (r *runner) menuPrice(ctx context.Context) (int, error) {
	var items []menuItem
	if err := r.getJSON(ctx, "GET", r.catalog+menuPath, nil, nil, &items); err != nil {
		return 0, err
	}
	for _, item := range items {
		if item.ID == itemID {
			return item.Price, nil
		}
	}
	return 0, errors.New("demo item missing; run make up")
}

// checkCacheKey сверяет реальное содержимое кэша с ценой из API.
func checkCacheKey(ctx context.Context, price int) error {
	raw, err := redis(ctx, "GET", cacheKey)
	if err != nil {
		return err
	}
	var items []struct {
		ID           string
		PriceKopecks int
	}
	if err = json.Unmarshal([]byte(raw), &items); err != nil {
		return err
	}
	for _, item := range items {
		if item.ID == itemID && item.PriceKopecks == price {
			return checkTTL(ctx, cacheKey, 70000)
		}
	}
	return errors.New("cached price differs from HTTP response")
}

// setPrice меняет цену через staff API каталога.
func (r *runner) setPrice(ctx context.Context, price int) error {
	return r.getJSON(ctx, "PATCH", r.catalog+"/v1/menu-items/"+itemID, map[string]int{"price_kopecks": price}, map[string]string{"X-Demo-Role": "staff"}, nil)
}

// ready проверяет ответ readiness сервиса.
func (r *runner) ready(ctx context.Context, base string) error {
	var result struct {
		Status string `json:"status"`
	}
	if err := r.getJSON(ctx, "GET", base+"/readyz", nil, nil, &result); err != nil {
		return err
	}
	if result.Status != "ok" {
		return fmt.Errorf("%s is not ready", base)
	}
	return nil
}

// restartRedis восстанавливает Redis даже после отмены основной проверки.
func restartRedis() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, err := compose(ctx, "", "up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "redis")
	return err
}

// checkMenuCache проверяет заполнение, инвалидацию и работу при недоступности Redis.
func (r *runner) checkMenuCache(ctx context.Context) (err error) {
	pong, err := redis(ctx, "PING")
	if err != nil {
		return err
	}
	if pong != "PONG" {
		return errors.New("Redis is not ready")
	}
	if err = r.ready(ctx, r.catalog); err != nil {
		return err
	}
	var resolved struct {
		Items []menuItem `json:"items"`
	}
	if err = r.getJSON(ctx, "POST", r.catalog+"/internal/v1/menu/resolve", map[string]any{"restaurant_id": restaurantID, "item_ids": []string{itemID}}, nil, &resolved); err != nil {
		return err
	}
	if len(resolved.Items) != 1 {
		return errors.New("resolve returned unexpected items")
	}
	original := resolved.Items[0].Price
	changed := original + 1
	if original >= 1000000 {
		changed = original - 1
	}
	price, err := r.menuPrice(ctx)
	if err != nil {
		return err
	}
	if err = checkCacheKey(ctx, price); err != nil {
		return err
	}
	restorePrice, restoreRedis := false, false
	defer func() {
		if restoreRedis {
			err = errors.Join(err, restartRedis())
		}
		if restorePrice {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e := r.setPrice(cleanup, original)
			err = errors.Join(err, e)
			if e == nil {
				p, e := r.menuPrice(cleanup)
				err = errors.Join(err, e)
				if e == nil && p != original {
					err = errors.Join(err, errors.New("original price was not restored"))
				}
				if e == nil {
					err = errors.Join(err, checkCacheKey(cleanup, original))
				}
			}
		}
	}()
	restorePrice = true
	if err = r.setPrice(ctx, changed); err != nil {
		return err
	}
	exists, err := redis(ctx, "EXISTS", cacheKey)
	if err != nil {
		return err
	}
	if exists != "0" {
		return errors.New("PATCH did not invalidate cache")
	}
	price, err = r.menuPrice(ctx)
	if err != nil {
		return err
	}
	if price != changed {
		return errors.New("menu price did not change")
	}
	if err = checkCacheKey(ctx, changed); err != nil {
		return err
	}
	restoreRedis = true
	if _, err = compose(ctx, "", "stop", "redis"); err != nil {
		return err
	}
	if err = r.ready(ctx, r.catalog); err != nil {
		return err
	}
	price, err = r.menuPrice(ctx)
	if err != nil {
		return err
	}
	if price != changed {
		return errors.New("PostgreSQL fallback returned wrong price")
	}
	fmt.Println("OK: cache, PATCH invalidation and PostgreSQL fallback; restoring Redis and price")
	return nil
}

// orderReply содержит идентификатор заказа либо поля HTTP-ошибки.
type orderReply struct {
	ID        string `json:"id"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}

// checkRateLimit проверяет окно лимита, независимость пользователей и fail-open.
func (r *runner) checkRateLimit(ctx context.Context) (err error) {
	if err = r.ready(ctx, r.orders); err != nil {
		return err
	}
	user, err := uuid()
	if err != nil {
		return err
	}
	other, err := uuid()
	if err != nil {
		return err
	}
	key := "limiter-check-" + user
	counter := "rate_limit:orders:create:v1:" + user
	headers := map[string]string{"X-Demo-User-ID": user, "Idempotency-Key": key}
	orderID := ""
	for attempt := 1; attempt <= 30; attempt++ {
		data, status, _, e := r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), headers)
		if e != nil {
			return e
		}
		want := 200
		if attempt == 1 {
			want = 201
		}
		if status != want {
			return fmt.Errorf("attempt %d: HTTP %d: %.250s", attempt, status, data)
		}
		var order orderReply
		if e = json.Unmarshal(data, &order); e != nil {
			return e
		}
		if order.ID == "" {
			return errors.New("missing order ID")
		}
		if attempt == 1 {
			orderID = order.ID
		}
		if order.ID != orderID {
			return errors.New("replay created another order")
		}
	}
	count, err := redis(ctx, "GET", counter)
	if err != nil {
		return err
	}
	if count != "30" {
		return fmt.Errorf("expected 30 counted attempts, got %s", count)
	}
	if err = checkTTL(ctx, counter, 60000); err != nil {
		return err
	}
	before, err := redis(ctx, "PTTL", counter)
	if err != nil {
		return err
	}
	beforeTTL, _ := strconv.Atoi(before)
	data, status, h, err := r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), headers)
	if err != nil {
		return err
	}
	var rejected orderReply
	if err = json.Unmarshal(data, &rejected); err != nil {
		return err
	}
	if status != 429 || rejected.Code != "rate_limit_exceeded" {
		return fmt.Errorf("attempt 31: HTTP %d: %.250s", status, data)
	}
	retry, e := strconv.Atoi(h.Get("Retry-After"))
	if e != nil || retry < 1 || retry > 60 {
		return errors.New("invalid Retry-After")
	}
	if rejected.RequestID == "" || rejected.RequestID != h.Get("X-Request-ID") {
		return errors.New("missing or inconsistent request ID")
	}
	count, err = redis(ctx, "GET", counter)
	if err != nil {
		return err
	}
	if count != "31" {
		return errors.New("rejected attempt was not counted")
	}
	if err = checkTTL(ctx, counter, beforeTTL); err != nil {
		return err
	}
	var independent orderReply
	data, status, _, err = r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), map[string]string{"X-Demo-User-ID": other, "Idempotency-Key": key})
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, &independent); err != nil {
		return err
	}
	if status != 201 || independent.ID == "" || independent.ID == orderID {
		return errors.New("independent user was limited")
	}
	if err = r.getJSON(ctx, "GET", r.orders+"/v1/orders/"+orderID, nil, headers, nil); err != nil {
		return err
	}
	if err = r.ready(ctx, r.orders); err != nil {
		return err
	}
	restore := true
	defer func() {
		if restore {
			err = errors.Join(err, restartRedis())
		}
	}()
	if _, err = compose(ctx, "", "stop", "redis"); err != nil {
		return err
	}
	if err = r.ready(ctx, r.orders); err != nil {
		return err
	}
	// Повтор того же ключа проверяет fail-open без создания лишнего заказа.
	replay := func() error {
		data, status, _, e := r.request(ctx, "POST", r.orders+"/v1/orders", orderPayload(), headers)
		if e != nil {
			return e
		}
		var order orderReply
		if e = json.Unmarshal(data, &order); e != nil {
			return e
		}
		if status != 200 || order.ID != orderID {
			return fmt.Errorf("replay: HTTP %d, order %s", status, order.ID)
		}
		return nil
	}
	if err = replay(); err != nil {
		return err
	}
	if err = restartRedis(); err != nil {
		return err
	}
	restore = false
	if err = replay(); err != nil {
		return err
	}
	count, err = redis(ctx, "GET", counter)
	if err != nil {
		return err
	}
	if count != "1" {
		return fmt.Errorf("limiter did not recover: %s", count)
	}
	if err = checkTTL(ctx, counter, 60000); err != nil {
		return err
	}
	fmt.Println("PASS: limiter, Retry-After, independent users, fail-open and recovery")
	fmt.Println("Redis Insight key:", counter)
	return nil
}
