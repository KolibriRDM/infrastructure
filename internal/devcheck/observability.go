package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// checkLogs находит в Elasticsearch свежую запись по уникальному request ID.
func (r *runner) checkLogs(ctx context.Context) error {
	id, err := randomID()
	if err != nil {
		return err
	}
	_, status, headers, err := r.request(ctx, "GET", r.orders+"/stage8-log-probe-"+id, nil, nil)
	if err != nil {
		return err
	}
	requestID := headers.Get("X-Request-ID")
	if status != 404 || requestID == "" {
		return fmt.Errorf("log probe: expected 404 and request ID, got %d", status)
	}
	query := map[string]any{"size": 1, "query": map[string]any{"term": map[string]string{"app.request_id.keyword": requestID}}}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		var result struct {
			Hits struct {
				Hits []struct {
					Index  string `json:"_index"`
					Source struct {
						Service struct {
							Name string `json:"name"`
						} `json:"service"`
						App struct {
							Status int `json:"status"`
						} `json:"app"`
					} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err = r.getJSON(ctx, "POST", r.elastic+"/food-delivery-logs*/_search", query, nil, &result); err != nil {
			return err
		}
		if len(result.Hits.Hits) > 0 {
			hit := result.Hits.Hits[0]
			if hit.Source.Service.Name != "orders-service" || hit.Source.App.Status != 404 {
				return errors.New("unexpected log service or status")
			}
			return printJSON(map[string]any{"request_id": requestID, "index": hit.Index, "service": hit.Source.Service.Name, "status": hit.Source.App.Status})
		}
		if err = wait(ctx, time.Second); err != nil {
			return fmt.Errorf("new log entry did not reach Elasticsearch: %w", err)
		}
	}
}

// page читает ограниченный ответ GUI и проверяет HTTP-статус.
func (r *runner) page(ctx context.Context, url string) ([]byte, error) {
	data, status, _, err := r.request(ctx, "GET", url, nil, map[string]string{"Accept": "*/*"})
	if err == nil && status != 200 {
		err = fmt.Errorf("%s: HTTP %d", url, status)
	}
	return data, err
}

// checkGUIs проверяет HTML, JavaScript и реальные данные Kafka UI и Redis Insight.
func (r *runner) checkGUIs(ctx context.Context) error {
	pages := map[string]any{}
	scriptsRE := regexp.MustCompile(`<script[^>]+src=["']([^"']+)`)
	for _, gui := range []struct{ base, title string }{{r.kafkaUI, "Kafbat UI"}, {r.redisUI, "Redis Insight"}} {
		html, err := r.page(ctx, gui.base)
		if err != nil {
			return err
		}
		if !strings.Contains(string(html), gui.title) {
			return fmt.Errorf("missing title %s", gui.title)
		}
		scripts := scriptsRE.FindAllSubmatch(html, -1)
		if len(scripts) == 0 {
			return fmt.Errorf("no JavaScript files: %s", gui.title)
		}
		base, err := url.Parse(gui.base)
		if err != nil {
			return err
		}
		for _, script := range scripts {
			source, err := base.Parse(string(script[1]))
			if err != nil {
				return err
			}
			if source.Host != base.Host {
				return errors.New("GUI script must use the local origin")
			}
			data, err := r.page(ctx, source.String())
			if err != nil {
				return err
			}
			if len(data) == 0 {
				return errors.New("empty JavaScript asset")
			}
		}
		pages[gui.title] = map[string]any{"http_status": 200, "javascript_files": len(scripts)}
	}
	if _, err := r.page(ctx, r.balancer+menuPath); err != nil {
		return err
	}
	var topics struct {
		Topics []struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitionCount"`
			Messages   int64  `json:"messagesCount"`
		} `json:"topics"`
	}
	if err := r.getJSON(ctx, "GET", r.kafkaUI+"/api/clusters/food-delivery/topics", nil, nil, &topics); err != nil {
		return err
	}
	kafka := map[string]any{}
	for _, topic := range topics.Topics {
		if topic.Name == "food.order-created.v1" {
			if topic.Partitions != 3 {
				return fmt.Errorf("expected 3 partitions, got %d", topic.Partitions)
			}
			kafka = map[string]any{"topic": topic.Name, "partitions": topic.Partitions, "messages": topic.Messages}
		}
	}
	if len(kafka) == 0 {
		return errors.New("order topic is missing")
	}
	var databases []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := r.getJSON(ctx, "GET", r.redisUI+"/api/databases", nil, nil, &databases); err != nil {
		return err
	}
	for _, db := range databases {
		if db.Name != "food-delivery" {
			continue
		}
		var info struct {
			Keys    int64  `json:"totalKeys"`
			Version string `json:"version"`
		}
		if err := r.getJSON(ctx, "GET", r.redisUI+"/api/databases/"+url.PathEscape(db.ID)+"/info", nil, nil, &info); err != nil {
			return err
		}
		if info.Keys < 1 {
			return errors.New("Redis Insight reports no keys")
		}
		return printJSON(map[string]any{
			"pages": pages,
			"kafka": kafka,
			"redis": map[string]any{"connection": db.Name, "version": info.Version, "keys": info.Keys},
			"scope": "HTTP, JS assets and live GUI APIs; no automated visual assertion",
		})
	}
	return errors.New("food-delivery connection is missing in Redis Insight")
}

// checkRedis проверяет временный ключ и доступность Redis Insight.
func (r *runner) checkRedis(ctx context.Context) (err error) {
	id, err := randomID()
	if err != nil {
		return err
	}
	key := "food:infra-check:" + id
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, e := redis(cleanup, "DEL", key)
		err = errors.Join(err, e)
	}()
	for _, test := range []struct {
		args []string
		want string
	}{{[]string{"PING"}, "PONG"}, {[]string{"SET", key, `{"check":"food-delivery"}`, "EX", "60", "NX"}, "OK"}, {[]string{"GET", key}, `{"check":"food-delivery"}`}} {
		got, e := redis(ctx, test.args...)
		if e != nil {
			return e
		}
		if got != test.want {
			return fmt.Errorf("Redis %s: got %q", test.args[0], got)
		}
	}
	if err = checkTTL(ctx, key, 60000); err != nil {
		return err
	}
	for _, test := range []struct{ op, want string }{{"DEL", "1"}, {"EXISTS", "0"}} {
		got, e := redis(ctx, test.op, key)
		if e != nil {
			return e
		}
		if got != test.want {
			return fmt.Errorf("Redis %s: got %q", test.op, got)
		}
	}
	if _, err = r.page(ctx, r.redisUI+"/api/health/"); err != nil {
		return err
	}
	html, err := r.page(ctx, r.redisUI+"/")
	if err != nil {
		return err
	}
	if !strings.Contains(strings.ToLower(string(html)), "<html") {
		return errors.New("Redis Insight HTML is missing")
	}
	fmt.Println("PASS: Redis PING/SET/GET/TTL/DEL and Redis Insight health/UI")
	return nil
}

// decodeMenu проверяет структуру ответа каталога для нагрузочного сценария.
func decodeMenu(data []byte) error {
	var items []menuItem
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	if len(items) == 0 {
		return errors.New("empty menu")
	}
	for _, item := range items {
		if item.ID == "" {
			return errors.New("menu item ID is missing")
		}
	}
	return nil
}
