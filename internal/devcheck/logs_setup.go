package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const kibanaURL = "http://127.0.0.1:5601"
const logViewID = "food-delivery-logs"
const retentionPolicy = "food-delivery-logs-retention"

// jsonFile читает JSON-конфигурацию без интерпретации её как строки.
func jsonFile(path string) (json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON: %s", path)
	}
	return json.RawMessage(data), nil
}

// setupLogs настраивает ILM, шаблон индексов и воспроизводимые объекты Kibana.
func (r *runner) setupLogs(ctx context.Context) error {
	policy, err := jsonFile("deploy/compose/filebeat/retention.json")
	if err != nil {
		return err
	}
	if err = r.getJSON(ctx, "PUT", r.elastic+"/_ilm/policy/"+retentionPolicy, policy, nil, nil); err != nil {
		return err
	}
	template, err := jsonFile("deploy/compose/filebeat/template.json")
	if err != nil {
		return err
	}
	if err = r.getJSON(ctx, "PUT", r.elastic+"/_index_template/food-delivery-logs-daily", template, nil, nil); err != nil {
		return err
	}
	// Старый обычный индекс сохраняем и тоже подключаем к ограниченному хранению.
	_, status, _, err := r.request(ctx, "HEAD", r.elastic+"/food-delivery-logs", nil, nil)
	if err != nil {
		return err
	}
	if status == 200 {
		if err = r.getJSON(ctx, "PUT", r.elastic+"/food-delivery-logs/_settings", map[string]any{"index.lifecycle.name": retentionPolicy}, nil, nil); err != nil {
			return err
		}
	} else if status != 404 {
		return fmt.Errorf("legacy index: HTTP %d", status)
	}
	var streams struct {
		Streams []struct {
			Name      string `json:"name"`
			Lifecycle struct {
				Retention string `json:"data_retention"`
			} `json:"lifecycle"`
		} `json:"data_streams"`
	}
	if err = r.getJSON(ctx, "GET", r.elastic+"/_data_stream/food-delivery-logs*", nil, nil, &streams); err != nil {
		return err
	}
	for _, stream := range streams.Streams {
		if stream.Lifecycle.Retention == "7d" {
			continue
		}
		path := "/_data_stream/" + url.PathEscape(stream.Name) + "/_lifecycle"
		if err = r.getJSON(ctx, "PUT", r.elastic+path, map[string]string{"data_retention": "7d"}, nil, nil); err != nil {
			return err
		}
		// Rollover позволяет удалить старый backing index без удаления текущего write index.
		if err = r.getJSON(ctx, "POST", r.elastic+"/"+url.PathEscape(stream.Name)+"/_rollover", map[string]any{}, nil, nil); err != nil {
			return err
		}
	}
	headers := map[string]string{"kbn-xsrf": "food-delivery-setup"}
	view := map[string]any{"data_view": map[string]any{"id": logViewID, "title": "food-delivery-logs*", "name": "Food Delivery — логи", "timeFieldName": "@timestamp", "allowNoIndex": true}, "override": true}
	if err = r.getJSON(ctx, "POST", kibanaURL+"/api/data_views/data_view", view, headers, nil); err != nil {
		return err
	}
	if err = r.getJSON(ctx, "POST", kibanaURL+"/api/data_views/default", map[string]any{"data_view_id": logViewID, "force": true}, headers, nil); err != nil {
		return err
	}
	for _, search := range []struct{ id, title, query string }{{"food-delivery-all", "Food Delivery — все логи", "service.name: *"}, {"food-delivery-orders", "Food Delivery — заказы", `service.name: "orders-service"`}, {"food-delivery-events", "Food Delivery — события", "app.event_id: *"}} {
		if err = r.saveSearch(ctx, search.id, search.title, search.query); err != nil {
			return err
		}
	}
	fmt.Println("PASS: daily indices, 7-day ILM, legacy retention, Kibana data view and saved searches")
	return nil
}

// saveSearch обновляет только сохранённый поиск проекта с заранее заданным ID.
func (r *runner) saveSearch(ctx context.Context, id, title, query string) error {
	source, err := json.Marshal(map[string]any{"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index", "query": map[string]string{"language": "kuery", "query": query}, "filter": []any{}})
	if err != nil {
		return err
	}
	body := map[string]any{"attributes": map[string]any{"title": title, "description": "Создано Go-командой make logs-setup", "columns": []string{"service.name", "app.msg", "app.request_id", "app.order_id", "app.event_id"}, "sort": [][]string{{"@timestamp", "desc"}}, "kibanaSavedObjectMeta": map[string]string{"searchSourceJSON": string(source)}}, "references": []any{map[string]string{"id": logViewID, "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}}}
	return r.getJSON(ctx, "POST", kibanaURL+"/api/saved_objects/search/"+id+"?overwrite=true", body, map[string]string{"kbn-xsrf": "food-delivery-setup"}, nil)
}

// searchLogs выполняет поиск по точным структурированным полям JSON-журнала.
func (r *runner) searchLogs(ctx context.Context, field, value string) ([]json.RawMessage, error) {
	query := map[string]any{"size": 50, "query": map[string]any{"term": map[string]string{field: value}}}
	var result struct {
		Hits struct {
			Hits []json.RawMessage `json:"hits"`
		} `json:"hits"`
	}
	err := r.getJSON(ctx, "POST", r.elastic+"/food-delivery-logs*/_search", query, nil, &result)
	return result.Hits.Hits, err
}

// checkKibana проверяет корреляцию реального заказа и подключённую политику хранения.
func (r *runner) checkKibana(ctx context.Context) error {
	order, err := r.newFaultOrder(ctx)
	if err != nil {
		return err
	}
	if err = r.waitFaultDelivery(ctx, order); err != nil {
		return err
	}
	checks := map[string]int{}
	for field, value := range map[string]string{"service.name.keyword": "orders-service", "app.order_id.keyword": order.ID, "app.event_id.keyword": order.EventID} {
		if err = eventually(ctx, "logs indexed: "+field, 90*time.Second, func(ctx context.Context) (bool, error) {
			hits, e := r.searchLogs(ctx, field, value)
			checks[field] = len(hits)
			return len(hits) > 0, e
		}); err != nil {
			return err
		}
	}
	services := map[string]bool{}
	err = eventually(ctx, "event logs from both services", 90*time.Second, func(ctx context.Context) (bool, error) {
		hits, err := r.searchLogs(ctx, "app.event_id.keyword", order.EventID)
		if err != nil {
			return false, err
		}
		for _, raw := range hits {
			var hit struct {
				Index  string `json:"_index"`
				Source struct {
					Service struct {
						Name string `json:"name"`
					} `json:"service"`
				} `json:"_source"`
			}
			if err = json.Unmarshal(raw, &hit); err != nil {
				return false, err
			}
			if !strings.HasPrefix(hit.Index, "food-delivery-logs-daily-") {
				return false, fmt.Errorf("new logs use unmanaged index %s", hit.Index)
			}
			services[hit.Source.Service.Name] = true
		}
		return services["orders-service"] && services["notifications-service"], nil
	})
	if err != nil {
		return err
	}
	if err = r.saveSearch(ctx, "food-delivery-last-order", "Food Delivery — последний проверенный заказ", `app.order_id: "`+order.ID+`"`); err != nil {
		return err
	}
	if err = r.saveSearch(ctx, "food-delivery-last-event", "Food Delivery — последнее проверенное событие", `app.event_id: "`+order.EventID+`"`); err != nil {
		return err
	}
	var view struct {
		DataView struct{ ID, Title, TimeFieldName string } `json:"data_view"`
	}
	if err = r.getJSON(ctx, "GET", kibanaURL+"/api/data_views/data_view/"+logViewID, nil, nil, &view); err != nil {
		return err
	}
	if view.DataView.ID != logViewID || view.DataView.Title != "food-delivery-logs*" || view.DataView.TimeFieldName != "@timestamp" {
		return errors.New("Kibana data view mismatch")
	}
	var lifecycle struct {
		Indices map[string]struct {
			Managed bool   `json:"managed"`
			Policy  string `json:"policy"`
			Step    string `json:"step"`
		} `json:"indices"`
	}
	if err = r.getJSON(ctx, "GET", r.elastic+"/food-delivery-logs-daily-*/_ilm/explain", nil, nil, &lifecycle); err != nil {
		return err
	}
	if len(lifecycle.Indices) == 0 {
		return errors.New("no daily log indices")
	}
	for name, state := range lifecycle.Indices {
		if !state.Managed || state.Policy != retentionPolicy || state.Step == "ERROR" {
			return fmt.Errorf("unmanaged or failed index %s: %+v", name, state)
		}
	}
	result := map[string]any{"checked_at": time.Now().UTC(), "order": order, "matches": checks, "services": services, "lifecycle": lifecycle, "discover": "http://localhost:5601/app/discover#/view/food-delivery-last-order"}
	if err = saveJSON("reports/stage9/kibana.json", result); err != nil {
		return err
	}
	return printJSON(result)
}
