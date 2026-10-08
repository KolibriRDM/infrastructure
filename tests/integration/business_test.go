//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

// Проверяет изменение меню через HTTP с правами сотрудника и полным ответом.
func testMenuPatch(t *testing.T, ctx context.Context, client *http.Client, catalogURL string, patch map[string]any, price int64, available bool) {
	t.Helper()
	endpoint := catalogURL + "/v1/menu-items/" + pizzaID
	forbidden, err := apiRequest(ctx, client, http.MethodPatch, endpoint, "", "", patch)
	if err != nil {
		t.Fatal(err)
	}
	assertBusinessError(t, forbidden, 403, "forbidden")
	response, err := apiRequest(ctx, client, http.MethodPatch, endpoint, "", "", patch, map[string]string{"X-Demo-Role": "staff"})
	if err != nil {
		t.Fatal(err)
	}
	if response.status != 200 {
		t.Fatalf("PATCH status=%d body=%s", response.status, response.body)
	}
	var item struct {
		ID           string `json:"id"`
		RestaurantID string `json:"restaurant_id"`
		Name         string `json:"name"`
		Price        int64  `json:"price_kopecks"`
		Available    bool   `json:"is_available"`
	}
	if err := json.Unmarshal(response.body, &item); err != nil {
		t.Fatal(err)
	}
	if item.ID != pizzaID || item.RestaurantID != restaurantID || item.Name != "Маргарита" || item.Price != price || item.Available != available {
		t.Fatalf("PATCH item=%+v", item)
	}
	menu, err := apiRequest(ctx, client, http.MethodGet, catalogURL+"/v1/restaurants/"+restaurantID+"/menu", "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var items []struct {
		ID        string `json:"id"`
		Price     int64  `json:"price_kopecks"`
		Available bool   `json:"is_available"`
	}
	if menu.status != 200 {
		t.Fatalf("menu status=%d body=%s", menu.status, menu.body)
	}
	if err := json.Unmarshal(menu.body, &items); err != nil {
		t.Fatal(err)
	}
	for _, got := range items {
		if got.ID == pizzaID {
			if got.Price != price || got.Available != available {
				t.Fatalf("stale menu: %+v", got)
			}
			return
		}
	}
	t.Fatal("updated item missing from menu")
}

// Проверяет статус и код HTTP-ошибки бизнес-операции.
func assertBusinessError(t *testing.T, response apiResponse, status int, code string) {
	t.Helper()
	var body ErrorResponse
	if err := json.Unmarshal(response.body, &body); err != nil {
		t.Fatal(err)
	}
	if response.status != status || body.Code != code || body.RequestID == "" {
		t.Fatalf("status=%d body=%s want=%d/%s", response.status, response.body, status, code)
	}
}

// Проверяет права, конфликт версии, цепочку доставки и отмену через живые процессы.
func testOrderBusinessOperations(t *testing.T, ctx context.Context, client *http.Client, ordersURL string, input CreateOrderRequest) {
	t.Helper()
	staff := map[string]string{"X-Demo-Role": "staff"}
	call := func(method, path, user, key string, payload any, headers ...map[string]string) apiResponse {
		t.Helper()
		got, err := apiRequest(ctx, client, method, ordersURL+path, user, key, payload, headers...)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	decode := func(response apiResponse, status int) OrderResponse {
		t.Helper()
		if response.status != status {
			t.Fatalf("status=%d body=%s want=%d", response.status, response.body, status)
		}
		var order OrderResponse
		if err := json.Unmarshal(response.body, &order); err != nil {
			t.Fatal(err)
		}
		return order
	}
	original := decode(call(http.MethodPost, "/v1/orders", userID, "business-status", input), 201)
	path := "/v1/orders/" + original.ID
	transition := map[string]any{"expected_version": 1, "status": "cooking"}
	assertBusinessError(t, call(http.MethodPatch, path+"/status", userID, "", transition), 403, "forbidden")
	assertBusinessError(t, call(http.MethodPost, path+"/cancel", otherUserID, "", map[string]any{"expected_version": 1}), 404, "not_found")
	assertBusinessError(t, call(http.MethodPatch, path+"/status", "", "", map[string]any{"expected_version": 1, "status": "delivered"}, staff), 409, "invalid_transition")
	if got := decode(call(http.MethodGet, path, userID, "", nil), 200); !reflect.DeepEqual(got, original) {
		t.Fatal("rejected operations changed order")
	}
	// Два HTTP-запроса начинают обновление с одной ожидаемой версии.
	type result struct {
		response apiResponse
		err      error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			response, err := apiRequest(ctx, client, http.MethodPatch, ordersURL+path+"/status", "", "", transition, staff)
			results <- result{response, err}
		}()
	}
	close(start)
	successes, conflicts := 0, 0
	var current OrderResponse
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.response.status == 200 {
				successes++
				current = decode(got.response, 200)
			} else {
				assertBusinessError(t, got.response, 409, "version_conflict")
				conflicts++
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if successes != 1 || conflicts != 1 || current.Status != "cooking" || current.Version != 2 {
		t.Fatalf("successes=%d conflicts=%d order=%+v", successes, conflicts, current)
	}
	assertBusinessError(t, call(http.MethodPost, path+"/cancel", userID, "", map[string]any{"expected_version": 2}), 409, "invalid_transition")
	for _, next := range []string{"ready", "delivering", "delivered"} {
		previous := current
		current = decode(call(http.MethodPatch, path+"/status", "", "", map[string]any{"expected_version": previous.Version, "status": next}, staff), 200)
		expected := previous
		expected.Status = next
		expected.Version++
		expected.UpdatedAt = current.UpdatedAt
		if !reflect.DeepEqual(current, expected) || current.UpdatedAt.Before(previous.UpdatedAt) {
			t.Fatalf("unexpected transition: %+v", current)
		}
	}
	assertBusinessError(t, call(http.MethodPatch, path+"/status", "", "", map[string]any{"expected_version": current.Version, "status": "cooking"}, staff), 409, "invalid_transition")
	replay := decode(call(http.MethodPost, "/v1/orders", userID, "business-status", input), 200)
	if !reflect.DeepEqual(replay, current) {
		t.Fatal("replay did not return current order state")
	}
	if !reflect.DeepEqual(current.Items, original.Items) || current.TotalKopecks != original.TotalKopecks {
		t.Fatal("status changed price snapshots")
	}
	cancelledBefore := decode(call(http.MethodPost, "/v1/orders", userID, "business-cancel", input), 201)
	cancelPath := "/v1/orders/" + cancelledBefore.ID
	cancelled := decode(call(http.MethodPost, cancelPath+"/cancel", userID, "", map[string]any{"expected_version": 1}), 200)
	expected := cancelledBefore
	expected.Status = "cancelled"
	expected.Version = 2
	expected.UpdatedAt = cancelled.UpdatedAt
	if !reflect.DeepEqual(cancelled, expected) || cancelled.UpdatedAt.Before(cancelledBefore.UpdatedAt) {
		t.Fatalf("cancelled=%+v", cancelled)
	}
	assertBusinessError(t, call(http.MethodPost, cancelPath+"/cancel", userID, "", map[string]any{"expected_version": 1}), 409, "version_conflict")
	assertBusinessError(t, call(http.MethodPost, cancelPath+"/cancel", userID, "", map[string]any{"expected_version": 2}), 409, "invalid_transition")
	if got := decode(call(http.MethodGet, cancelPath, userID, "", nil), 200); !reflect.DeepEqual(got, cancelled) {
		t.Fatal("cancelled order changed after rejected repeat")
	}
}
