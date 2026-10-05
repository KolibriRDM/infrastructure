//go:build integration

package integration_test

import "time"

// Блюдо и количество из JSON-запроса клиента.
type CreateOrderItemRequest struct {
	MenuItemID string `json:"menu_item_id"`
	Quantity   int    `json:"quantity"`
}

// JSON-запрос заказа без цен и данных заголовков.
type CreateOrderRequest struct {
	RestaurantID    string                   `json:"restaurant_id"`
	DeliveryAddress string                   `json:"delivery_address"`
	Items           []CreateOrderItemRequest `json:"items"`
}

// Сохранённая позиция заказа в HTTP-ответе.
type OrderItemResponse struct {
	MenuItemID       string `json:"menu_item_id"`
	NameSnapshot     string `json:"name_snapshot"`
	UnitPriceKopecks int64  `json:"unit_price_kopecks"`
	Quantity         int    `json:"quantity"`
}

// Заказ со снимками позиций в HTTP-ответе.
type OrderResponse struct {
	ID              string              `json:"id"`
	UserID          string              `json:"user_id"`
	RestaurantID    string              `json:"restaurant_id"`
	Items           []OrderItemResponse `json:"items"`
	Status          string              `json:"status"`
	Version         int64               `json:"version"`
	TotalKopecks    int64               `json:"total_kopecks"`
	DeliveryAddress string              `json:"delivery_address"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
}

// JSON-ошибка с кодом, сообщением и request ID.
type ErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}
