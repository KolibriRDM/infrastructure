package balancer

import "errors"

var (
	// Ошибка настройки экземпляров каталога.
	ErrInvalidConfig = errors.New("invalid balancer configuration")
	// Нет доступного экземпляра для выбранного протокола.
	ErrNoHealthyBackends = errors.New("no healthy catalog backends")
)
