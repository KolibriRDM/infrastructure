package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
)

// Хранит постоянное имя и адреса одного экземпляра каталога.
type BackendConfig struct {
	Name     string
	HTTPURL  string
	GRPCAddr string
}

// Описывает слушающие порты, логирование и два экземпляра каталога.
type BalancerConfig struct {
	HTTPAddr string
	GRPCAddr string
	LogLevel slog.Level
	Backends []BackendConfig
}

// Читает настройки процесса; адреса каталогов проверяет NewBackend при сборке.
func LoadBalancer() (BalancerConfig, error) {
	var cfg BalancerConfig
	var err error
	cfg.HTTPAddr, err = listenAddr("BALANCER_HTTP_ADDR", ":8084")
	if err != nil {
		return BalancerConfig{}, err
	}
	cfg.GRPCAddr, err = listenAddr("BALANCER_GRPC_ADDR", ":9084")
	if err != nil {
		return BalancerConfig{}, err
	}
	level, err := setting("LOG_LEVEL", "info")
	if err != nil {
		return BalancerConfig{}, err
	}
	switch strings.ToLower(level) {
	case "debug":
		cfg.LogLevel = slog.LevelDebug
	case "info":
		cfg.LogLevel = slog.LevelInfo
	case "warn":
		cfg.LogLevel = slog.LevelWarn
	case "error":
		cfg.LogLevel = slog.LevelError
	default:
		return BalancerConfig{}, fmt.Errorf("LOG_LEVEL: expected debug, info, warn or error")
	}
	for i, ports := range [][2]string{{"8082", "9082"}, {"8085", "9085"}} {
		httpURL, err := setting(fmt.Sprintf("BALANCER_CATALOG_%d_HTTP_URL", i+1), "http://127.0.0.1:"+ports[0])
		if err != nil {
			return BalancerConfig{}, err
		}
		grpcAddr, err := setting(fmt.Sprintf("BALANCER_CATALOG_%d_GRPC_ADDR", i+1), "127.0.0.1:"+ports[1])
		if err != nil {
			return BalancerConfig{}, err
		}
		cfg.Backends = append(cfg.Backends, BackendConfig{Name: fmt.Sprintf("catalog-%d", i+1), HTTPURL: httpURL, GRPCAddr: grpcAddr})
	}
	return cfg, nil
}

// Читает непустое значение окружения или использует значение по умолчанию.
func setting(key, fallback string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", key)
	}
	return value, nil
}

// Проверяет формат слушающего адреса и диапазон TCP-порта.
func listenAddr(key, fallback string) (string, error) {
	value, err := setting(key, fallback)
	if err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || strings.ContainsAny(host, "/?#@\\ \t\r\n") {
		return "", fmt.Errorf("%s: expected host:port", key)
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("%s: port must be an integer from 1 to 65535", key)
		}
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", fmt.Errorf("%s: port must be an integer from 1 to 65535", key)
	}
	return value, nil
}
