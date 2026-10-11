package config

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

// Изолирует тест от локальных переменных окружения и восстанавливает их после проверки.
func clearBalancerEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"BALANCER_HTTP_ADDR", "BALANCER_GRPC_ADDR", "LOG_LEVEL", "BALANCER_CATALOG_1_HTTP_URL", "BALANCER_CATALOG_1_GRPC_ADDR", "BALANCER_CATALOG_2_HTTP_URL", "BALANCER_CATALOG_2_GRPC_ADDR"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

// Проверяет локальные адреса и фиксированные имена двух экземпляров.
func TestLoadBalancerDefaults(t *testing.T) {
	clearBalancerEnvironment(t)
	cfg, err := LoadBalancer()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8084" || cfg.GRPCAddr != ":9084" || cfg.LogLevel != slog.LevelInfo || len(cfg.Backends) != 2 {
		t.Fatalf("config=%+v", cfg)
	}
	if cfg.Backends[0] != (BackendConfig{Name: "catalog-1", HTTPURL: "http://127.0.0.1:8082", GRPCAddr: "127.0.0.1:9082"}) || cfg.Backends[1] != (BackendConfig{Name: "catalog-2", HTTPURL: "http://127.0.0.1:8085", GRPCAddr: "127.0.0.1:9085"}) {
		t.Fatalf("backends=%+v", cfg.Backends)
	}
}

// Настройки окружения заменяют адреса и нормализуют уровень логирования.
func TestLoadBalancerOverrides(t *testing.T) {
	clearBalancerEnvironment(t)
	values := map[string]string{"BALANCER_HTTP_ADDR": " [::1]:18084 ", "BALANCER_GRPC_ADDR": "127.0.0.1:19084", "LOG_LEVEL": " DEBUG ", "BALANCER_CATALOG_1_HTTP_URL": " http://catalog-1:8082/ ", "BALANCER_CATALOG_1_GRPC_ADDR": " catalog-1:9082 ", "BALANCER_CATALOG_2_HTTP_URL": "http://catalog-2:8082", "BALANCER_CATALOG_2_GRPC_ADDR": "catalog-2:9082"}
	for k, v := range values {
		t.Setenv(k, v)
	}
	cfg, err := LoadBalancer()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "[::1]:18084" || cfg.GRPCAddr != "127.0.0.1:19084" || cfg.LogLevel != slog.LevelDebug || cfg.Backends[0].HTTPURL != "http://catalog-1:8082/" || cfg.Backends[1].GRPCAddr != "catalog-2:9082" {
		t.Fatalf("config=%+v", cfg)
	}
}

// Ошибки окружения называют проблемный параметр без раскрытия его значения.
func TestLoadBalancerInvalid(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"BALANCER_HTTP_ADDR", ""}, {"BALANCER_HTTP_ADDR", "host"}, {"BALANCER_HTTP_ADDR", ":0"}, {"BALANCER_HTTP_ADDR", ":65536"}, {"BALANCER_HTTP_ADDR", ":+12"}, {"BALANCER_HTTP_ADDR", "bad host:12"},
		{"BALANCER_GRPC_ADDR", ":bad"}, {"LOG_LEVEL", "secret-invalid"}, {"BALANCER_CATALOG_1_HTTP_URL", " "}, {"BALANCER_CATALOG_2_GRPC_ADDR", ""},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			clearBalancerEnvironment(t)
			t.Setenv(tc.key, tc.value)
			_, err := LoadBalancer()
			if err == nil || !strings.Contains(err.Error(), tc.key) || strings.Contains(err.Error(), "secret-invalid") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
