package balancer

import (
	"errors"
	"testing"
)

// Проверяет нормализацию адресов, начальное состояние и независимость копии URL.
func TestNewBackend(t *testing.T) {
	b, err := NewBackend(" catalog-1 ", " http://catalog-1:8082/ ", " catalog-1:9082 ")
	if err != nil {
		t.Fatal(err)
	}
	u := b.HTTPURL()
	if b.Name() != "catalog-1" || u.String() != "http://catalog-1:8082" || b.GRPCAddress() != "catalog-1:9082" || b.httpHealthy.Load() || b.grpcHealthy.Load() {
		t.Fatalf("unexpected backend configuration")
	}
	u.Host = "changed"
	if b.HTTPURL().Host != "catalog-1:8082" {
		t.Fatal("HTTP address changed through copy")
	}
	for _, addresses := range [][2]string{{"https://catalog", "catalog:443"}, {"http://[::1]:8082", "[::1]:9082"}} {
		if _, err := NewBackend("catalog", addresses[0], addresses[1]); err != nil {
			t.Fatal(err)
		}
	}
}

// Отклоняет неверные адреса до создания сетевых соединений.
func TestNewBackendInvalidConfiguration(t *testing.T) {
	cases := []struct{ name, http, grpc string }{
		{" ", "http://catalog:8082", "catalog:9082"},
		{"catalog", "%", "catalog:9082"},
		{"catalog", "ftp://catalog", "catalog:9082"},
		{"catalog", "http:///", "catalog:9082"},
		{"catalog", "http://user:pass@catalog", "catalog:9082"},
		{"catalog", "http://catalog?", "catalog:9082"},
		{"catalog", "http://catalog?key=value", "catalog:9082"},
		{"catalog", "http://catalog#", "catalog:9082"},
		{"catalog", "http://catalog/path", "catalog:9082"},
		{"catalog", "http://catalog/%2F", "catalog:9082"},
		{"catalog", "http://catalog:", "catalog:9082"},
		{"catalog", "http://catalog:0", "catalog:9082"},
		{"catalog", "http://catalog:65536", "catalog:9082"},
		{"catalog", "http://catalog:bad", "catalog:9082"},
		{"catalog", "http://catalog", "catalog"},
		{"catalog", "http://catalog", ":9082"},
		{"catalog", "http://catalog", "catalog:0"},
		{"catalog", "http://catalog", "catalog:65536"},
		{"catalog", "http://catalog", "catalog:+90"},
		{"catalog", "http://catalog", "http://catalog:9082"},
		{"catalog", "http://catalog", "cata log:9082"},
	}
	for _, tc := range cases {
		b, err := NewBackend(tc.name, tc.http, tc.grpc)
		if b != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("name=%q http=%q grpc=%q: backend=%v error=%v", tc.name, tc.http, tc.grpc, b, err)
		}
	}
}
