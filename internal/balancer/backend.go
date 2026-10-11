package balancer

import (
	"net/url"
	"sync/atomic"
)

// Хранит адреса экземпляра каталога и доступность каждого протокола.
type Backend struct {
	name        string
	httpURL     url.URL
	grpcAddress string
	httpHealthy atomic.Bool
	grpcHealthy atomic.Bool
}

// Возвращает постоянное имя экземпляра.
func (b *Backend) Name() string { return b.name }

// Возвращает копию HTTP-адреса без изменения исходной настройки.
func (b *Backend) HTTPURL() url.URL { return b.httpURL }

// Возвращает gRPC-адрес в формате host:port.
func (b *Backend) GRPCAddress() string { return b.grpcAddress }

// Возвращает доступность HTTP экземпляра.
func (b *Backend) HTTPHealthy() bool { return b.httpHealthy.Load() }

// Возвращает доступность gRPC экземпляра.
func (b *Backend) GRPCHealthy() bool { return b.grpcHealthy.Load() }

// Обновляет доступность HTTP экземпляра.
func (b *Backend) SetHTTPHealthy(healthy bool) { b.httpHealthy.Store(healthy) }

// Обновляет доступность gRPC экземпляра.
func (b *Backend) SetGRPCHealthy(healthy bool) { b.grpcHealthy.Store(healthy) }
