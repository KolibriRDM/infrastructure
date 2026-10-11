package balancer

import (
	"fmt"
	"sync/atomic"
)

// Хранит неизменяемый список экземпляров и отдельную очередь выбора для протоколов.
type Pool struct {
	backends []*Backend
	nextHTTP atomic.Uint64
	nextGRPC atomic.Uint64
}

// Проверяет состав пула и сохраняет собственную копию списка.
func NewPool(backends []*Backend) (*Pool, error) {
	if len(backends) == 0 {
		return nil, fmt.Errorf("create backend pool: backends are required: %w", ErrInvalidConfig)
	}
	seen := make(map[string]struct{}, len(backends))
	for i, backend := range backends {
		if backend == nil {
			return nil, fmt.Errorf("create backend pool: backend at index %d is nil: %w", i, ErrInvalidConfig)
		}
		name := backend.Name()
		if name == "" {
			return nil, fmt.Errorf("create backend pool: backend at index %d has no name: %w", i, ErrInvalidConfig)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("create backend pool: duplicate backend name %q: %w", name, ErrInvalidConfig)
		}
		seen[name] = struct{}{}
	}
	// Изменение исходного среза не должно менять состав пула.
	copied := make([]*Backend, len(backends))
	copy(copied, backends)
	return &Pool{
		backends: copied,
	}, nil
}

// Выбирает доступный экземпляр за один ограниченный обход.
func (p *Pool) next(counter *atomic.Uint64, isHealthy func(*Backend) bool) (*Backend, error) {
	count := uint64(len(p.backends))
	if count == 0 {
		return nil, ErrNoHealthyBackends
	}
	start := (counter.Add(1) - 1) % count
	for offset := uint64(0); offset < count; offset++ {
		// Остаток возвращает обход к началу списка.
		index := (start + offset) % count
		backend := p.backends[index]
		healthy := isHealthy(backend)
		if healthy {
			return backend, nil
		}
	}
	return nil, ErrNoHealthyBackends
}

// Выбирает доступный HTTP-экземпляр по round-robin.
func (p *Pool) NextHTTP() (*Backend, error) {
	return p.next(&p.nextHTTP, func(backend *Backend) bool {
		return backend.HTTPHealthy()
	})
}

// Выбирает доступный gRPC-экземпляр по round-robin.
func (p *Pool) NextGRPC() (*Backend, error) {
	return p.next(&p.nextGRPC, func(backend *Backend) bool {
		return backend.GRPCHealthy()
	})
}

// Проверяет наличие доступных экземпляров для обоих протоколов.
func (p *Pool) Ready() bool {
	var httpReady, grpcReady bool
	for _, backend := range p.backends {
		httpReady = httpReady || backend.HTTPHealthy()
		grpcReady = grpcReady || backend.GRPCHealthy()
		if httpReady && grpcReady {
			return true
		}
	}
	return false
}
