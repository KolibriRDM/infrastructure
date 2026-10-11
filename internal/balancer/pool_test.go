package balancer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// Создаёт пул с реальными настройками без сетевых подключений.
func testPool(t *testing.T, count int) (*Pool, []*Backend) {
	t.Helper()
	backends := make([]*Backend, count)
	for i := range backends {
		name := fmt.Sprintf("catalog-%d", i+1)
		b, err := NewBackend(name, "http://"+name+":8082", name+":9082")
		if err != nil {
			t.Fatal(err)
		}
		backends[i] = b
	}
	p, err := NewPool(backends)
	if err != nil {
		t.Fatal(err)
	}
	return p, backends
}

// Отклоняет пустой список, отсутствующие экземпляры и повторные имена.
func TestNewPoolInvalidConfiguration(t *testing.T) {
	_, b := testPool(t, 2)
	duplicate, err := NewBackend(b[0].Name(), "http://other:8082", "other:9082")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		backends []*Backend
	}{
		{"nil", nil}, {"empty", []*Backend{}},
		{"nil backend", []*Backend{b[0], nil}},
		{"unnamed backend", []*Backend{new(Backend)}},
		{"same pointer", []*Backend{b[0], b[0]}},
		{"same name", []*Backend{b[0], duplicate}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPool(tc.backends)
			if p != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("pool=%v error=%v", p, err)
			}
		})
	}
}

// Изменение исходного списка не меняет пул, но доступность экземпляра остаётся общей.
func TestNewPoolCopiesSlice(t *testing.T) {
	p, input := testPool(t, 2)
	first := input[0]
	input[0] = nil
	input[1] = first
	first.SetHTTPHealthy(true)
	got, err := p.NextHTTP()
	if got != first || err != nil {
		t.Fatalf("backend=%v error=%v", got, err)
	}
	if p.backends[1] == first {
		t.Fatal("pool shares caller's slice")
	}
}

// Доступность одного протокола не изменяет состояние другого.
func TestBackendHealth(t *testing.T) {
	_, backends := testPool(t, 1)
	b := backends[0]
	for _, state := range [][2]bool{{false, false}, {true, false}, {true, true}, {false, true}, {false, false}} {
		b.SetHTTPHealthy(state[0])
		b.SetGRPCHealthy(state[1])
		if b.HTTPHealthy() != state[0] || b.GRPCHealthy() != state[1] {
			t.Fatalf("unexpected health for %v", state)
		}
	}
}

// HTTP и gRPC независимо обходят все готовые экземпляры по кругу.
func TestPoolRoundRobin(t *testing.T) {
	for _, count := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			p, backends := testPool(t, count)
			for _, b := range backends {
				b.SetHTTPHealthy(true)
				b.SetGRPCHealthy(true)
			}
			for i := 0; i < count*3+1; i++ {
				got, err := p.NextHTTP()
				if got != backends[i%count] || err != nil {
					t.Fatalf("HTTP call %d: backend=%v error=%v", i, got, err)
				}
			}
			for i := 0; i < count*3; i++ {
				got, err := p.NextGRPC()
				if got != backends[i%count] || err != nil {
					t.Fatalf("gRPC call %d: backend=%v error=%v", i, got, err)
				}
			}
		})
	}
}

// Недоступные экземпляры пропускаются и возвращаются после восстановления.
func TestPoolUnavailableAndRecovery(t *testing.T) {
	p, b := testPool(t, 2)
	for _, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
		if got, err := selectBackend(); got != nil || !errors.Is(err, ErrNoHealthyBackends) {
			t.Fatalf("initial backend=%v error=%v", got, err)
		}
	}
	b[1].SetHTTPHealthy(true)
	b[0].SetGRPCHealthy(true)
	for i := 0; i < 6; i++ {
		if got, err := p.NextHTTP(); got != b[1] || err != nil {
			t.Fatalf("HTTP backend=%v error=%v", got, err)
		}
		if got, err := p.NextGRPC(); got != b[0] || err != nil {
			t.Fatalf("gRPC backend=%v error=%v", got, err)
		}
	}
	b[0].SetHTTPHealthy(true)
	b[1].SetGRPCHealthy(true)
	for name, selectBackend := range map[string]func() (*Backend, error){"HTTP": p.NextHTTP, "gRPC": p.NextGRPC} {
		seen := map[*Backend]bool{}
		for i := 0; i < 2; i++ {
			got, err := selectBackend()
			if err != nil {
				t.Fatal(err)
			}
			seen[got] = true
		}
		if !seen[b[0]] || !seen[b[1]] {
			t.Fatalf("%s did not restore both backends", name)
		}
	}
	for _, backend := range b {
		backend.SetHTTPHealthy(false)
		backend.SetGRPCHealthy(false)
	}
	for _, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
		if got, err := selectBackend(); got != nil || !errors.Is(err, ErrNoHealthyBackends) {
			t.Fatalf("down backend=%v error=%v", got, err)
		}
	}
}

// Пустой пул возвращает ошибку без деления на ноль.
func TestPoolZeroValue(t *testing.T) {
	var p Pool
	for _, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
		if got, err := selectBackend(); got != nil || !errors.Is(err, ErrNoHealthyBackends) {
			t.Fatalf("backend=%v error=%v", got, err)
		}
	}
}

// Переполнение счётчика не нарушает выбор двух экземпляров.
func TestPoolCounterOverflow(t *testing.T) {
	p, b := testPool(t, 2)
	for _, backend := range b {
		backend.SetHTTPHealthy(true)
		backend.SetGRPCHealthy(true)
	}
	p.nextHTTP.Store(^uint64(0) - 1)
	p.nextGRPC.Store(^uint64(0) - 1)
	for _, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
		for i := 0; i < 4; i++ {
			got, err := selectBackend()
			if got != b[i%2] || err != nil {
				t.Fatalf("call %d: backend=%v error=%v", i, got, err)
			}
		}
	}
}

// Параллельные запросы равномерно распределяются отдельно для каждого протокола.
func TestPoolConcurrentSelection(t *testing.T) {
	p, b := testPool(t, 2)
	for _, backend := range b {
		backend.SetHTTPHealthy(true)
		backend.SetGRPCHealthy(true)
	}
	var counts [2][2]atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				for protocol, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
					got, err := selectBackend()
					if err != nil {
						t.Error(err)
						return
					}
					switch got {
					case b[0]:
						counts[protocol][0].Add(1)
					case b[1]:
						counts[protocol][1].Add(1)
					default:
						t.Error("unknown backend")
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for protocol := range counts {
		for backend := range counts[protocol] {
			if got := counts[protocol][backend].Load(); got != 1600 {
				t.Errorf("protocol=%d backend=%d count=%d, want 1600", protocol, backend, got)
			}
		}
	}
}

// Обновление доступности одновременно с выбором не вызывает гонок и зависаний.
func TestPoolConcurrentHealthChanges(t *testing.T) {
	p, b := testPool(t, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				for _, selectBackend := range []func() (*Backend, error){p.NextHTTP, p.NextGRPC} {
					got, err := selectBackend()
					if err != nil {
						if got != nil || !errors.Is(err, ErrNoHealthyBackends) {
							t.Errorf("backend=%v error=%v", got, err)
						}
					} else if got != b[0] && got != b[1] {
						t.Error("unknown backend")
					}
					// Доступность может измениться сразу после выбора экземпляра.
				}
			}
		}()
	}
	for _, backend := range b {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				backend.SetHTTPHealthy(i%2 == 0)
				backend.SetGRPCHealthy(i%2 != 0)
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, backend := range b {
		backend.SetHTTPHealthy(true)
		backend.SetGRPCHealthy(true)
	}
	if _, err := p.NextHTTP(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.NextGRPC(); err != nil {
		t.Fatal(err)
	}
}
