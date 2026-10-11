package balancer

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
)

// Хранит отдельный реестр и метрики запросов и доступности балансировщика.
type Metrics struct {
	pool       *Pool
	registry   *prometheus.Registry
	selections *prometheus.CounterVec
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	backends   map[*Backend]string
}

// Создаёт реестр; доступность читается из atomic-флагов при сборе метрик.
func NewMetrics(pool *Pool) *Metrics {
	m := &Metrics{pool: pool, registry: prometheus.NewRegistry(), backends: make(map[*Backend]string)}
	labels := prometheus.Labels{"service": "catalog-balancer"}
	m.selections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "food", Subsystem: "balancer", Name: "selections_total",
		Help: "Number of backend selections.", ConstLabels: labels,
	}, []string{"protocol", "backend"})
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "food", Subsystem: "balancer", Name: "requests_total",
		Help: "Completed requests by HTTP or gRPC code; aborted marks interrupted HTTP responses.", ConstLabels: labels,
	}, []string{"protocol", "backend", "code"})
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "food", Subsystem: "balancer", Name: "request_duration_seconds",
		Help: "Request handling duration in seconds.", ConstLabels: labels,
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"protocol", "backend"})
	m.registry.MustRegister(m.selections, m.requests, m.duration, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if pool != nil {
		for _, backend := range pool.backends {
			m.backends[backend] = backend.Name()
			for _, protocol := range []string{"http", "grpc"} {
				healthy := backend.HTTPHealthy
				if protocol == "grpc" {
					healthy = backend.GRPCHealthy
				}
				m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
					Namespace: "food", Subsystem: "balancer", Name: "backend_up",
					Help:        "Whether the backend is eligible for protocol selection.",
					ConstLabels: prometheus.Labels{"service": "catalog-balancer", "backend": backend.Name(), "protocol": protocol},
				}, func() float64 {
					if healthy() {
						return 1
					}
					return 0
				}))
			}
		}
	}
	return m
}

// Учитывает успешный выбор экземпляра для HTTP или gRPC.
func (m *Metrics) ObserveSelection(protocol string, backend *Backend) {
	if protocol != "http" && protocol != "grpc" {
		return
	}
	name, ok := m.backends[backend]
	if !ok {
		return
	}
	m.selections.WithLabelValues(protocol, name).Inc()
}

// Записывает завершение запроса, ограничивая набор меток.
func (m *Metrics) ObserveRequest(protocol string, backend *Backend, code string, elapsed time.Duration) {
	if protocol != "http" && protocol != "grpc" {
		return
	}
	name := m.backends[backend]
	// Пустое имя обозначает запрос, для которого экземпляр не был выбран.
	if protocol == "http" {
		if code != "aborted" {
			number, err := strconv.Atoi(code)
			if err != nil || number < 100 || number > 599 {
				code = "other"
			} else {
				code = strconv.Itoa(number)
			}
		}
	} else {
		known := false
		for candidate := codes.OK; candidate <= codes.Unauthenticated; candidate++ {
			if candidate.String() == code {
				known = true
				break
			}
		}
		if !known {
			code = "other"
		}
	}
	m.requests.WithLabelValues(protocol, name, code).Inc()
	m.duration.WithLabelValues(protocol, name).Observe(elapsed.Seconds())
}

// Возвращает HTTP-обработчик отдельного реестра Prometheus.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
