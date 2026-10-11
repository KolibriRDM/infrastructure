package devcheck

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// loadOptions задаёт ограниченную нагрузку без автоматического повтора запросов.
type loadOptions struct {
	scenario                       string
	clients                        int
	seconds, thinkMS, drainTimeout float64
	output                         string
}

// workerResult хранит измерения одной горутины без общей изменяемой памяти.
type workerResult struct {
	codes                 map[string]int
	durations, successful []float64
	examples              []map[string]any
	sample                *orderSample
}

// outboxSample связывает длину очереди с доступностью свежей статистики.
type outboxSample struct {
	Time    float64 `json:"time"`
	Pending float64 `json:"pending"`
	StatsUp float64 `json:"stats_up"`
}

// outboxStats собирается одной горутиной и читается после её завершения.
type outboxStats struct {
	Samples    []outboxSample `json:"samples"`
	MaxPending float64        `json:"max_pending"`
	Errors     []string       `json:"errors"`
}

// metricTotal извлекает сумму серии, отличая отсутствие метрики от нулевого значения.
func metricTotal(text, name string) (float64, error) {
	sum := 0.0
	found := false
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+" ") && !strings.HasPrefix(line, name+"{") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return 0, fmt.Errorf("invalid metric %s", name)
		}
		n, err := strconv.ParseFloat(parts[1], 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, fmt.Errorf("invalid metric value %s", name)
		}
		sum += n
		found = true
	}
	if !found {
		return 0, fmt.Errorf("missing metric %s", name)
	}
	return sum, nil
}

// selections группирует счётчики выбора по протоколу и экземпляру каталога.
func selections(text string) (map[string]float64, error) {
	result := map[string]float64{}
	backend := regexp.MustCompile(`backend="([^"]+)"`)
	protocol := regexp.MustCompile(`protocol="([^"]+)"`)
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "food_balancer_selections_total{") {
			continue
		}
		b, p := backend.FindStringSubmatch(line), protocol.FindStringSubmatch(line)
		if len(b) != 2 || len(p) != 2 {
			return nil, errors.New("missing backend or protocol label")
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, errors.New("invalid selections metric")
		}
		n, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, err
		}
		result[p[1]+"/"+b[1]] = n
	}
	return result, nil
}

// percentile использует ближайший ранг и не меняет порядок исходных измерений.
func percentile(values []float64, fraction float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	value := sorted[max(0, int(math.Ceil(float64(len(sorted))*fraction))-1)]
	return &value
}

// metrics читает текстовый экспорт с коротким таймаутом.
func (r *runner) metrics(ctx context.Context, base string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	data, err := r.page(ctx, base+"/metrics")
	return string(data), err
}

// outboxSnapshot требует наличия обеих метрик для достоверной проверки очереди.
func (r *runner) outboxSnapshot(ctx context.Context) (outboxSample, error) {
	text, err := r.metrics(ctx, r.orders)
	if err != nil {
		return outboxSample{}, err
	}
	pending, err := metricTotal(text, "food_outbox_pending_events")
	if err != nil {
		return outboxSample{}, err
	}
	up, err := metricTotal(text, "food_outbox_stats_up")
	return outboxSample{Time: float64(time.Now().UnixNano()) / 1e9, Pending: pending, StatsUp: up}, err
}

// monitorOutbox опрашивает очередь до отмены и возвращает собранные измерения.
func (r *runner) monitorOutbox(ctx context.Context) outboxStats {
	stats := outboxStats{Samples: []outboxSample{}, Errors: []string{}}
	for {
		sample, err := r.outboxSnapshot(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return stats
			}
			stats.Errors = append(stats.Errors, err.Error())
		} else {
			stats.Samples = append(stats.Samples, sample)
			stats.MaxPending = max(stats.MaxPending, sample.Pending)
		}
		if wait(ctx, time.Second) != nil {
			return stats
		}
	}
}

// drainOutbox ждёт двух последовательных нулевых измерений со свежей статистикой.
func (r *runner) drainOutbox(ctx context.Context, timeout time.Duration) (*float64, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stable := 0
	for {
		sample, err := r.outboxSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		if sample.StatsUp == 1 && sample.Pending == 0 {
			stable++
		} else {
			stable = 0
		}
		if stable == 2 {
			seconds := time.Since(started).Seconds()
			return &seconds, nil
		}
		if err = wait(ctx, 5*time.Second); err != nil {
			return nil, fmt.Errorf("outbox drain: %w", err)
		}
	}
}

// loadWorker выполняет один запрос за раз и собирает собственную статистику.
func (r *runner) loadWorker(ctx context.Context, opt loadOptions, runID string, users []string, number int, deadline time.Time) workerResult {
	result := workerResult{codes: map[string]int{}, examples: []map[string]any{}}
	// Свой transport ограничивает каждого клиента одним активным соединением.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 1
	transport.MaxConnsPerHost = 1
	client := *r
	client.client = &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer transport.CloseIdleConnections()
	for seq := 1; time.Now().Before(deadline) && ctx.Err() == nil; seq++ {
		method, url, payload := "GET", r.balancer+menuPath, any(nil)
		headers := map[string]string{}
		user := ""
		if opt.scenario == "orders" {
			method, url, payload = "POST", r.orders+"/v1/orders", orderPayload()
			user = users[((seq-1)*opt.clients+number)%len(users)]
			headers["X-Demo-User-ID"] = user
			headers["Idempotency-Key"] = fmt.Sprintf("load-%s-%d-%d", runID, number, seq)
		}
		started := time.Now()
		data, status, _, err := client.request(ctx, method, url, payload, headers)
		code := strconv.Itoa(status)
		if err == nil && status >= 200 && status < 300 {
			if opt.scenario == "orders" {
				var reply orderReply
				err = json.Unmarshal(data, &reply)
				if err == nil && reply.ID == "" {
					err = errors.New("missing order ID")
				}
				if err == nil && result.sample == nil {
					result.sample = &orderSample{OrderID: reply.ID, UserID: user}
				}
			} else {
				err = decodeMenu(data)
			}
			if err == nil {
				result.successful = append(result.successful, float64(time.Since(started))/float64(time.Millisecond))
			}
		}
		if err != nil {
			code = "transport_or_validation_error"
		}
		if err != nil || status < 200 || status >= 300 {
			if len(result.examples) < 10 {
				example := map[string]any{"code": code, "idempotency_key": headers["Idempotency-Key"], "user_id": user}
				if err != nil {
					example["error"] = truncate(err.Error(), 200)
				} else {
					example["body"] = truncate(string(data), 250)
				}
				result.examples = append(result.examples, example)
			}
		}
		result.codes[code]++
		result.durations = append(result.durations, float64(time.Since(started))/float64(time.Millisecond))
		if opt.thinkMS > 0 && wait(ctx, time.Duration(opt.thinkMS*float64(time.Millisecond))) != nil {
			break
		}
	}
	return result
}

// truncate ограничивает диагностический текст, сохраняя корректный UTF-8.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}

// load запускает горутины клиентов, наблюдает outbox и сохраняет отчёт.
func (r *runner) load(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "menu" && args[0] != "orders" {
		return errors.New("usage: devcheck load menu|orders [--clients 20 --seconds 180 --think-ms 20 --drain-timeout 900 --output path]")
	}
	opt := loadOptions{scenario: args[0]}
	flags := flag.NewFlagSet("load", flag.ContinueOnError)
	flags.IntVar(&opt.clients, "clients", 20, "concurrent clients")
	flags.Float64Var(&opt.seconds, "seconds", 180, "load duration")
	flags.Float64Var(&opt.thinkMS, "think-ms", 20, "pause after each request")
	flags.Float64Var(&opt.drainTimeout, "drain-timeout", 900, "outbox drain deadline")
	flags.StringVar(&opt.output, "output", "", "JSON report path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected load arguments")
	}
	if opt.clients < 1 || !finitePositive(opt.seconds) || !finitePositive(opt.drainTimeout) || math.IsNaN(opt.thinkMS) || math.IsInf(opt.thinkMS, 0) || opt.thinkMS < 0 {
		return errors.New("clients/seconds/drain-timeout must be positive and finite; think-ms nonnegative")
	}
	runID, err := randomID()
	if err != nil {
		return err
	}
	users := make([]string, 10000)
	for i := range users {
		users[i], err = uuid()
		if err != nil {
			return err
		}
	}
	raw, err := r.metrics(ctx, r.balancer)
	if err != nil {
		return err
	}
	initial, err := selections(raw)
	if err != nil {
		return err
	}
	monitorCtx, stop := context.WithCancel(ctx)
	defer stop()
	statsCh := make(chan outboxStats, 1)
	go func() { statsCh <- r.monitorOutbox(monitorCtx) }()
	started := time.Now()
	deadline := started.Add(time.Duration(opt.seconds * float64(time.Second)))
	results := make([]workerResult, opt.clients)
	var workers sync.WaitGroup
	for i := range results {
		workers.Add(1)
		go func(i int) { defer workers.Done(); results[i] = r.loadWorker(ctx, opt, runID, users, i, deadline) }(i)
	}
	workers.Wait()
	elapsed := time.Since(started).Seconds()
	finished := float64(time.Now().UnixNano()) / 1e9
	var verificationErrors []string
	final := map[string]float64{}
	raw, err = r.metrics(ctx, r.balancer)
	if err == nil {
		final, err = selections(raw)
	}
	if err != nil {
		verificationErrors = append(verificationErrors, err.Error())
	}
	drained, drainErr := r.drainOutbox(ctx, time.Duration(opt.drainTimeout*float64(time.Second)))
	if drainErr != nil {
		verificationErrors = append(verificationErrors, drainErr.Error())
	}
	stop()
	stats := <-statsCh
	codes := map[string]int{}
	var durations, successful []float64
	examples := []map[string]any{}
	samples := []orderSample{}
	for _, result := range results {
		for k, v := range result.codes {
			codes[k] += v
		}
		durations = append(durations, result.durations...)
		successful = append(successful, result.successful...)
		examples = append(examples, result.examples...)
		if result.sample != nil {
			samples = append(samples, *result.sample)
		}
	}
	delta := map[string]float64{}
	for k, v := range initial {
		delta[k] = -v
	}
	for k, v := range final {
		delta[k] += v
	}
	var machine map[string]any
	var containers []json.RawMessage
	// Ошибка сбора окружения не уничтожает уже полученные измерения.
	raw, err = command(ctx, "", "docker", "info", "--format", "{{json .}}")
	if err == nil {
		err = json.Unmarshal([]byte(raw), &machine)
	}
	if err != nil {
		verificationErrors = append(verificationErrors, err.Error())
	}
	raw, err = compose(ctx, "", "--profile", "logs", "--profile", "monitoring", "ps", "--format", "json")
	if err == nil {
		for _, line := range strings.Split(raw, "\n") {
			if strings.TrimSpace(line) != "" {
				if !json.Valid([]byte(line)) {
					err = errors.New("invalid Compose JSON")
					break
				}
				containers = append(containers, json.RawMessage(line))
			}
		}
	}
	if err != nil {
		verificationErrors = append(verificationErrors, err.Error())
	}
	docker := map[string]any{}
	for _, k := range []string{"ServerVersion", "Architecture", "NCPU", "MemTotal"} {
		docker[k] = machine[k]
	}
	http5xx := 0
	failed := false
	for k, v := range codes {
		if strings.HasPrefix(k, "5") {
			http5xx += v
		}
		if !strings.HasPrefix(k, "2") {
			failed = true
		}
	}
	poolSize := 0
	if opt.scenario == "orders" {
		poolSize = len(users)
	}
	report := map[string]any{
		"scenario":                opt.scenario,
		"run_id":                  runID,
		"timestamp":               time.Now().UTC().Format(time.RFC3339Nano),
		"clients":                 opt.clients,
		"duration_requested_s":    opt.seconds,
		"load_finished_at":        finished,
		"drain_timeout_s":         opt.drainTimeout,
		"elapsed_s":               elapsed,
		"think_ms":                opt.thinkMS,
		"load_model":              "closed loop, one outstanding request per client; not a maximum capacity benchmark",
		"user_pool":               poolSize,
		"limiter_enabled":         true,
		"requests":                len(durations),
		"rps":                     float64(len(durations)) / elapsed,
		"successful_rps":          float64(len(successful)) / elapsed,
		"status_counts":           codes,
		"http_429":                codes["429"],
		"http_5xx":                http5xx,
		"p95_ms":                  percentile(durations, .95),
		"successful_p95_ms":       percentile(successful, .95),
		"p99_ms":                  percentile(durations, .99),
		"backend_selection_delta": delta,
		"outbox":                  stats,
		"outbox_drained_after_s":  drained,
		"error_examples":          examples,
		"sample_orders":           samples,
		"host":                    runtime.GOOS + "/" + runtime.GOARCH,
		"docker":                  docker,
		"containers":              containers,
		"verification_errors":     verificationErrors,
		"generator":               "Go " + runtime.Version(),
	}
	if opt.output == "" {
		opt.output = filepath.Join("reports", "stage8", opt.scenario+"-load.json")
	}
	if err = saveJSON(opt.output, report); err != nil {
		return err
	}
	summary := map[string]any{}
	for _, k := range []string{"scenario", "requests", "rps", "p95_ms", "status_counts", "backend_selection_delta", "outbox_drained_after_s"} {
		summary[k] = report[k]
	}
	if err = printJSON(summary); err != nil {
		return err
	}
	fmt.Println("Report:", opt.output)
	if failed || drained == nil || len(verificationErrors) > 0 || len(durations) == 0 {
		return fmt.Errorf("load check failed; see report %s", opt.output)
	}
	return ctx.Err()
}

// finitePositive исключает значения, которые нельзя преобразовать в длительность.
func finitePositive(n float64) bool {
	return n > 0 && !math.IsNaN(n) && !math.IsInf(n, 0) && n < float64(math.MaxInt64)/float64(time.Second)
}
