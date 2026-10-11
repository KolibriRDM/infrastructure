//go:build integration

package balancer_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const restaurant = "00000000-0000-4000-8000-000000000001"
const item = "00000000-0000-4000-8000-000000000101"

// Проверяет распределение, отказы и восстановление настоящего Compose-стенда.
func TestBalancerCompose(t *testing.T) {
	root := os.Getenv("FOOD_BALANCER_COMPOSE_DIR")
	if root == "" {
		t.Skip("set FOOD_BALANCER_COMPOSE_DIR to opt in: test stops catalog containers")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("FOOD_BALANCER_COMPOSE_DIR must be absolute")
	}
	compose := func(args ...string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "docker", append([]string{"compose", "--env-file", filepath.Join(root, ".env"), "-f", filepath.Join(root, "deploy/compose/compose.yaml")}, args...)...)
		command.Dir = root
		out, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("compose %v: %w: %s", args, err, out)
		}
		return nil
	}
	// Восстанавливаем процессы даже после проваленной проверки.
	t.Cleanup(func() {
		if err := compose("up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "60", "catalog", "catalog-2", "balancer"); err != nil {
			t.Error("restore:", err)
		}
	})
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	get := func(path string) (int, string, error) {
		resp, err := client.Get("http://127.0.0.1:8084" + path)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, string(b), err
	}
	metrics := func() string {
		t.Helper()
		code, b, err := get("/metrics")
		if err != nil || code != 200 {
			t.Fatalf("metrics: %d %v", code, err)
		}
		return b
	}
	sample := func(text, name, backend, protocol string) float64 {
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, name+"{") && strings.Contains(line, `backend="`+backend+`"`) && strings.Contains(line, `protocol="`+protocol+`"`) {
				fields := strings.Fields(line)
				v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
		}
		return 0
	}
	waitHealth := func(first, second float64) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			m := metrics()
			ok := true
			for i, want := range []float64{first, second} {
				for _, p := range []string{"http", "grpc"} {
					if sample(m, "food_balancer_backend_up", fmt.Sprintf("catalog-%d", i+1), p) != want {
						ok = false
					}
				}
			}
			if ok {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("backend health did not converge")
	}
	conn, err := grpc.NewClient("passthrough:///127.0.0.1:9084", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpc := catalogv1.NewCatalogServiceClient(conn)
	resolve := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r, err := rpc.ResolveItems(ctx, &catalogv1.ResolveItemsRequest{RestaurantId: restaurant, ItemIds: []string{item}}, grpc.WaitForReady(true))
		if err == nil && (r.GetRestaurantId() != restaurant || len(r.GetItems()) != 1 || r.Items[0].GetId() != item) {
			return fmt.Errorf("invalid RPC response: %v", r)
		}
		return err
	}
	traffic := func(first, second float64) {
		t.Helper()
		before := metrics()
		for range 4 {
			code, _, err := get("/v1/restaurants/" + restaurant + "/menu")
			if err != nil || code != 200 {
				t.Fatalf("menu: %d %v", code, err)
			}
			if err := resolve(); err != nil {
				t.Fatal(err)
			}
		}
		after := metrics()
		for i, want := range []float64{first, second} {
			for _, p := range []string{"http", "grpc"} {
				b := fmt.Sprintf("catalog-%d", i+1)
				delta := sample(after, "food_balancer_selections_total", b, p) - sample(before, "food_balancer_selections_total", b, p)
				if delta != want {
					t.Fatalf("%s %s selections delta=%v want=%v", b, p, delta, want)
				}
			}
		}
	}
	waitHealth(1, 1)
	traffic(2, 2)
	t.Log("both healthy: HTTP and RPC split 2/2 on one gRPC connection")
	if err := compose("stop", "catalog-2"); err != nil {
		t.Fatal(err)
	}
	waitHealth(1, 0)
	if code, _, err := get("/readyz"); err != nil || code != 200 {
		t.Fatalf("one backend readiness: %d %v", code, err)
	}
	traffic(4, 0)
	t.Log("catalog-2 stopped: all HTTP/RPC sent to catalog-1")
	if err := compose("up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "catalog-2"); err != nil {
		t.Fatal(err)
	}
	waitHealth(1, 1)
	traffic(2, 2)
	t.Log("catalog-2 restored: split 2/2 without restarting balancer")
	if err := compose("stop", "catalog", "catalog-2"); err != nil {
		t.Fatal(err)
	}
	waitHealth(0, 0)
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 503, "/v1/restaurants/" + restaurant + "/menu": 503} {
		start := time.Now()
		code, _, err := get(path)
		if err != nil || code != want || time.Since(start) > 2*time.Second {
			t.Fatalf("both unavailable %s: %d %v", path, code, err)
		}
	}
	start := time.Now()
	err = resolve()
	if status.Code(err) != codes.Unavailable || time.Since(start) > time.Second {
		t.Fatalf("both unavailable RPC: %v", err)
	}
	t.Log("both stopped: HTTP 503, RPC Unavailable, healthz 200; errors returned within limits")
	if err := compose("up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "catalog", "catalog-2"); err != nil {
		t.Fatal(err)
	}
	waitHealth(1, 1)
	traffic(2, 2)
	// Отменённый клиентский вызов не должен превращаться в недоступность каталога.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = rpc.ResolveItems(ctx, &catalogv1.ResolveItemsRequest{RestaurantId: restaurant, ItemIds: []string{item}})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("canceled RPC: %v", err)
	}
	if err := compose("stop", "balancer"); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	cmd := exec.CommandContext(ctx, "docker", "compose", "--env-file", filepath.Join(root, ".env"), "-f", filepath.Join(root, "deploy/compose/compose.yaml"), "ps", "-a", "--format", "json", "balancer")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`"ExitCode"\s*:\s*0(?:,|})`).Match(out) {
		t.Fatalf("balancer did not exit cleanly: %s", out)
	}
	if err := compose("up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "balancer"); err != nil {
		t.Fatal(err)
	}
	waitHealth(1, 1)
	traffic(2, 2)
	t.Log("SIGTERM exit code 0; after restart HTTP/RPC work and old client reconnects")
}
