package devcheck

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestReadyContracts различает ответы приложения и балансировщика без ложной готовности.
func TestReadyContracts(t *testing.T) {
	for _, tc := range []struct {
		status string
		code   int
		ready  bool
	}{{"ok", 200, true}, {"ready", 200, true}, {"not_ready", 503, false}, {"broken", 200, false}} {
		t.Run(tc.status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
				fmt.Fprintf(w, `{"status":%q}`, tc.status)
			}))
			defer server.Close()
			err := newRunner().ready(context.Background(), server.URL)
			if (err == nil) != tc.ready {
				t.Fatalf("ready=%v, error=%v", tc.ready, err)
			}
		})
	}
}

// TestEventuallyPropagatesDeadline ограничивает не только паузу, но и само наблюдение.
func TestEventuallyPropagatesDeadline(t *testing.T) {
	err := eventually(context.Background(), "probe", 20*time.Millisecond, func(ctx context.Context) (bool, error) { <-ctx.Done(); return false, ctx.Err() })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
}
