package balancer

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

// Передаёт только один request ID допустимой длины и сохраняет дедлайн и отмену.
func TestCatalogOutgoingContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
		want string
	}{
		{"missing", nil, ""}, {"valid", []string{"request-123"}, "request-123"},
		{"empty", []string{""}, ""}, {"multiple", []string{"a", "b"}, ""},
		{"max", []string{strings.Repeat("x", 128)}, strings.Repeat("x", 128)},
		{"too long", []string{strings.Repeat("x", 129)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
			defer cancel()
			incoming := metadata.MD{"x-request-id": tc.ids, "authorization": []string{"secret"}, "private-bin": []string{"secret"}}
			ctx = metadata.NewIncomingContext(ctx, incoming)
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("private", "secret", "x-request-id", "old"))
			got := catalogOutgoingContext(ctx)
			md, _ := metadata.FromOutgoingContext(got)
			if tc.want == "" {
				if len(md) != 0 {
					t.Fatalf("unexpected metadata: %v", md)
				}
			} else if len(md) != 1 || len(md.Get("x-request-id")) != 1 || md.Get("x-request-id")[0] != tc.want {
				t.Fatalf("metadata=%v", md)
			}
			original, _ := metadata.FromIncomingContext(ctx)
			if original.Get("authorization")[0] != "secret" || len(original.Get("x-request-id")) != len(tc.ids) {
				t.Fatal("incoming metadata modified")
			}
			wantDeadline, _ := ctx.Deadline()
			deadline, ok := got.Deadline()
			if !ok || !deadline.Equal(wantDeadline) {
				t.Fatal("deadline lost")
			}
			cancel()
			if got.Err() != context.Canceled {
				t.Fatal("cancellation lost")
			}
		})
	}
}
