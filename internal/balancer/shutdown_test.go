package balancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	grpcserver "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Проверяет завершение активных HTTP и gRPC запросов и отмену по сроку.
func TestShutdownServers(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "graceful"
		if force {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			requestsCtx, cancelRequests := context.WithCancel(context.Background())
			defer cancelRequests()
			release := make(chan struct{})
			releaseRequests := sync.OnceFunc(func() { close(release) })
			defer releaseRequests()
			httpStarted, grpcStarted := make(chan struct{}), make(chan struct{})
			httpCanceled, grpcCanceled := make(chan bool, 1), make(chan bool, 1)
			httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(httpStarted)
				select {
				case <-release:
					httpCanceled <- false
					_, _ = io.WriteString(w, "finished")
				case <-r.Context().Done():
					httpCanceled <- true
				}
			}))
			httpServer.Config.BaseContext = func(net.Listener) context.Context { return requestsCtx }
			shutdownStarted := make(chan struct{})
			httpServer.Config.RegisterOnShutdown(func() { close(shutdownStarted) })
			httpServer.Start()
			t.Cleanup(httpServer.Close)

			stub := &catalogServerStub{resolve: func(ctx context.Context, request *catalogv1.ResolveItemsRequest) (*catalogv1.ResolveItemsResponse, error) {
				restaurantID, itemIDs := request.GetRestaurantId(), request.GetItemIds()
				close(grpcStarted)
				select {
				case <-release:
					grpcCanceled <- false
					return &catalogv1.ResolveItemsResponse{RestaurantId: restaurantID, Items: []*catalogv1.ResolvedItem{
						{Id: itemIDs[0], Name: "Чай", PriceKopecks: 6000},
					}}, nil
				case <-ctx.Done():
					grpcCanceled <- true
					return nil, ctx.Err()
				}
			}}
			grpcServer := grpcserver.NewServer()
			catalogv1.RegisterCatalogServiceServer(grpcServer, stub)
			listener := bufconn.Listen(64 << 10)
			t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
			serveDone := make(chan error, 1)
			go func() { serveDone <- grpcServer.Serve(listener) }()
			conn, err := grpcserver.NewClient("passthrough:///catalog-test",
				grpcserver.WithTransportCredentials(insecure.NewCredentials()),
				grpcserver.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			httpResult, grpcResult := make(chan error, 1), make(chan error, 1)
			go func() {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL, nil)
				if err != nil {
					httpResult <- err
					return
				}
				resp, err := httpServer.Client().Do(req)
				if err == nil {
					defer resp.Body.Close()
					var body []byte
					body, err = io.ReadAll(resp.Body)
					if err == nil && (resp.StatusCode != http.StatusOK || string(body) != "finished") {
						err = fmt.Errorf("unexpected HTTP response: status=%d body=%q", resp.StatusCode, body)
					}
				}
				httpResult <- err
			}()
			go func() {
				resp, err := catalogv1.NewCatalogServiceClient(conn).ResolveItems(ctx, &catalogv1.ResolveItemsRequest{
					RestaurantId: "restaurant-1", ItemIds: []string{"item-1"},
				})
				if err == nil && (resp.GetRestaurantId() != "restaurant-1" || len(resp.GetItems()) != 1 ||
					resp.Items[0].GetId() != "item-1" || resp.Items[0].GetName() != "Чай" || resp.Items[0].GetPriceKopecks() != 6000) {
					err = fmt.Errorf("unexpected gRPC response: %v", resp)
				}
				grpcResult <- err
			}()
			// Начинаем остановку только после входа обоих запросов в обработчики.
			for _, started := range []<-chan struct{}{httpStarted, grpcStarted} {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("requests did not start", ctx.Err())
				}
			}
			shutdownCtx := ctx
			if force {
				// Истёкший срок проверяет принудительную остановку без задержек в тесте.
				var cancelShutdown context.CancelFunc
				shutdownCtx, cancelShutdown = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancelShutdown()
			}
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- ShutdownServers(shutdownCtx, httpServer.Config, grpcServer, cancelRequests) }()
			if !force {
				select {
				case <-shutdownStarted:
				case <-ctx.Done():
					t.Fatal("shutdown did not start", ctx.Err())
				}
				select {
				case err := <-shutdownDone:
					t.Fatalf("shutdown returned before active requests completed: %v", err)
				default:
				}
				releaseRequests()
			}
			select {
			case err := <-shutdownDone:
				if force && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("shutdown error = %v, want deadline exceeded", err)
				}
				if !force && err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("shutdown did not finish", ctx.Err())
			}
			for name, canceled := range map[string]<-chan bool{"HTTP": httpCanceled, "gRPC": grpcCanceled} {
				select {
				case got := <-canceled:
					if got != force {
						t.Errorf("%s canceled = %v, want %v", name, got, force)
					}
				case <-ctx.Done():
					t.Fatal(name, "handler did not finish", ctx.Err())
				}
			}
			if got := requestsCtx.Err() != nil; got != force {
				t.Errorf("HTTP base context canceled = %v, want %v", got, force)
			}
			for name, result := range map[string]<-chan error{"HTTP": httpResult, "gRPC": grpcResult} {
				select {
				case err := <-result:
					if !force && err != nil {
						t.Error(name, err)
					}
					if force && name == "gRPC" && err == nil {
						t.Error("forced RPC unexpectedly succeeded")
					}
				case <-ctx.Done():
					t.Fatal(name, "client did not finish", ctx.Err())
				}
			}
			select {
			case err := <-serveDone:
				if err != nil {
					t.Fatal("gRPC Serve", err)
				}
			case <-ctx.Done():
				t.Fatal("gRPC Serve did not finish", ctx.Err())
			}
		})
	}
}
