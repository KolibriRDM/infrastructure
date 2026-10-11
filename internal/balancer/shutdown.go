package balancer

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	grpcserver "google.golang.org/grpc"
)

// Останавливает оба сервера, отменяя запросы после истечения срока.
func ShutdownServers(
	ctx context.Context,
	httpServer *http.Server,
	grpcServer *grpcserver.Server,
	cancelRequests context.CancelFunc,
) error {
	httpDone := make(chan error, 1)
	// HTTP завершается параллельно с gRPC в пределах общего срока.
	go func() {
		err := httpServer.Shutdown(ctx)
		if err != nil {
			cancelRequests()
			_ = httpServer.Close()
			httpDone <- fmt.Errorf("shutdown HTTP: %w", err)
			return
		}
		httpDone <- nil
	}()
	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()
	var grpcErr error
	select {
	case <-grpcDone:
	case <-ctx.Done():
		cancelRequests()
		// По истечении срока отменяем незавершённые RPC.
		grpcServer.Stop()
		<-grpcDone
		grpcErr = fmt.Errorf("shutdown gRPC: %w", ctx.Err())
	}
	httpErr := <-httpDone
	return errors.Join(httpErr, grpcErr)
}
