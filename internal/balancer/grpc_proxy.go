package balancer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Ограничивает размер одного protobuf-сообщения до 64 КиБ.
const maxGRPCMessageBytes = 64 << 10

// Ограничивает RPC, сохраняя более короткий входящий дедлайн.
const grpcRequestTimeout = 500 * time.Millisecond

// Выбирает каталог для каждого RPC и хранит переиспользуемые подключения.
type GRPCProxy struct {
	catalogv1.UnimplementedCatalogServiceServer
	pool        *Pool
	metrics     *Metrics
	connections map[*Backend]*grpc.ClientConn
	clients     map[*Backend]catalogv1.CatalogServiceClient
}

// Закрывает все подключения к экземплярам каталога.
func (h *GRPCProxy) Close() error {
	var closeErrors []error
	for backend, conn := range h.connections {
		err := conn.Close()
		if err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close catalog gRPC connection %q: %w", backend.Name(), err))
		}
	}
	return errors.Join(closeErrors...)
}

// Создаёт отдельное переиспользуемое gRPC-подключение для каждого экземпляра.
func NewGRPCProxy(pool *Pool, metrics *Metrics) (*GRPCProxy, error) {
	if pool == nil || len(pool.backends) == 0 {
		return nil, fmt.Errorf("create gRPC proxy: backend pool is required: %w", ErrInvalidConfig)
	}
	if metrics == nil || metrics.pool != pool {
		return nil, fmt.Errorf("create gRPC proxy: metrics must use the same pool: %w", ErrInvalidConfig)
	}
	h := &GRPCProxy{
		pool:        pool,
		metrics:     metrics,
		connections: make(map[*Backend]*grpc.ClientConn, len(pool.backends)),
		clients:     make(map[*Backend]catalogv1.CatalogServiceClient, len(pool.backends)),
	}
	for _, backend := range pool.backends {
		target := "passthrough:///" + backend.GRPCAddress()
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(maxGRPCMessageBytes),
		))
		if err != nil {
			createErr := fmt.Errorf("create catalog gRPC connection %q: %w", backend.Name(), err)
			closeErr := h.Close()
			return nil, errors.Join(createErr, closeErr)
		}
		h.connections[backend] = conn
		client := catalogv1.NewCatalogServiceClient(conn)
		h.clients[backend] = client
	}
	return h, nil
}

// Передаёт RPC выбранному экземпляру, сохраняя дедлайн и статус ошибки.
func (h *GRPCProxy) ResolveItems(
	ctx context.Context,
	req *catalogv1.ResolveItemsRequest,
) (response *catalogv1.ResolveItemsResponse, err error) {
	started := time.Now()
	var selected *Backend
	// Записываем итоговый gRPC-код и длительность каждого вызова.
	defer func() {
		panicValue := recover()
		code := status.Code(err).String()
		if panicValue != nil {
			code = codes.Internal.String()
		}
		h.metrics.ObserveRequest("grpc", selected, code, time.Since(started))
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	backend, err := h.pool.NextGRPC()
	if err != nil {
		if errors.Is(err, ErrNoHealthyBackends) {
			return nil, status.Error(codes.Unavailable, "no healthy catalog backends")
		}
		return nil, status.Error(codes.Internal, "select catalog backend failed")
	}
	selected = backend
	h.metrics.ObserveSelection("grpc", backend)
	rpcCtx, cancel := context.WithTimeout(ctx, grpcRequestTimeout)
	defer cancel()
	rpcCtx = catalogOutgoingContext(rpcCtx)
	client := h.clients[backend]
	response, err = client.ResolveItems(rpcCtx, req)
	if rpcCtx.Err() != nil {
		return nil, status.FromContextError(rpcCtx.Err()).Err()
	}
	if err != nil {
		return nil, err
	}
	return response, nil
}
