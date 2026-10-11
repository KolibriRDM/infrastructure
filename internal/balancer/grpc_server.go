package balancer

import (
	"fmt"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/transport/grpc/catalogv1"
	"google.golang.org/grpc"
)

// Регистрирует прокси каталога и ограничивает размер gRPC-сообщений.
func NewGRPCServer(proxy *GRPCProxy) (*grpc.Server, error) {
	if proxy == nil {
		return nil, fmt.Errorf("create gRPC server: proxy is required: %w", ErrInvalidConfig)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
	)
	catalogv1.RegisterCatalogServiceServer(server, proxy)
	return server, nil
}
