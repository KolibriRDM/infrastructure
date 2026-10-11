package balancer

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Передаёт каталогу только разрешённую metadata, сохраняя контекст запроса.
func catalogOutgoingContext(ctx context.Context) context.Context {
	incoming, _ := metadata.FromIncomingContext(ctx)
	outgoing := metadata.MD{}
	values := incoming.Get("x-request-id")
	if len(values) == 1 && len(values[0]) >= 1 && len(values[0]) <= 128 {
		outgoing.Set("x-request-id", values[0])
	}
	return metadata.NewOutgoingContext(ctx, outgoing)
}
