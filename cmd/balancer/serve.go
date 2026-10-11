package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/balancer"
	"github.com/KolibriRDM/food-delivery-infrastructure/internal/platform/config"
	grpcserver "google.golang.org/grpc"
)

// Собирает компоненты балансировщика и управляет их жизненным циклом.
func serve(ctx context.Context, cfg config.BalancerConfig, logger *slog.Logger) (runErr error) {
	if ctx.Err() != nil {
		return nil
	}
	backends := make([]*balancer.Backend, 0, len(cfg.Backends))
	for _, item := range cfg.Backends {
		backend, err := balancer.NewBackend(item.Name, item.HTTPURL, item.GRPCAddr)
		if err != nil {
			return fmt.Errorf("configure backend %q: %w", item.Name, err)
		}
		backends = append(backends, backend)
	}
	pool, err := balancer.NewPool(backends)
	if err != nil {
		return err
	}
	serviceMetrics := balancer.NewMetrics(pool)
	grpcProxy, err := balancer.NewGRPCProxy(pool, serviceMetrics)
	if err != nil {
		return err
	}
	// Закрываем исходящие gRPC-подключения после завершения остальных компонентов.
	defer func() { runErr = errors.Join(runErr, grpcProxy.Close()) }()
	httpProxy, err := balancer.NewHTTPProxy(pool, logger, serviceMetrics)
	if err != nil {
		return err
	}
	defer httpProxy.CloseIdleConnections()
	checker, err := balancer.NewHealthChecker(pool, grpcProxy)
	if err != nil {
		return err
	}
	defer checker.CloseIdleConnections()
	router, err := balancer.NewRouter(pool, httpProxy, serviceMetrics)
	if err != nil {
		return err
	}

	// Сигнал остановки не отменяет активные запросы до окончания graceful shutdown.
	requestsCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr, Handler: router,
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		ErrorLog:    slog.NewLogLogger(logger.Handler(), slog.LevelError),
		BaseContext: func(net.Listener) context.Context { return requestsCtx },
	}
	defer httpServer.Close()
	grpcServer, err := balancer.NewGRPCServer(grpcProxy)
	if err != nil {
		return err
	}
	defer grpcServer.Stop()

	// Проверяем оба порта до запуска обработки запросов.
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}
	defer grpcListener.Close()

	checksCtx, cancelChecks := context.WithCancel(context.Background())
	checksDone := make(chan struct{})
	// Останавливаем проверки до закрытия исходящих соединений.
	defer func() { cancelChecks(); <-checksDone }()
	go func() { defer close(checksDone); checker.Run(checksCtx) }()

	// Буфер вмещает результаты обоих серверов независимо от порядка остановки.
	serverErrors := make(chan error, 2)
	go func() {
		err := httpServer.Serve(httpListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- fmt.Errorf("serve HTTP: %w", err)
			return
		}
		serverErrors <- nil
	}()
	go func() {
		err := grpcServer.Serve(grpcListener)
		if err != nil && !errors.Is(err, grpcserver.ErrServerStopped) {
			serverErrors <- fmt.Errorf("serve gRPC: %w", err)
			return
		}
		serverErrors <- nil
	}()
	logger.Info("HTTP server started", "addr", httpListener.Addr().String())
	logger.Info("gRPC server started", "addr", grpcListener.Addr().String())
	remainingServers := 2
	select {
	case err := <-serverErrors:
		remainingServers--
		if err != nil {
			runErr = err
		} else if ctx.Err() == nil {
			runErr = fmt.Errorf("server stopped unexpectedly")
		}
	case <-ctx.Done():
	}
	logger.Info("servers stopping")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	shutdownErr := balancer.ShutdownServers(shutdownCtx, httpServer, grpcServer, cancelRequests)
	// Дожидаемся обеих горутин Serve, учитывая уже полученный результат.
	for ; remainingServers > 0; remainingServers-- {
		runErr = errors.Join(runErr, <-serverErrors)
	}
	cancelRequests()
	logger.Info("servers stopped")
	return errors.Join(runErr, shutdownErr)
}
