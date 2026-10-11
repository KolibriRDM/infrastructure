package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/platform/config"
)

// Запускает балансировщик и завершает процесс при ошибке.
func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("balancer failed", "service", "catalog-balancer", "error", err)
		os.Exit(1)
	}
}

// Загружает настройки, создаёт логгер и обрабатывает сигналы остановки.
func run() error {
	cfg, err := config.LoadBalancer()
	if err != nil {
		return fmt.Errorf("load balancer config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "catalog-balancer")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, logger)
}
