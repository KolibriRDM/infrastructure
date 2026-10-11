package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/pprof"
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
func run() (runErr error) {
	if path := os.Getenv("BALANCER_CPU_PROFILE"); path != "" {
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			return fmt.Errorf("start CPU profile: %w", err)
		}
		// Сначала завершаем сбор профиля, затем закрываем файл.
		defer func() { pprof.StopCPUProfile(); runErr = errors.Join(runErr, file.Close()) }()
	}
	cfg, err := config.LoadBalancer()
	if err != nil {
		return fmt.Errorf("load balancer config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "catalog-balancer")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, logger)
}
