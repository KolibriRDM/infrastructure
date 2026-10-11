// Команда запускает локальные проверки инфраструктуры и нагрузочные сценарии.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/KolibriRDM/food-delivery-infrastructure/internal/devcheck"
)

// main отменяет проверки по сигналу и сохраняет ненулевой код при ошибке.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := devcheck.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
