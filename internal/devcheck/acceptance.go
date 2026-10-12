package devcheck

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// checkAcceptance сверяет изоляцию ролей, живые targets и все запросы dashboard.
func (r *runner) checkAcceptance(ctx context.Context) error {
	isolated, err := sql(ctx, "postgres", `SELECT bool_and(NOT has_database_privilege(r,d,'CONNECT')) FROM (VALUES ('catalog_app','orders_db'),('catalog_app','notifications_db'),('orders_app','catalog_db'),('orders_app','notifications_db'),('notifications_app','catalog_db'),('notifications_app','orders_db')) AS pairs(r,d);`)
	if err != nil {
		return err
	}
	if isolated != "t" {
		return errors.New("service database roles are not isolated")
	}
	var targets struct {
		Status string `json:"status"`
		Data   struct {
			Active []struct {
				Health string            `json:"health"`
				Labels map[string]string `json:"labels"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	if err = eventually(ctx, "five Prometheus targets UP", 30*time.Second, func(ctx context.Context) (bool, error) {
		if e := r.getJSON(ctx, "GET", "http://127.0.0.1:9090/api/v1/targets", nil, nil, &targets); e != nil {
			return false, e
		}
		if targets.Status != "success" || len(targets.Data.Active) != 5 {
			return false, nil
		}
		for _, target := range targets.Data.Active {
			if target.Health != "up" {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	// Для локального API читаем только две настройки Grafana, не выводя пароль.
	data, err := os.ReadFile(".env")
	if err != nil {
		return err
	}
	user, password := "admin", ""
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		value = strings.Trim(value, "\"'")
		switch key {
		case "GRAFANA_ADMIN_USER":
			user = value
		case "GRAFANA_ADMIN_PASSWORD":
			password = value
		}
	}
	if password == "" {
		return errors.New("GRAFANA_ADMIN_PASSWORD is missing")
	}
	headers := map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))}
	var dashboard struct {
		Dashboard struct {
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		} `json:"dashboard"`
	}
	if err = r.getJSON(ctx, "GET", "http://127.0.0.1:3000/api/dashboards/uid/food-delivery-overview", nil, headers, &dashboard); err != nil {
		return err
	}
	if len(dashboard.Dashboard.Panels) < 17 {
		return errors.New("Grafana dashboard is incomplete")
	}
	queries := 0
	for _, panel := range dashboard.Dashboard.Panels {
		for _, target := range panel.Targets {
			if target.Expr == "" {
				continue
			}
			var response struct {
				Status string `json:"status"`
				Error  string `json:"error"`
			}
			// Grafana подставляет интервал сама; для проверки берём окно в одну минуту.
			expr := strings.ReplaceAll(target.Expr, "$__rate_interval", "1m")
			if err = r.getJSON(ctx, "GET", "http://127.0.0.1:9090/api/v1/query?query="+url.QueryEscape(expr), nil, nil, &response); err != nil {
				return err
			}
			if response.Status != "success" {
				return fmt.Errorf("panel %s: %s", panel.Title, response.Error)
			}
			queries++
		}
	}
	if queries == 0 {
		return errors.New("dashboard has no PromQL queries")
	}
	if err = r.checkGUIs(ctx); err != nil {
		return err
	}
	result := map[string]any{"checked_at": time.Now().UTC(), "database_roles_isolated": true, "targets_up": len(targets.Data.Active), "grafana_panels": len(dashboard.Dashboard.Panels), "promql_queries_checked": queries}
	if err = saveJSON("reports/stage9/acceptance.json", result); err != nil {
		return err
	}
	return printJSON(result)
}
