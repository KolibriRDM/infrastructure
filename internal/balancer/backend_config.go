package balancer

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Проверяет адреса и создаёт экземпляр, ещё не прошедший проверку готовности.
func NewBackend(name, httpAddress, grpcAddress string) (*Backend, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("create catalog backend: name is required: %w", ErrInvalidConfig)
	}
	httpAddress = strings.TrimSpace(httpAddress)
	parsed, err := url.Parse(httpAddress)
	if err != nil {
		return nil, fmt.Errorf("create catalog backend %q: invalid HTTP URL: %w", name, ErrInvalidConfig)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(httpAddress, "#") || (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" {
		return nil, fmt.Errorf("create catalog backend %q: HTTP URL must contain only scheme and host: %w", name, ErrInvalidConfig)
	}
	if strings.HasSuffix(parsed.Host, ":") || (parsed.Port() != "" && !validPort(parsed.Port())) {
		return nil, fmt.Errorf("create catalog backend %q: invalid HTTP port: %w", name, ErrInvalidConfig)
	}
	parsed.Path = ""
	grpcAddress = strings.TrimSpace(grpcAddress)
	host, port, err := net.SplitHostPort(grpcAddress)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " /?#@\\") || !validPort(port) {
		return nil, fmt.Errorf("create catalog backend %q: gRPC address must be host:port: %w", name, ErrInvalidConfig)
	}
	return &Backend{name: name, httpURL: *parsed, grpcAddress: grpcAddress}, nil
}

// Проверяет числовой TCP-порт в допустимом диапазоне.
func validPort(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	port, err := strconv.Atoi(value)
	return err == nil && port >= 1 && port <= 65535
}
