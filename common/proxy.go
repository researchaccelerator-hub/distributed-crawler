package common

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/net/proxy"
)

// PodProxyAddr returns the SOCKS5 proxy address for this pod by selecting
// from a list of proxy addresses using the pod's StatefulSet ordinal.
// Returns "" if addrs is empty (proxies disabled → direct connection).
//
// If proxyOrdinal >= 0, it is used directly instead of parsing the pod name.
// This supports local development where there is no StatefulSet pod name.
func PodProxyAddr(podName string, addrs []string, proxyOrdinal int) (string, error) {
	if len(addrs) == 0 {
		return "", nil
	}

	ordinal := proxyOrdinal
	if ordinal < 0 {
		// Derive from pod name: "crawler-3" → 3
		parts := strings.Split(podName, "-")
		parsed, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			ordinal = 0 // local dev / non-StatefulSet — use first proxy
		} else {
			ordinal = parsed
		}
	}

	if ordinal >= len(addrs) {
		return "", fmt.Errorf("proxy ordinal %d exceeds proxy list length %d", ordinal, len(addrs))
	}
	return addrs[ordinal], nil
}

// VerifyOutboundIP checks the outbound IP by hitting ifconfig.me, routing through
// the SOCKS5 proxy if proxyAddr is non-empty. Returns an error if the check fails.
// This should be called once at startup to confirm proxy connectivity.
func VerifyOutboundIP(proxyAddr, proxyUser, proxyPass string) error {
	var httpClient *http.Client

	if proxyAddr != "" {
		var auth *proxy.Auth
		if proxyUser != "" {
			auth = &proxy.Auth{User: proxyUser, Password: proxyPass}
		}
		dialer, err := proxy.SOCKS5("tcp", proxyAddr, auth, proxy.Direct)
		if err != nil {
			return fmt.Errorf("failed to create SOCKS5 dialer for IP check: %w", err)
		}
		contextDialer, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return fmt.Errorf("SOCKS5 dialer does not implement ContextDialer")
		}
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DialContext: contextDialer.DialContext,
			},
		}
	} else {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	req, err := http.NewRequest("GET", "https://ifconfig.me/ip", nil)
	if err != nil {
		return fmt.Errorf("failed to create IP check request: %w", err)
	}
	req.Header.Set("User-Agent", "curl/7.79.1")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("IP check failed (proxy=%q): %w", proxyAddr, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read IP check response: %w", err)
	}

	ipAddress := strings.TrimSpace(string(body))

	if proxyAddr != "" {
		log.Info().Str("outbound_ip", ipAddress).Str("proxy", proxyAddr).Msg("proxy IP verified")
	} else {
		log.Info().Str("outbound_ip", ipAddress).Msg("direct outbound IP")
	}
	return nil
}
