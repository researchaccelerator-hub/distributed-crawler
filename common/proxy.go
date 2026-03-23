package common

import (
	"fmt"
	"strconv"
	"strings"
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
