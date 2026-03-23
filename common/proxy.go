package common

import (
	"fmt"
	"strconv"
	"strings"
)

// PodProxyAddr returns the SOCKS5 proxy address for this pod, derived from
// the pod's StatefulSet ordinal and the DNS base name + region.
// Returns "" if proxyDNSBase is empty (proxies disabled → direct connection).
//
// If proxyOrdinal >= 0, it is used directly instead of parsing the pod name.
// This supports local development where there is no StatefulSet pod name.
func PodProxyAddr(podName, proxyDNSBase, proxyRegion string, proxyOrdinal int) string {
	if proxyDNSBase == "" {
		return ""
	}
	ordinal := proxyOrdinal
	if ordinal < 0 {
		// Derive from pod name: "crawler-3" → 3
		parts := strings.Split(podName, "-")
		parsed, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			ordinal = 0 // local dev / non-StatefulSet — use proxy-0
		} else {
			ordinal = parsed
		}
	}
	return fmt.Sprintf("%s-%d.%s.azurecontainer.io:1080", proxyDNSBase, ordinal, proxyRegion)
}
