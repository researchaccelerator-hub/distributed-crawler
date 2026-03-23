package common

import "testing"

func TestPodProxyAddr(t *testing.T) {
	tests := []struct {
		name         string
		podName      string
		proxyDNSBase string
		proxyRegion  string
		proxyOrdinal int
		want         string
	}{
		{
			name:         "disabled when dns base empty",
			podName:      "crawler-0",
			proxyDNSBase: "",
			proxyRegion:  "eastus",
			proxyOrdinal: -1,
			want:         "",
		},
		{
			name:         "derives ordinal from pod name",
			podName:      "crawler-3",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: -1,
			want:         "mycrawl-proxy-3.eastus.azurecontainer.io:1080",
		},
		{
			name:         "ordinal 0 from pod name",
			podName:      "crawler-0",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "westus2",
			proxyOrdinal: -1,
			want:         "mycrawl-proxy-0.westus2.azurecontainer.io:1080",
		},
		{
			name:         "multi-segment pod name uses last segment",
			podName:      "my-app-crawler-12",
			proxyDNSBase: "prod-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: -1,
			want:         "prod-proxy-12.eastus.azurecontainer.io:1080",
		},
		{
			name:         "non-numeric pod suffix defaults to 0",
			podName:      "crawler-abc",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: -1,
			want:         "mycrawl-proxy-0.eastus.azurecontainer.io:1080",
		},
		{
			name:         "empty pod name defaults to 0",
			podName:      "",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: -1,
			want:         "mycrawl-proxy-0.eastus.azurecontainer.io:1080",
		},
		{
			name:         "explicit ordinal overrides pod name",
			podName:      "crawler-3",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: 7,
			want:         "mycrawl-proxy-7.eastus.azurecontainer.io:1080",
		},
		{
			name:         "explicit ordinal 0",
			podName:      "crawler-5",
			proxyDNSBase: "mycrawl-proxy",
			proxyRegion:  "eastus",
			proxyOrdinal: 0,
			want:         "mycrawl-proxy-0.eastus.azurecontainer.io:1080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PodProxyAddr(tt.podName, tt.proxyDNSBase, tt.proxyRegion, tt.proxyOrdinal)
			if got != tt.want {
				t.Errorf("PodProxyAddr(%q, %q, %q, %d) = %q, want %q",
					tt.podName, tt.proxyDNSBase, tt.proxyRegion, tt.proxyOrdinal, got, tt.want)
			}
		})
	}
}
