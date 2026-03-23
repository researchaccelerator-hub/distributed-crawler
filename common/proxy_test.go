package common

import "testing"

func TestPodProxyAddr(t *testing.T) {
	addrs := []string{
		"1.2.3.4:1080",
		"5.6.7.8:1080",
		"9.10.11.12:1080",
	}

	tests := []struct {
		name         string
		podName      string
		addrs        []string
		proxyOrdinal int
		want         string
		wantErr      bool
	}{
		{
			name:         "disabled when addrs empty",
			podName:      "crawler-0",
			addrs:        nil,
			proxyOrdinal: -1,
			want:         "",
		},
		{
			name:         "derives ordinal from pod name",
			podName:      "crawler-2",
			addrs:        addrs,
			proxyOrdinal: -1,
			want:         "9.10.11.12:1080",
		},
		{
			name:         "ordinal 0 from pod name",
			podName:      "crawler-0",
			addrs:        addrs,
			proxyOrdinal: -1,
			want:         "1.2.3.4:1080",
		},
		{
			name:         "multi-segment pod name uses last segment",
			podName:      "my-app-crawler-1",
			addrs:        addrs,
			proxyOrdinal: -1,
			want:         "5.6.7.8:1080",
		},
		{
			name:         "non-numeric pod suffix defaults to 0",
			podName:      "crawler-abc",
			addrs:        addrs,
			proxyOrdinal: -1,
			want:         "1.2.3.4:1080",
		},
		{
			name:         "empty pod name defaults to 0",
			podName:      "",
			addrs:        addrs,
			proxyOrdinal: -1,
			want:         "1.2.3.4:1080",
		},
		{
			name:         "explicit ordinal overrides pod name",
			podName:      "crawler-0",
			addrs:        addrs,
			proxyOrdinal: 2,
			want:         "9.10.11.12:1080",
		},
		{
			name:         "explicit ordinal 0",
			podName:      "crawler-2",
			addrs:        addrs,
			proxyOrdinal: 0,
			want:         "1.2.3.4:1080",
		},
		{
			name:         "ordinal exceeds list length",
			podName:      "crawler-5",
			addrs:        addrs,
			proxyOrdinal: -1,
			wantErr:      true,
		},
		{
			name:         "explicit ordinal exceeds list length",
			podName:      "crawler-0",
			addrs:        addrs,
			proxyOrdinal: 10,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PodProxyAddr(tt.podName, tt.addrs, tt.proxyOrdinal)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("PodProxyAddr(%q, %v, %d) = %q, want %q",
					tt.podName, tt.addrs, tt.proxyOrdinal, got, tt.want)
			}
		})
	}
}
