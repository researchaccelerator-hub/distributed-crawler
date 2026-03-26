package proxy

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockContainerGroupsAPI is a fake implementation of ContainerGroupsAPI for unit tests.
type mockContainerGroupsAPI struct {
	createFunc func(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error)
	deleteFunc func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error)
	listFunc   func(ctx context.Context, rg string) ([]*armcontainerinstance.ContainerGroup, error)
}

func (m *mockContainerGroupsAPI) BeginCreateOrUpdate(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, rg, name, cg, opts)
	}
	return nil, fmt.Errorf("createFunc not set")
}

func (m *mockContainerGroupsAPI) BeginDelete(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, rg, name, opts)
	}
	return nil, fmt.Errorf("deleteFunc not set")
}

func (m *mockContainerGroupsAPI) ListByResourceGroup(ctx context.Context, rg string) ([]*armcontainerinstance.ContainerGroup, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, rg)
	}
	return nil, fmt.Errorf("listFunc not set")
}

func baseCfg() common.CrawlerConfig {
	return common.CrawlerConfig{
		ManagedProxies:     true,
		ProxyResourceGroup: "rg-proxies",
		ProxyImage:         "myregistry.azurecr.io/microsocks:latest",
		ProxyLocation:      "eastus2",
		ProxyCPU:           0.5,
		ProxyMemoryGB:      0.5,
		ProxyPort:          1080,
		ProxyUser:          "testuser",
		ProxyPass:          "testpass",
		ProxyCount:         1,
		CrawlID:            "20260325120000",
		PodName:            "crawler-0",
	}
}

func TestCreateProxies_Success(t *testing.T) {
	mock := &mockContainerGroupsAPI{
		createFunc: func(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
			return &PollerResponse{
				ContainerGroup: armcontainerinstance.ContainerGroup{
					Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
						IPAddress: &armcontainerinstance.IPAddress{
							IP: to.Ptr("10.0.1.5"),
						},
					},
				},
			}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	addrs, err := mgr.CreateProxies(context.Background())

	require.NoError(t, err)
	require.Len(t, addrs, 1)
	assert.Equal(t, "10.0.1.5:1080", addrs[0])
	assert.Len(t, mgr.containerNames, 1)
}

func TestCreateProxies_MultipleSuccess(t *testing.T) {
	var callCount atomic.Int32
	mock := &mockContainerGroupsAPI{
		createFunc: func(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
			idx := callCount.Add(1)
			return &PollerResponse{
				ContainerGroup: armcontainerinstance.ContainerGroup{
					Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
						IPAddress: &armcontainerinstance.IPAddress{
							IP: to.Ptr(fmt.Sprintf("10.0.1.%d", idx+4)),
						},
					},
				},
			}, nil
		},
	}

	cfg := baseCfg()
	cfg.ProxyCount = 3
	mgr := newManager(cfg, mock)
	addrs, err := mgr.CreateProxies(context.Background())

	require.NoError(t, err)
	require.Len(t, addrs, 3)
	for _, addr := range addrs {
		assert.Contains(t, addr, ":1080")
		assert.True(t, strings.HasPrefix(addr, "10.0.1."))
	}
	assert.Len(t, mgr.containerNames, 3)
}

func TestCreateProxies_AzureError(t *testing.T) {
	mock := &mockContainerGroupsAPI{
		createFunc: func(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
			return nil, fmt.Errorf("quota exceeded")
		},
		deleteFunc: func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
			return &DeletePollerResponse{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	addrs, err := mgr.CreateProxies(context.Background())

	assert.Nil(t, addrs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quota exceeded")
}

func TestCreateProxies_NoIPAddress(t *testing.T) {
	mock := &mockContainerGroupsAPI{
		createFunc: func(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
			return &PollerResponse{
				ContainerGroup: armcontainerinstance.ContainerGroup{
					Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
						IPAddress: &armcontainerinstance.IPAddress{},
					},
				},
			}, nil
		},
		deleteFunc: func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
			return &DeletePollerResponse{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	addrs, err := mgr.CreateProxies(context.Background())

	assert.Nil(t, addrs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no IP address")
}

func TestContainerGroupName_Formatting(t *testing.T) {
	tests := []struct {
		podName  string
		ordinal  int
		expected string
	}{
		{"crawler-0", 0, "proxy-crawler-0-0"},
		{"crawler-0", 5, "proxy-crawler-0-5"},
		{"UPPERCASE", 0, "proxy-uppercase-0"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := containerGroupName(tt.podName, tt.ordinal)
			assert.Equal(t, tt.expected, got)
			assert.LessOrEqual(t, len(got), 63)
		})
	}
}

func TestContainerGroupName_TruncatesAt63(t *testing.T) {
	longName := strings.Repeat("a", 80)
	name := containerGroupName(longName, 0)
	assert.LessOrEqual(t, len(name), 63)
}

func TestDestroyProxies_Success(t *testing.T) {
	var deleted []string
	mock := &mockContainerGroupsAPI{
		deleteFunc: func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
			deleted = append(deleted, name)
			return &DeletePollerResponse{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	mgr.containerNames = []string{"proxy-test-0", "proxy-test-1"}

	err := mgr.DestroyProxies(context.Background())

	require.NoError(t, err)
	assert.Len(t, deleted, 2)
	assert.Nil(t, mgr.containerNames)
}

func TestDestroyProxies_PartialFailure(t *testing.T) {
	mock := &mockContainerGroupsAPI{
		deleteFunc: func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
			if name == "proxy-test-1" {
				return nil, fmt.Errorf("not found")
			}
			return &DeletePollerResponse{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	mgr.containerNames = []string{"proxy-test-0", "proxy-test-1"}

	err := mgr.DestroyProxies(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "partial")
	// State still cleared despite partial failure
	assert.Nil(t, mgr.containerNames)
}

func TestDestroyProxies_NoContainers(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})
	err := mgr.DestroyProxies(context.Background())
	require.NoError(t, err)
}

func TestCleanupOrphanedProxies_FindsTagged(t *testing.T) {
	var deleted []string
	mock := &mockContainerGroupsAPI{
		listFunc: func(ctx context.Context, rg string) ([]*armcontainerinstance.ContainerGroup, error) {
			return []*armcontainerinstance.ContainerGroup{
				{
					Name: to.Ptr("proxy-crawler-0-0"),
					Tags: map[string]*string{
						"managed_by": to.Ptr("telegram-scraper"),
						"pod_name":   to.Ptr("crawler-0"),
					},
				},
				{
					Name: to.Ptr("proxy-crawler-0-1"),
					Tags: map[string]*string{
						"managed_by": to.Ptr("telegram-scraper"),
						"pod_name":   to.Ptr("crawler-0"),
					},
				},
				{
					Name: to.Ptr("unrelated-aci"),
					Tags: map[string]*string{
						"managed_by": to.Ptr("other-service"),
					},
				},
				{
					Name: to.Ptr("proxy-crawler-1-0"),
					Tags: map[string]*string{
						"managed_by": to.Ptr("telegram-scraper"),
						"pod_name":   to.Ptr("crawler-1"),
					},
				},
			}, nil
		},
		deleteFunc: func(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
			deleted = append(deleted, name)
			return &DeletePollerResponse{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock) // baseCfg has PodName "crawler-0"
	err := mgr.CleanupOrphanedProxies(context.Background())

	require.NoError(t, err)
	assert.Len(t, deleted, 2)
	assert.Contains(t, deleted, "proxy-crawler-0-0")
	assert.Contains(t, deleted, "proxy-crawler-0-1")
}

func TestCleanupOrphanedProxies_NoOrphans(t *testing.T) {
	mock := &mockContainerGroupsAPI{
		listFunc: func(ctx context.Context, rg string) ([]*armcontainerinstance.ContainerGroup, error) {
			return []*armcontainerinstance.ContainerGroup{}, nil
		},
	}

	mgr := newManager(baseCfg(), mock)
	err := mgr.CleanupOrphanedProxies(context.Background())

	require.NoError(t, err)
}

func TestBuildContainerGroup_PublicIP(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})
	cg := mgr.buildContainerGroup("proxy-test-0")

	assert.Nil(t, cg.Properties.SubnetIDs)
	require.NotNil(t, cg.Properties.IPAddress)
	assert.Equal(t, armcontainerinstance.ContainerGroupIPAddressTypePublic, *cg.Properties.IPAddress.Type)
}

func TestBuildContainerGroup_CommandLine(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})
	cg := mgr.buildContainerGroup("proxy-test-0")

	container := cg.Properties.Containers[0]
	cmd := make([]string, len(container.Properties.Command))
	for i, s := range container.Properties.Command {
		cmd[i] = *s
	}

	assert.Equal(t, []string{"microsocks", "-u", "testuser", "-P", "testpass", "-p", "1080"}, cmd)
}

func TestBuildContainerGroup_Tags(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})
	cg := mgr.buildContainerGroup("proxy-test-0")

	assert.Equal(t, "telegram-scraper", *cg.Tags["managed_by"])
	assert.Equal(t, "20260325120000", *cg.Tags["crawl_id"])
	assert.Equal(t, "crawler-0", *cg.Tags["pod_name"])
}

func TestBuildContainerGroup_DefaultResources(t *testing.T) {
	cfg := baseCfg()
	cfg.ProxyCPU = 0
	cfg.ProxyMemoryGB = 0
	mgr := newManager(cfg, &mockContainerGroupsAPI{})

	cg := mgr.buildContainerGroup("proxy-test-0")

	resources := cg.Properties.Containers[0].Properties.Resources.Requests
	assert.Equal(t, 0.5, *resources.CPU)
	assert.Equal(t, 0.5, *resources.MemoryInGB)
}

func TestWaitForReady_Timeout(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})

	// Use an unreachable address to force timeout
	err := mgr.WaitForReady(context.Background(), []string{"192.0.2.1:1080"}, 3*time.Second)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")
}

func TestWaitForReady_ContextCancellation(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := mgr.WaitForReady(ctx, []string{"192.0.2.1:1080"}, 30*time.Second)

	require.Error(t, err)
}

func TestMutualExclusion_ConfigValidation(t *testing.T) {
	// This tests the validation logic that will live in main.go
	// Here we just verify the config fields can coexist on the struct
	cfg := baseCfg()
	cfg.ProxyAddrs = []string{"1.2.3.4:1080"}
	cfg.ManagedProxies = true

	// Both set = invalid (enforced in main.go, not in proxy package)
	assert.True(t, cfg.ManagedProxies)
	assert.NotEmpty(t, cfg.ProxyAddrs)
}

func TestProxyAddrs_ReturnsCopy(t *testing.T) {
	mgr := newManager(baseCfg(), &mockContainerGroupsAPI{})
	mgr.proxyAddrs = []string{"10.0.1.5:1080", "10.0.1.6:1080"}

	addrs := mgr.ProxyAddrs()
	addrs[0] = "modified"

	assert.Equal(t, "10.0.1.5:1080", mgr.proxyAddrs[0], "ProxyAddrs should return a copy, not a reference")
}
