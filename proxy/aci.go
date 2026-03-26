package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/rs/zerolog/log"
)

// ContainerGroupsAPI abstracts the Azure SDK container groups client for testability.
type ContainerGroupsAPI interface {
	BeginCreateOrUpdate(ctx context.Context, resourceGroupName string, containerGroupName string, containerGroup armcontainerinstance.ContainerGroup, options *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error)
	BeginDelete(ctx context.Context, resourceGroupName string, containerGroupName string, options *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error)
	ListByResourceGroup(ctx context.Context, resourceGroupName string) ([]*armcontainerinstance.ContainerGroup, error)
}

// PollerResponse wraps the result of a create-or-update long-running operation.
type PollerResponse struct {
	ContainerGroup armcontainerinstance.ContainerGroup
}

// DeletePollerResponse wraps the result of a delete long-running operation.
type DeletePollerResponse struct{}

// azureContainerGroupsClient wraps the real Azure SDK client.
type azureContainerGroupsClient struct {
	inner *armcontainerinstance.ContainerGroupsClient
}

func (c *azureContainerGroupsClient) BeginCreateOrUpdate(ctx context.Context, rg, name string, cg armcontainerinstance.ContainerGroup, opts *armcontainerinstance.ContainerGroupsClientBeginCreateOrUpdateOptions) (*PollerResponse, error) {
	poller, err := c.inner.BeginCreateOrUpdate(ctx, rg, name, cg, opts)
	if err != nil {
		return nil, err
	}
	resp, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &PollerResponse{ContainerGroup: resp.ContainerGroup}, nil
}

func (c *azureContainerGroupsClient) BeginDelete(ctx context.Context, rg, name string, opts *armcontainerinstance.ContainerGroupsClientBeginDeleteOptions) (*DeletePollerResponse, error) {
	poller, err := c.inner.BeginDelete(ctx, rg, name, opts)
	if err != nil {
		return nil, err
	}
	_, err = poller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &DeletePollerResponse{}, nil
}

func (c *azureContainerGroupsClient) ListByResourceGroup(ctx context.Context, rg string) ([]*armcontainerinstance.ContainerGroup, error) {
	pager := c.inner.NewListByResourceGroupPager(rg, nil)
	var result []*armcontainerinstance.ContainerGroup
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Value...)
	}
	return result, nil
}

// ACIProxyManager manages the lifecycle of Azure Container Instance SOCKS5 proxies.
type ACIProxyManager struct {
	client        ContainerGroupsAPI
	resourceGroup string
	location      string
	image         string
	cpu           float64
	memoryGB      float64
	port          int
	proxyUser     string
	proxyPass     string
	crawlID       string
	podName       string
	proxyCount    int

	mu             sync.Mutex
	containerNames []string // names of ACIs this manager created
	proxyAddrs     []string // resolved ip:port for each ACI
}

// NewACIProxyManager creates a new manager using DefaultAzureCredential (managed identity
// in AKS, CLI credentials for local development).
func NewACIProxyManager(cfg common.CrawlerConfig, subscriptionID string) (*ACIProxyManager, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("azure credential: %w", err)
	}
	client, err := armcontainerinstance.NewContainerGroupsClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("container groups client: %w", err)
	}
	return newManager(cfg, &azureContainerGroupsClient{inner: client}), nil
}

// newManager is the internal constructor shared by production and test code.
func newManager(cfg common.CrawlerConfig, api ContainerGroupsAPI) *ACIProxyManager {
	count := cfg.ProxyCount
	if count < 1 {
		count = 1
	}
	return &ACIProxyManager{
		client:        api,
		resourceGroup: cfg.ProxyResourceGroup,
		location:      cfg.ProxyLocation,
		image:         cfg.ProxyImage,
		cpu:           cfg.ProxyCPU,
		memoryGB:      cfg.ProxyMemoryGB,
		port:          cfg.ProxyPort,
		proxyUser:     cfg.ProxyUser,
		proxyPass:     cfg.ProxyPass,
		crawlID:       cfg.CrawlID,
		podName:       cfg.PodName,
		proxyCount:    count,
	}
}

// containerGroupName generates a DNS-safe name for the ACI container group.
// Format: proxy-{podName}-{ordinal}, truncated to 63 characters.
// Using podName (rather than crawlID) ensures each pod in a statefulset
// gets its own proxy without colliding with other pods' proxies.
func containerGroupName(podName string, ordinal int) string {
	name := fmt.Sprintf("proxy-%s-%d", strings.ToLower(podName), ordinal)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// buildContainerGroup constructs the ACI container group spec.
func (m *ACIProxyManager) buildContainerGroup(name string) armcontainerinstance.ContainerGroup {
	portStr := strconv.Itoa(int(m.port))
	cpu := m.cpu
	if cpu <= 0 {
		cpu = 0.5
	}
	mem := m.memoryGB
	if mem <= 0 {
		mem = 0.5
	}

	cg := armcontainerinstance.ContainerGroup{
		Location: to.Ptr(m.location),
		Tags: map[string]*string{
			"managed_by": to.Ptr("telegram-scraper"),
			"crawl_id":   to.Ptr(m.crawlID),
			"pod_name":   to.Ptr(m.podName),
		},
		Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
			OSType:        to.Ptr(armcontainerinstance.OperatingSystemTypesLinux),
			RestartPolicy: to.Ptr(armcontainerinstance.ContainerGroupRestartPolicyAlways),
			Containers: []*armcontainerinstance.Container{
				{
					Name: to.Ptr("microsocks"),
					Properties: &armcontainerinstance.ContainerProperties{
						Image:   to.Ptr(m.image),
						Command: []*string{to.Ptr("microsocks"), to.Ptr("-u"), to.Ptr(m.proxyUser), to.Ptr("-P"), to.Ptr(m.proxyPass), to.Ptr("-p"), to.Ptr(portStr)},
						Ports: []*armcontainerinstance.ContainerPort{
							{Port: to.Ptr(int32(m.port)), Protocol: to.Ptr(armcontainerinstance.ContainerNetworkProtocolTCP)},
						},
						Resources: &armcontainerinstance.ResourceRequirements{
							Requests: &armcontainerinstance.ResourceRequests{
								CPU:        to.Ptr(cpu),
								MemoryInGB: to.Ptr(mem),
							},
						},
					},
				},
			},
			IPAddress: &armcontainerinstance.IPAddress{
				Ports: []*armcontainerinstance.Port{
					{Port: to.Ptr(int32(m.port)), Protocol: to.Ptr(armcontainerinstance.ContainerGroupNetworkProtocolTCP)},
				},
			},
		},
	}

	cg.Properties.IPAddress.Type = to.Ptr(armcontainerinstance.ContainerGroupIPAddressTypePublic)

	return cg
}

// CreateProxies creates the configured number of ACI container groups in parallel.
// Returns a slice of proxy addresses (ip:port) ready for use.
func (m *ACIProxyManager) CreateProxies(ctx context.Context) ([]string, error) {
	type result struct {
		ordinal int
		addr    string
		err     error
	}

	results := make(chan result, m.proxyCount)
	for i := 0; i < m.proxyCount; i++ {
		go func(ordinal int) {
			name := containerGroupName(m.podName, ordinal)
			cg := m.buildContainerGroup(name)

			log.Info().Str("name", name).Str("location", m.location).Msg("Creating ACI proxy")

			resp, err := m.client.BeginCreateOrUpdate(ctx, m.resourceGroup, name, cg, nil)
			if err != nil {
				results <- result{ordinal: ordinal, err: fmt.Errorf("create ACI %s: %w", name, err)}
				return
			}

			ip := ""
			if resp.ContainerGroup.Properties != nil && resp.ContainerGroup.Properties.IPAddress != nil && resp.ContainerGroup.Properties.IPAddress.IP != nil {
				ip = *resp.ContainerGroup.Properties.IPAddress.IP
			}
			if ip == "" {
				results <- result{ordinal: ordinal, err: fmt.Errorf("ACI %s has no IP address after provisioning", name)}
				return
			}

			addr := fmt.Sprintf("%s:%d", ip, m.port)
			log.Info().Str("name", name).Str("addr", addr).Msg("ACI proxy provisioned")
			results <- result{ordinal: ordinal, addr: addr}
		}(i)
	}

	addrs := make([]string, m.proxyCount)
	var names []string
	var errs []error
	for i := 0; i < m.proxyCount; i++ {
		r := <-results
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		addrs[r.ordinal] = r.addr
		names = append(names, containerGroupName(m.podName, r.ordinal))
	}

	if len(errs) > 0 {
		// Best-effort cleanup of any successfully created ACIs
		m.mu.Lock()
		m.containerNames = names
		m.mu.Unlock()
		_ = m.DestroyProxies(ctx)
		return nil, fmt.Errorf("failed to create %d/%d proxies: %v", len(errs), m.proxyCount, errs)
	}

	m.mu.Lock()
	m.containerNames = names
	m.proxyAddrs = addrs
	m.mu.Unlock()

	return addrs, nil
}

// WaitForReady polls each proxy address with a TCP check until all respond or timeout.
func (m *ACIProxyManager) WaitForReady(ctx context.Context, addrs []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, addr := range addrs {
		for {
			if time.Now().After(deadline) {
				return fmt.Errorf("proxy %s not ready after %v", addr, timeout)
			}
			if err := common.CheckProxyTCP(addr, 3*time.Second); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	return nil
}

// DestroyProxies deletes all ACI container groups created by this manager.
// Errors are logged but not fatal — this is best-effort cleanup.
func (m *ACIProxyManager) DestroyProxies(ctx context.Context) error {
	m.mu.Lock()
	names := make([]string, len(m.containerNames))
	copy(names, m.containerNames)
	m.mu.Unlock()

	if len(names) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, name := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			log.Info().Str("name", n).Msg("Deleting ACI proxy")
			_, err := m.client.BeginDelete(ctx, m.resourceGroup, n, nil)
			if err != nil {
				log.Warn().Err(err).Str("name", n).Msg("Failed to delete ACI proxy")
				mu.Lock()
				errs = append(errs, fmt.Errorf("delete %s: %w", n, err))
				mu.Unlock()
			}
		}(name)
	}
	wg.Wait()

	m.mu.Lock()
	m.containerNames = nil
	m.proxyAddrs = nil
	m.mu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("partial proxy cleanup failure: %v", errs)
	}
	return nil
}

// CleanupOrphanedProxies finds and deletes any ACI container groups tagged with
// managed_by=telegram-scraper and this pod's name. This recovers from crashes
// where DestroyProxies was never called, without touching other pods' proxies.
func (m *ACIProxyManager) CleanupOrphanedProxies(ctx context.Context) error {
	groups, err := m.client.ListByResourceGroup(ctx, m.resourceGroup)
	if err != nil {
		return fmt.Errorf("list container groups: %w", err)
	}

	var orphans []string
	for _, g := range groups {
		if g.Tags == nil || g.Name == nil {
			continue
		}
		managedBy := g.Tags["managed_by"]
		pn := g.Tags["pod_name"]
		if managedBy != nil && pn != nil && *managedBy == "telegram-scraper" && *pn == m.podName {
			orphans = append(orphans, *g.Name)
		}
	}

	if len(orphans) == 0 {
		return nil
	}

	log.Warn().Int("count", len(orphans)).Strs("names", orphans).Msg("Cleaning up orphaned ACI proxies")

	var wg sync.WaitGroup
	for _, name := range orphans {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			_, err := m.client.BeginDelete(ctx, m.resourceGroup, n, nil)
			if err != nil {
				log.Warn().Err(err).Str("name", n).Msg("Failed to delete orphaned ACI proxy")
			}
		}(name)
	}
	wg.Wait()

	return nil
}

// ProxyAddrs returns the resolved proxy addresses after CreateProxies completes.
func (m *ACIProxyManager) ProxyAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.proxyAddrs))
	copy(out, m.proxyAddrs)
	return out
}
