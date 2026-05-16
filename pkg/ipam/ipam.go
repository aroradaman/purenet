// Package ipam implements an in-process IP address manager for a single pod
// CIDR. It replaces the fork of the external host-local binary, removing
// subprocess overhead on every pod ADD/DEL/CHECK.
//
// Allocations are kept in memory for speed and persisted to a CBOR file on
// every change so the agent survives restarts without double-allocating IPs.
package ipam

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"k8s.io/klog/v2"
)

const stateFilename = "allocations.cbor"

// Allocator manages IP addresses within a single pod CIDR.
type Allocator struct {
	mu      sync.Mutex
	subnet  *net.IPNet
	gateway net.IP
	dataDir string

	// allocs maps containerID → allocated IP.
	allocs map[string]net.IP
	// used maps ip.String() → containerID (reverse index for O(1) conflict checks).
	used map[string]string
}

// New creates an Allocator for podCIDR, restoring any existing allocations
// from dataDir/allocations.cbor.
func New(podCIDR, dataDir string) (*Allocator, error) {
	_, subnet, err := net.ParseCIDR(podCIDR)
	if err != nil {
		return nil, fmt.Errorf("invalid pod CIDR %q: %w", podCIDR, err)
	}

	a := &Allocator{
		subnet:  subnet,
		gateway: firstHostIP(subnet),
		dataDir: dataDir,
		allocs:  make(map[string]net.IP),
		used:    make(map[string]string),
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create IPAM data dir %q: %w", dataDir, err)
	}
	if err := a.load(); err != nil {
		return nil, err
	}
	return a, nil
}

// Subnet returns the pod CIDR this allocator manages.
func (a *Allocator) Subnet() *net.IPNet { return a.subnet }

// Gateway returns the gateway IP for the pod CIDR.
func (a *Allocator) Gateway() net.IP { return a.gateway }

// Add allocates an IP for containerID. If containerID already has an
// allocation (kubelet retry), the same IP and gateway are returned.
func (a *Allocator) Add(containerID string) (podIP, gateway net.IP, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if ip, ok := a.allocs[containerID]; ok {
		klog.V(4).InfoS("IPAM: returning existing allocation",
			"containerID", shortID(containerID), "ip", ip)
		return ip, a.gateway, nil
	}

	ip, err := a.nextFreeIP()
	if err != nil {
		return nil, nil, err
	}

	a.allocs[containerID] = ip
	a.used[ip.String()] = containerID

	if err := a.persist(); err != nil {
		// Roll back in-memory state so it stays consistent with disk.
		delete(a.allocs, containerID)
		delete(a.used, ip.String())
		return nil, nil, fmt.Errorf("persist allocation: %w", err)
	}

	klog.V(4).InfoS("IPAM: allocated",
		"containerID", shortID(containerID), "ip", ip, "gateway", a.gateway)
	return ip, a.gateway, nil
}

// Del releases the IP held by containerID. It is idempotent: releasing an
// already-released or never-allocated container is a no-op.
func (a *Allocator) Del(containerID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	ip, ok := a.allocs[containerID]
	if !ok {
		return nil
	}

	delete(a.allocs, containerID)
	delete(a.used, ip.String())

	if err := a.persist(); err != nil {
		// Re-insert so in-memory and on-disk state stay consistent.
		a.allocs[containerID] = ip
		a.used[ip.String()] = containerID
		return fmt.Errorf("persist release: %w", err)
	}

	klog.V(4).InfoS("IPAM: released", "containerID", shortID(containerID), "ip", ip)
	return nil
}

// Check returns the IP allocated to containerID, or an error if no allocation
// exists.
func (a *Allocator) Check(containerID string) (net.IP, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	ip, ok := a.allocs[containerID]
	if !ok {
		return nil, fmt.Errorf("no allocation for container %s", containerID)
	}
	return ip, nil
}

// nextFreeIP scans the usable host range and returns the first unallocated IP.
// Skipped addresses: network address (offset 0), gateway (offset 1), and
// broadcast (last address in the subnet).
// Must be called with a.mu held.
func (a *Allocator) nextFreeIP() (net.IP, error) {
	base := a.subnet.IP.To4()
	if base == nil {
		return nil, fmt.Errorf("only IPv4 subnets are supported")
	}

	ones, bits := a.subnet.Mask.Size()
	size := 1 << uint(bits-ones)
	baseN := ipToUint32(base)

	for offset := 2; offset < size-1; offset++ {
		ip := uint32ToIP(baseN + uint32(offset))
		if _, taken := a.used[ip.String()]; !taken {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("subnet %s is exhausted", a.subnet)
}

// ── persistence ───────────────────────────────────────────────────────────────

// persist encodes the current allocations as CBOR and atomically overwrites
// the state file. Must be called with a.mu held.
func (a *Allocator) persist() error {
	// Encode as ip→containerID so the file is easy to inspect with a CBOR
	// viewer (cbor.me, cbordiag, etc.).
	m := make(map[string]string, len(a.allocs))
	for cid, ip := range a.allocs {
		m[ip.String()] = cid
	}

	data, err := cbor.Marshal(m)
	if err != nil {
		return fmt.Errorf("cbor marshal: %w", err)
	}

	path := a.statePath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write tmp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename state file: %w", err)
	}
	return nil
}

// load restores allocations from the state file. Entries that are outside
// the current subnet are silently skipped (e.g. after a pod CIDR reassignment).
// Called once during New.
func (a *Allocator) load() error {
	path := a.statePath()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read IPAM state file: %w", err)
	}

	var m map[string]string // ip → containerID
	if err := cbor.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("decode IPAM state file: %w", err)
	}

	for ipStr, cid := range m {
		ip := net.ParseIP(ipStr).To4()
		if ip == nil || !a.subnet.Contains(ip) {
			klog.Warningf("IPAM: skipping stale entry %s→%s from state file", ipStr, cid)
			continue
		}
		a.allocs[cid] = ip
		a.used[ipStr] = cid
	}
	klog.V(2).InfoS("IPAM state loaded", "count", len(a.allocs), "file", path)
	return nil
}

func (a *Allocator) statePath() string {
	return a.dataDir + "/" + stateFilename
}

// ── helpers ───────────────────────────────────────────────────────────────────

// firstHostIP returns the first usable host address in the subnet (network
// address + 1), used as the pod default gateway.
func firstHostIP(subnet *net.IPNet) net.IP {
	ip := make(net.IP, len(subnet.IP))
	copy(ip, subnet.IP)
	ip[len(ip)-1]++
	return ip
}

func ipToUint32(ip net.IP) uint32 {
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

func uint32ToIP(n uint32) net.IP {
	return net.IP{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
