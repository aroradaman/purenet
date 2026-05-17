//go:build linux

// Package nodesync watches Kubernetes Node objects and programs host
// networking so pods on this node can reach pods on every other node without
// NAT.
//
// Concrete forwarding strategies are plugged in via the NodeSyncer interface,
// making it straightforward to add new backends (e.g. VXLAN overlay) without
// touching the agent startup code.
package nodesync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
)

// NodeSyncer watches Kubernetes Node objects and maintains inter-node
// forwarding state on the host.
type NodeSyncer interface {
	// Run starts watching nodes and returns the own node's pod CIDR once the
	// initial cache sync completes. It continues running in the background
	// until ctx is cancelled.
	Run(ctx context.Context) (ownPodCIDR string, err error)

	// WriteCNIConfig writes the per-node CNI config to the filesystem path
	// supplied at construction time.
	WriteCNIConfig() error
}

// New returns a NodeSyncer backed by direct host routes (host-gateway mode).
// Each remote node gets an "ip route add <podCIDR> via <nodeIP>" entry.
// All cluster nodes must share an L2 segment for this to work.
func New(nodeName string, client kubernetes.Interface, cniConfPath string) NodeSyncer {
	return newHostGateway(nodeName, client, cniConfPath)
}

// writeCNIConfig writes a minimal purenet CNI config to path. There is no
// IPAM section because IP allocation is handled in-process by the agent.
// Shared by all NodeSyncer implementations.
func writeCNIConfig(path string) error {
	type cniConf struct {
		CNIVersion string `json:"cniVersion"`
		Name       string `json:"name"`
		Type       string `json:"type"`
		MTU        int    `json:"mtu"`
	}

	data, err := json.MarshalIndent(cniConf{
		CNIVersion: "1.0.0",
		Name:       "purenet",
		Type:       "purenet",
		MTU:        1500,
	}, "", "  ")
	if err != nil {
		return err
	}

	// Atomic write: write to a temp file then rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing CNI config tmp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming CNI config: %w", err)
	}
	return nil
}
