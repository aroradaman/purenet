//go:build linux

// Package nodesync watches Kubernetes Node objects and programs host
// networking so pods on this node can reach pods on every other node without
// NAT.
//
// Concrete forwarding strategies are plugged in via the NodeSyncer interface.
// Choose a backend at startup via New(); the rest of the agent is unaffected.
package nodesync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// Mode selects how inter-node pod traffic is forwarded.
type Mode string

const (
	// ModeHostGateway programs a direct host route via the remote node IP.
	// Requires all cluster nodes to be on the same L2 segment.
	ModeHostGateway Mode = "host-gateway"

	// ModeVXLAN builds a UDP overlay (VNI 42, port 4789) so nodes can
	// communicate across routed subnets without any L2 adjacency requirement.
	ModeVXLAN Mode = "vxlan"
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

// New returns the NodeSyncer for the requested mode.
func New(nodeName string, client kubernetes.Interface, cniConfPath string, mode Mode) NodeSyncer {
	switch mode {
	case ModeVXLAN:
		return newVXLAN(nodeName, client, cniConfPath)
	default:
		return newHostGateway(nodeName, client, cniConfPath)
	}
}

// ── shared types and helpers ──────────────────────────────────────────────────

// nodeRoute holds the currently-programmed state for a remote node.
// Both backends use the same struct to track what they installed in the kernel
// so they know what to remove on update/delete events.
type nodeRoute struct {
	podCIDR string
	nodeIP  string
}

func internalIP(node *corev1.Node) string {
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address
		}
	}
	return ""
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
