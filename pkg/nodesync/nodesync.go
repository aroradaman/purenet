//go:build linux

// Package nodesync watches Kubernetes Node objects and programs host routes
// so that pods on this node can reach pods on every other node without NAT.
//
// For each remote node it maintains:
//
//	ip route add <node.spec.podCIDR> via <node.status.addresses[InternalIP]>
//
// It also writes a minimal per-node CNI config to the host filesystem.
// IP allocation itself is handled in-process by the agent's IPAM allocator.
package nodesync

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/vishvananda/netlink"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Controller watches nodes and maintains the inter-node route table.
type Controller struct {
	nodeName    string
	client      kubernetes.Interface
	cniConfPath string // path on the host to write the per-node CNI config

	mu     sync.Mutex
	routes map[string]nodeRoute // nodeName → current programmed route
}

type nodeRoute struct {
	podCIDR string
	nodeIP  string
}

// New creates a Controller.
//
//   - nodeName: own Kubernetes node name (typically from spec.nodeName)
//   - client: in-cluster Kubernetes client
//   - cniConfPath: absolute path (inside the container) where the per-node
//     CNI config should be written (e.g. /host/etc/cni/net.d/10-purenet.conf)
func New(nodeName string, client kubernetes.Interface, cniConfPath string) *Controller {
	return &Controller{
		nodeName:    nodeName,
		client:      client,
		cniConfPath: cniConfPath,
		routes:      make(map[string]nodeRoute),
	}
}

// Run starts the node informer, waits for the cache to sync, then runs until
// ctx is cancelled. It returns the own node's pod CIDR so the caller can
// configure IPAM and set up the gRPC server.
func (c *Controller) Run(ctx context.Context) (ownPodCIDR string, err error) {
	factory := informers.NewSharedInformerFactory(c.client, 0)
	nodeInformer := factory.Core().V1().Nodes().Informer()

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				c.onAdd(node)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			oldNode, ok1 := old.(*corev1.Node)
			newNode, ok2 := new.(*corev1.Node)
			if ok1 && ok2 {
				c.onUpdate(oldNode, newNode)
			}
		},
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				// Tombstone
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				node, ok = tombstone.Obj.(*corev1.Node)
				if !ok {
					return
				}
			}
			c.onDelete(node)
		},
	})

	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return "", fmt.Errorf("timed out waiting for node informer cache sync")
	}

	// Retrieve own node's pod CIDR from the synced cache.
	// Node keys are just their names (cluster-scoped resource).
	item, exists, err := nodeInformer.GetStore().GetByKey(c.nodeName)
	if err != nil {
		return "", fmt.Errorf("error looking up own node %q: %w", c.nodeName, err)
	}
	if !exists {
		return "", fmt.Errorf("own node %q not found in cache", c.nodeName)
	}
	ownNode := item.(*corev1.Node)
	if ownNode.Spec.PodCIDR == "" {
		return "", fmt.Errorf("node %q has no pod CIDR assigned; ensure --cluster-cidr is set on kube-controller-manager", c.nodeName)
	}

	klog.InfoS("Node sync running",
		"ownNode", c.nodeName,
		"ownPodCIDR", ownNode.Spec.PodCIDR,
	)
	return ownNode.Spec.PodCIDR, nil
}

// WriteCNIConfig writes a minimal CNI config (no IPAM section — IP allocation
// is handled in-process by the agent) to the path given at construction time.
func (c *Controller) WriteCNIConfig() error {
	return writeCNIConfig(c.cniConfPath)
}

// ── event handlers ────────────────────────────────────────────────────────────

func (c *Controller) onAdd(node *corev1.Node) {
	if node.Name == c.nodeName {
		return
	}
	podCIDR := node.Spec.PodCIDR
	nodeIP := internalIP(node)
	if podCIDR == "" || nodeIP == "" {
		klog.V(4).InfoS("Node add: missing CIDR or IP, skipping", "node", node.Name, "podCIDR", podCIDR, "nodeIP", nodeIP)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := addRoute(podCIDR, nodeIP); err != nil {
		klog.ErrorS(err, "Failed to add route", "node", node.Name, "podCIDR", podCIDR, "via", nodeIP)
		return
	}
	c.routes[node.Name] = nodeRoute{podCIDR: podCIDR, nodeIP: nodeIP}
	klog.InfoS("Route added", "node", node.Name, "podCIDR", podCIDR, "via", nodeIP)
}

func (c *Controller) onUpdate(old, new *corev1.Node) {
	if new.Name == c.nodeName {
		return
	}
	oldCIDR, newCIDR := old.Spec.PodCIDR, new.Spec.PodCIDR
	oldIP, newIP := internalIP(old), internalIP(new)

	if oldCIDR == newCIDR && oldIP == newIP {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldCIDR != "" && oldIP != "" {
		if err := delRoute(oldCIDR, oldIP); err != nil {
			klog.ErrorS(err, "Failed to remove stale route on update", "node", old.Name)
		} else {
			klog.V(2).InfoS("Stale route removed", "node", old.Name, "podCIDR", oldCIDR, "via", oldIP)
		}
		delete(c.routes, old.Name)
	}
	if newCIDR != "" && newIP != "" {
		if err := addRoute(newCIDR, newIP); err != nil {
			klog.ErrorS(err, "Failed to add updated route", "node", new.Name)
			return
		}
		c.routes[new.Name] = nodeRoute{podCIDR: newCIDR, nodeIP: newIP}
		klog.InfoS("Route updated", "node", new.Name, "podCIDR", newCIDR, "via", newIP)
	}
}

func (c *Controller) onDelete(node *corev1.Node) {
	if node.Name == c.nodeName {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.routes[node.Name]
	if !ok {
		return
	}
	if err := delRoute(r.podCIDR, r.nodeIP); err != nil {
		klog.ErrorS(err, "Failed to remove route for deleted node", "node", node.Name)
	} else {
		klog.InfoS("Route removed (node deleted)", "node", node.Name, "podCIDR", r.podCIDR, "via", r.nodeIP)
	}
	delete(c.routes, node.Name)
}

// ── route helpers ─────────────────────────────────────────────────────────────

func addRoute(podCIDR, nodeIP string) error {
	_, dst, err := net.ParseCIDR(podCIDR)
	if err != nil {
		return fmt.Errorf("invalid pod CIDR %q: %w", podCIDR, err)
	}
	gw := net.ParseIP(nodeIP)
	if gw == nil {
		return fmt.Errorf("invalid node IP %q", nodeIP)
	}
	if err := netlink.RouteAdd(&netlink.Route{Dst: dst, Gw: gw}); err != nil {
		if isEEXIST(err) {
			return nil
		}
		return err
	}
	return nil
}

func delRoute(podCIDR, nodeIP string) error {
	_, dst, err := net.ParseCIDR(podCIDR)
	if err != nil {
		return fmt.Errorf("invalid pod CIDR %q: %w", podCIDR, err)
	}
	gw := net.ParseIP(nodeIP)
	if gw == nil {
		return fmt.Errorf("invalid node IP %q", nodeIP)
	}
	if err := netlink.RouteDel(&netlink.Route{Dst: dst, Gw: gw}); err != nil {
		if isENOENT(err) {
			return nil
		}
		return err
	}
	return nil
}

func isEEXIST(err error) bool { return err != nil && err.Error() == "file exists" }
func isENOENT(err error) bool { return err != nil && err.Error() == "no such process" }

func internalIP(node *corev1.Node) string {
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address
		}
	}
	return ""
}

// ── CNI config writer ─────────────────────────────────────────────────────────

// writeCNIConfig writes a minimal purenet CNI config to path. There is no IPAM
// section because IP allocation is handled in-process by the agent.
func writeCNIConfig(path string) error {
	type cniConf struct {
		CNIVersion string `json:"cniVersion"`
		Name       string `json:"name"`
		Type       string `json:"type"`
		MTU        int    `json:"mtu"`
	}

	conf := cniConf{
		CNIVersion: "1.0.0",
		Name:       "purenet",
		Type:       "purenet",
		MTU:        1500,
	}

	data, err := json.MarshalIndent(conf, "", "  ")
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
