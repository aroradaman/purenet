//go:build linux

package nodesync

import (
	"context"
	"fmt"
	"net"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/aroradaman/purenet/pkg/network"
)

// vxlanController implements NodeSyncer using a VXLAN overlay (VNI 42).
//
// For each remote node it programs three kernel objects:
//
//  1. Route:  <podCIDR> via <vtepIP> dev purenet-gw onlink
//  2. Neigh:  <vtepIP>  → <vtepMAC>  (permanent ARP)
//  3. FDB:    <vtepMAC> → <nodeIP>   (permanent bridge FDB)
//
// MAC addresses are derived deterministically from pod CIDR network addresses
// so no out-of-band signalling or node annotations are required.
//
// Run() creates the local purenet-gw interface before registering event
// handlers, which guarantees AddVXLANPeer can always find the interface.
type vxlanController struct {
	nodeName    string
	client      kubernetes.Interface
	cniConfPath string

	mu    sync.Mutex
	peers map[string]nodeRoute // nodeName → currently-programmed peer
}

func newVXLAN(nodeName string, client kubernetes.Interface, cniConfPath string) *vxlanController {
	return &vxlanController{
		nodeName:    nodeName,
		client:      client,
		cniConfPath: cniConfPath,
		peers:       make(map[string]nodeRoute),
	}
}

// Run syncs the node cache, creates the purenet-gw VTEP, then registers event
// handlers. client-go calls AddFunc for every object already in the store when
// handlers are registered after cache sync, so no nodes are missed.
func (c *vxlanController) Run(ctx context.Context) (ownPodCIDR string, err error) {
	factory := informers.NewSharedInformerFactory(c.client, 0)
	nodeInformer := factory.Core().V1().Nodes().Informer()

	// Start without handlers so the store populates before we touch it.
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return "", fmt.Errorf("timed out waiting for node informer cache sync")
	}

	item, exists, err := nodeInformer.GetStore().GetByKey(c.nodeName)
	if err != nil {
		return "", fmt.Errorf("looking up own node %q: %w", c.nodeName, err)
	}
	if !exists {
		return "", fmt.Errorf("own node %q not found in cache", c.nodeName)
	}
	ownNode := item.(*corev1.Node)
	if ownNode.Spec.PodCIDR == "" {
		return "", fmt.Errorf("node %q has no pod CIDR assigned; ensure --allocate-node-cidrs is enabled on kube-controller-manager", c.nodeName)
	}
	ownPodCIDR = ownNode.Spec.PodCIDR

	// Create the local VTEP before any peer events fire.
	_, cidr, err := net.ParseCIDR(ownPodCIDR)
	if err != nil {
		return "", fmt.Errorf("parse own pod CIDR %q: %w", ownPodCIDR, err)
	}
	if err := network.SetupVXLAN(cidr); err != nil {
		return "", fmt.Errorf("VXLAN setup: %w", err)
	}
	klog.InfoS("VXLAN interface ready",
		"iface", network.VXLANIfName,
		"vtepIP", cidr.IP,
		"vtepMAC", network.VTEPMAC(cidr.IP.To4()),
	)

	// Register handlers now. client-go calls AddFunc for all cached nodes so
	// the initial set of peers is programmed before Run returns.
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck
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

	klog.InfoS("Node sync running",
		"ownNode", c.nodeName,
		"ownPodCIDR", ownPodCIDR,
		"mode", ModeVXLAN,
	)
	return ownPodCIDR, nil
}

// WriteCNIConfig writes the per-node CNI config.
func (c *vxlanController) WriteCNIConfig() error {
	return writeCNIConfig(c.cniConfPath)
}

// ── event handlers ────────────────────────────────────────────────────────────

func (c *vxlanController) onAdd(node *corev1.Node) {
	if node.Name == c.nodeName {
		return
	}
	podCIDR := node.Spec.PodCIDR
	nodeIP := internalIP(node)
	if podCIDR == "" || nodeIP == "" {
		klog.V(4).InfoS("VXLAN node add: missing CIDR or IP, skipping",
			"node", node.Name, "podCIDR", podCIDR, "nodeIP", nodeIP)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.addPeer(podCIDR, nodeIP); err != nil {
		klog.ErrorS(err, "Failed to add VXLAN peer",
			"node", node.Name, "podCIDR", podCIDR, "nodeIP", nodeIP)
		return
	}
	c.peers[node.Name] = nodeRoute{podCIDR: podCIDR, nodeIP: nodeIP}
	klog.InfoS("VXLAN peer added", "node", node.Name, "podCIDR", podCIDR, "nodeIP", nodeIP)
}

func (c *vxlanController) onUpdate(old, new *corev1.Node) {
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
		if err := c.delPeer(oldCIDR, oldIP); err != nil {
			klog.ErrorS(err, "Failed to remove stale VXLAN peer on update", "node", old.Name)
		} else {
			klog.V(2).InfoS("Stale VXLAN peer removed",
				"node", old.Name, "podCIDR", oldCIDR, "nodeIP", oldIP)
		}
		delete(c.peers, old.Name)
	}
	if newCIDR != "" && newIP != "" {
		if err := c.addPeer(newCIDR, newIP); err != nil {
			klog.ErrorS(err, "Failed to add updated VXLAN peer", "node", new.Name)
			return
		}
		c.peers[new.Name] = nodeRoute{podCIDR: newCIDR, nodeIP: newIP}
		klog.InfoS("VXLAN peer updated", "node", new.Name, "podCIDR", newCIDR, "nodeIP", newIP)
	}
}

func (c *vxlanController) onDelete(node *corev1.Node) {
	if node.Name == c.nodeName {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.peers[node.Name]
	if !ok {
		return
	}
	if err := c.delPeer(r.podCIDR, r.nodeIP); err != nil {
		klog.ErrorS(err, "Failed to remove VXLAN peer for deleted node", "node", node.Name)
	} else {
		klog.InfoS("VXLAN peer removed (node deleted)",
			"node", node.Name, "podCIDR", r.podCIDR, "nodeIP", r.nodeIP)
	}
	delete(c.peers, node.Name)
}

// ── peer helpers ──────────────────────────────────────────────────────────────

func (c *vxlanController) addPeer(podCIDR, nodeIP string) error {
	_, cidr, err := net.ParseCIDR(podCIDR)
	if err != nil {
		return fmt.Errorf("invalid pod CIDR %q: %w", podCIDR, err)
	}
	ip := net.ParseIP(nodeIP)
	if ip == nil {
		return fmt.Errorf("invalid node IP %q", nodeIP)
	}
	return network.AddVXLANPeer(cidr, ip)
}

func (c *vxlanController) delPeer(podCIDR, nodeIP string) error {
	_, cidr, err := net.ParseCIDR(podCIDR)
	if err != nil {
		return fmt.Errorf("invalid pod CIDR %q: %w", podCIDR, err)
	}
	ip := net.ParseIP(nodeIP)
	if ip == nil {
		return fmt.Errorf("invalid node IP %q", nodeIP)
	}
	return network.DelVXLANPeer(cidr, ip)
}
