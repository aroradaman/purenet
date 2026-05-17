//go:build linux

package nodesync

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/vishvananda/netlink"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// hostGatewayController implements NodeSyncer using direct host routes.
// For each remote node it programs:
//
//	ip route add <node.spec.podCIDR> via <node.status.addresses[InternalIP]>
//
// This requires all nodes to be L2-adjacent (same subnet).
type hostGatewayController struct {
	nodeName    string
	client      kubernetes.Interface
	cniConfPath string

	mu     sync.Mutex
	routes map[string]nodeRoute // nodeName → currently-programmed route
}

func newHostGateway(nodeName string, client kubernetes.Interface, cniConfPath string) *hostGatewayController {
	return &hostGatewayController{
		nodeName:    nodeName,
		client:      client,
		cniConfPath: cniConfPath,
		routes:      make(map[string]nodeRoute),
	}
}

// Run starts the node informer, waits for the cache to sync, then returns the
// own node's pod CIDR. The informer continues watching for node changes in the
// background until ctx is cancelled.
func (c *hostGatewayController) Run(ctx context.Context) (ownPodCIDR string, err error) {
	factory := informers.NewSharedInformerFactory(c.client, 0)
	nodeInformer := factory.Core().V1().Nodes().Informer()

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

	klog.InfoS("Node sync running",
		"ownNode", c.nodeName,
		"ownPodCIDR", ownNode.Spec.PodCIDR,
		"mode", "host-gateway",
	)
	return ownNode.Spec.PodCIDR, nil
}

// WriteCNIConfig writes the per-node CNI config.
func (c *hostGatewayController) WriteCNIConfig() error {
	return writeCNIConfig(c.cniConfPath)
}

// ── event handlers ────────────────────────────────────────────────────────────

func (c *hostGatewayController) onAdd(node *corev1.Node) {
	if node.Name == c.nodeName {
		return
	}
	podCIDR := node.Spec.PodCIDR
	nodeIP := internalIP(node)
	if podCIDR == "" || nodeIP == "" {
		klog.V(4).InfoS("Node add: missing CIDR or IP, skipping",
			"node", node.Name, "podCIDR", podCIDR, "nodeIP", nodeIP)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := addRoute(podCIDR, nodeIP); err != nil {
		klog.ErrorS(err, "Failed to add route",
			"node", node.Name, "podCIDR", podCIDR, "via", nodeIP)
		return
	}
	c.routes[node.Name] = nodeRoute{podCIDR: podCIDR, nodeIP: nodeIP}
	klog.InfoS("Route added", "node", node.Name, "podCIDR", podCIDR, "via", nodeIP)
}

func (c *hostGatewayController) onUpdate(old, new *corev1.Node) {
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
			klog.V(2).InfoS("Stale route removed",
				"node", old.Name, "podCIDR", oldCIDR, "via", oldIP)
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

func (c *hostGatewayController) onDelete(node *corev1.Node) {
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
		klog.InfoS("Route removed (node deleted)",
			"node", node.Name, "podCIDR", r.podCIDR, "via", r.nodeIP)
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
