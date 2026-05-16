//go:build linux

// Package agent implements the CNIBackendServer gRPC interface.  All actual
// network programming lives here; the CNI binary shim is just a thin client.
package agent

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/vishvananda/netlink"
	"k8s.io/klog/v2"

	"github.com/aroradaman/purenet/pkg/cni"
	"github.com/aroradaman/purenet/pkg/config"
	"github.com/aroradaman/purenet/pkg/ipam"
	"github.com/aroradaman/purenet/pkg/network"
)

// Server implements cni.CNIBackendServer.
type Server struct {
	cni.UnimplementedCNIBackendServer
	// containerAccess serialises concurrent gRPC calls for the same container
	// ID, preventing races between ADD and DEL for the same container.
	containerAccess *containerAccessArbitrator
	alloc           *ipam.Allocator
}

// NewServer creates a Server ready to serve gRPC requests.
func NewServer(alloc *ipam.Allocator) *Server {
	return &Server{
		containerAccess: newContainerAccessArbitrator(),
		alloc:           alloc,
	}
}

// containerAccessArbitrator ensures at most one in-flight RPC per container ID.
type containerAccessArbitrator struct {
	mu   sync.Mutex
	cond *sync.Cond
	busy map[string]bool
}

func newContainerAccessArbitrator() *containerAccessArbitrator {
	a := &containerAccessArbitrator{busy: make(map[string]bool)}
	a.cond = sync.NewCond(&a.mu)
	return a
}

func (a *containerAccessArbitrator) lock(containerID string) {
	a.cond.L.Lock()
	defer a.cond.L.Unlock()
	for a.busy[containerID] {
		a.cond.Wait()
	}
	a.busy[containerID] = true
}

func (a *containerAccessArbitrator) unlock(containerID string) {
	a.cond.L.Lock()
	defer a.cond.L.Unlock()
	delete(a.busy, containerID)
	a.cond.Broadcast()
}

// ── gRPC handlers ─────────────────────────────────────────────────────────────

func (s *Server) CmdAdd(_ context.Context, req *cni.CNIRequest) (*cni.CNIResponse, error) {
	klog.V(2).InfoS("CmdAdd start",
		"containerID", shortID(req.ContainerID),
		"netns", req.Netns,
		"ifName", req.IfName,
	)
	klog.V(4).InfoS("CmdAdd full args",
		"containerID", req.ContainerID,
		"netns", req.Netns,
		"ifName", req.IfName,
		"args", req.Args,
		"path", req.Path,
		"configLen", len(req.Config),
	)

	s.containerAccess.lock(req.ContainerID)
	defer s.containerAccess.unlock(req.ContainerID)

	result, err := s.cmdAdd(req)
	if err != nil {
		klog.ErrorS(err, "CmdAdd failed", "containerID", shortID(req.ContainerID))
		return cni.ErrorResponse(cni.ErrorCodeConfigInterfaceFailure, err), nil
	}

	var buf bytes.Buffer
	if err := result.PrintTo(&buf); err != nil {
		return nil, fmt.Errorf("failed to serialise CNI result: %w", err)
	}

	klog.V(2).InfoS("CmdAdd complete",
		"containerID", shortID(req.ContainerID),
		"ips", result.IPs,
	)
	return &cni.CNIResponse{Result: buf.Bytes()}, nil
}

func (s *Server) CmdDel(_ context.Context, req *cni.CNIRequest) (*cni.CNIResponse, error) {
	klog.V(2).InfoS("CmdDel start",
		"containerID", shortID(req.ContainerID),
		"netns", req.Netns,
		"ifName", req.IfName,
	)

	s.containerAccess.lock(req.ContainerID)
	defer s.containerAccess.unlock(req.ContainerID)

	if err := s.cmdDel(req); err != nil {
		klog.ErrorS(err, "CmdDel failed", "containerID", shortID(req.ContainerID))
		return cni.ErrorResponse(cni.ErrorCodeConfigInterfaceFailure, err), nil
	}

	klog.V(2).InfoS("CmdDel complete", "containerID", shortID(req.ContainerID))
	return &cni.CNIResponse{}, nil
}

func (s *Server) CmdCheck(_ context.Context, req *cni.CNIRequest) (*cni.CNIResponse, error) {
	klog.V(2).InfoS("CmdCheck start",
		"containerID", shortID(req.ContainerID),
		"netns", req.Netns,
		"ifName", req.IfName,
	)

	s.containerAccess.lock(req.ContainerID)
	defer s.containerAccess.unlock(req.ContainerID)

	if err := s.cmdCheck(req); err != nil {
		klog.ErrorS(err, "CmdCheck failed", "containerID", shortID(req.ContainerID))
		return cni.ErrorResponse(cni.ErrorCodeCheckInterfaceFailure, err), nil
	}

	klog.V(2).InfoS("CmdCheck passed", "containerID", shortID(req.ContainerID))
	return &cni.CNIResponse{}, nil
}

// ── internal implementation ───────────────────────────────────────────────────

func (s *Server) cmdAdd(req *cni.CNIRequest) (*current.Result, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	conf, err := config.LoadConf(req.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	klog.V(4).InfoS("Config loaded",
		"containerID", shortID(req.ContainerID),
		"cniVersion", conf.CNIVersion,
		"mtu", conf.MTU,
	)

	netNS, err := ns.GetNS(req.Netns)
	if err != nil {
		return nil, fmt.Errorf("failed to open netns %q: %w", req.Netns, err)
	}
	defer netNS.Close()
	klog.V(4).InfoS("Opened container netns",
		"containerID", shortID(req.ContainerID),
		"netns", req.Netns,
	)

	var hostIface, contIface *current.Interface

	if err := netNS.Do(func(hostNS ns.NetNS) error {
		hostLink, contLink, vethErr := network.SetupVeth(req.IfName, req.ContainerID, conf.MTU)
		if vethErr != nil {
			return vethErr
		}
		hostIface = &current.Interface{Name: hostLink.Attrs().Name}
		contIface = &current.Interface{Name: contLink.Attrs().Name, Sandbox: req.Netns}
		klog.V(4).InfoS("Veth pair created",
			"containerID", shortID(req.ContainerID),
			"hostVeth", hostIface.Name,
			"contVeth", contIface.Name,
			"mtu", conf.MTU,
		)

		if err := netlink.LinkSetNsFd(hostLink, int(hostNS.Fd())); err != nil {
			return fmt.Errorf("failed to move %q to host netns: %w", hostIface.Name, err)
		}
		klog.V(4).InfoS("Host veth moved to host netns",
			"containerID", shortID(req.ContainerID),
			"hostVeth", hostIface.Name,
		)
		return nil
	}); err != nil {
		return nil, err
	}

	hostLink, err := netlink.LinkByName(hostIface.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to find host veth %q: %w", hostIface.Name, err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %q: %w", hostIface.Name, err)
	}
	klog.V(4).InfoS("Host veth is up",
		"containerID", shortID(req.ContainerID),
		"hostVeth", hostIface.Name,
	)

	// Enable proxy ARP so the host responds to ARP requests for the pod's
	// default gateway (a virtual IP that lives nowhere on any interface).
	// Without this, outbound traffic from the pod stalls at ARP resolution.
	if err := network.EnableProxyARP(hostIface.Name); err != nil {
		return nil, err
	}
	klog.V(4).InfoS("Proxy ARP enabled", "containerID", shortID(req.ContainerID), "hostVeth", hostIface.Name)

	podIP, gw, err := s.alloc.Add(req.ContainerID)
	if err != nil {
		return nil, fmt.Errorf("IPAM add failed: %w", err)
	}
	klog.V(2).InfoS("IPAM allocated IP",
		"containerID", shortID(req.ContainerID),
		"ip", podIP,
		"gateway", gw,
	)

	_, defaultNet, _ := net.ParseCIDR("0.0.0.0/0")
	result := &current.Result{
		CNIVersion: conf.CNIVersion,
		Interfaces: []*current.Interface{hostIface, contIface},
		IPs: []*current.IPConfig{
			{
				Interface: current.Int(1),
				Address:   net.IPNet{IP: podIP, Mask: s.alloc.Subnet().Mask},
				Gateway:   gw,
			},
		},
		Routes: []*types.Route{
			{Dst: *defaultNet},
		},
	}

	// Add a /32 host route for each pod IP so the host kernel can reach the
	// pod directly through the veth.  Without this, kubelet health probes
	// and any host→pod traffic would be dropped (no route to 10.244.x.x).
	for _, ipc := range result.IPs {
		if err := network.SetupHostRoute(hostIface.Name, ipc.Address.IP); err != nil {
			return nil, err
		}
		klog.V(4).InfoS("Host route added",
			"containerID", shortID(req.ContainerID),
			"podIP", ipc.Address.IP,
			"hostVeth", hostIface.Name,
		)
	}

	if err := netNS.Do(func(_ ns.NetNS) error {
		for _, ipc := range result.IPs {
			addr := &netlink.Addr{IPNet: &ipc.Address}
			klog.V(4).InfoS("Assigning address",
				"containerID", shortID(req.ContainerID),
				"ifName", req.IfName,
				"addr", addr,
			)
			if err := network.ConfigureInterface(req.IfName, addr); err != nil {
				return err
			}
		}
		for _, route := range result.Routes {
			routeGW := route.GW
			if routeGW == nil && len(result.IPs) > 0 {
				routeGW = result.IPs[0].Gateway
			}
			if routeGW == nil {
				klog.V(4).InfoS("Skipping route — no gateway",
					"containerID", shortID(req.ContainerID),
					"dst", route.Dst,
				)
				continue
			}
			klog.V(4).InfoS("Adding route",
				"containerID", shortID(req.ContainerID),
				"dst", route.Dst,
				"gw", routeGW,
			)
			if routeErr := netlink.RouteAdd(&netlink.Route{Dst: &route.Dst, Gw: routeGW}); routeErr != nil {
				return fmt.Errorf("failed to add route %v via %v: %w", route.Dst, routeGW, routeErr)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	versioned, err := result.GetAsVersion(conf.CNIVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to convert result to version %s: %w", conf.CNIVersion, err)
	}
	typedResult, ok := versioned.(*current.Result)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T", versioned)
	}
	return typedResult, nil
}

func (s *Server) cmdDel(req *cni.CNIRequest) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := s.alloc.Del(req.ContainerID); err != nil {
		return fmt.Errorf("IPAM delete failed: %w", err)
	}
	klog.V(4).InfoS("IPAM del complete", "containerID", shortID(req.ContainerID))

	if req.Netns == "" {
		klog.V(4).InfoS("No netns provided, skipping veth teardown",
			"containerID", shortID(req.ContainerID),
		)
		return nil
	}

	netNS, err := ns.GetNS(req.Netns)
	if err != nil {
		klog.V(2).InfoS("Netns already gone, skipping veth teardown",
			"containerID", shortID(req.ContainerID),
			"netns", req.Netns,
		)
		return nil
	}
	defer netNS.Close()

	klog.V(4).InfoS("Deleting container veth",
		"containerID", shortID(req.ContainerID),
		"ifName", req.IfName,
	)
	return netNS.Do(func(_ ns.NetNS) error {
		return network.DeleteVeth(req.IfName)
	})
}

func (s *Server) cmdCheck(req *cni.CNIRequest) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if _, err := s.alloc.Check(req.ContainerID); err != nil {
		return fmt.Errorf("IPAM check failed: %w", err)
	}

	netNS, err := ns.GetNS(req.Netns)
	if err != nil {
		return fmt.Errorf("failed to open netns %q: %w", req.Netns, err)
	}
	defer netNS.Close()

	return netNS.Do(func(_ ns.NetNS) error {
		link, err := netlink.LinkByName(req.IfName)
		if err != nil {
			return fmt.Errorf("interface %q not found: %w", req.IfName, err)
		}
		if link.Attrs().Flags&net.FlagUp == 0 {
			return fmt.Errorf("interface %q is down", req.IfName)
		}
		addrs, err := netlink.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to list addresses on %q: %w", req.IfName, err)
		}
		if len(addrs) == 0 {
			return fmt.Errorf("interface %q has no addresses", req.IfName)
		}
		klog.V(4).InfoS("Interface check passed",
			"containerID", shortID(req.ContainerID),
			"ifName", req.IfName,
			"addrs", addrs,
		)
		return nil
	})
}

// shortID returns the first 12 characters of a container ID for concise logging.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
