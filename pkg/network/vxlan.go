//go:build linux

package network

import (
	"fmt"
	"net"
	"syscall"

	"github.com/vishvananda/netlink"
)

const (
	// VXLANIfName is the VXLAN tunnel interface created on each node.
	VXLANIfName = "purenet-gw"

	vxlanVNI  = 42   // fixed VNI for the purenet overlay
	vxlanPort = 4789 // IANA-assigned VXLAN port
)

// VTEPMAC derives a stable, locally-administered unicast MAC from the four
// bytes of a pod CIDR network address. The upper two octets are always 02:00
// (locally-administered, unicast); the lower four encode the IPv4 address
// byte-for-byte.
//
// Every node gets a unique, deterministic MAC without any annotation or
// API round-trip — any peer can compute it solely from node.spec.podCIDR.
//
// Example: 10.244.1.0 → 02:00:0a:f4:01:00
func VTEPMAC(podCIDRIP net.IP) net.HardwareAddr {
	ip4 := podCIDRIP.To4()
	if ip4 == nil {
		ip4 = []byte{0, 0, 0, 0}
	}
	return net.HardwareAddr{0x02, 0x00, ip4[0], ip4[1], ip4[2], ip4[3]}
}

// SetupVXLAN creates the purenet-gw VXLAN interface (VNI 42, port 4789,
// MAC learning disabled), assigns the VTEP IP — the network address of
// podCIDR as a /32 host address — and pins a deterministic MAC derived from
// that IP. Idempotent: if the interface already exists only the address
// assignment is re-attempted.
func SetupVXLAN(podCIDR *net.IPNet) error {
	vtepIP := podCIDR.IP.To4()
	mac := VTEPMAC(vtepIP)

	if _, err := netlink.LinkByName(VXLANIfName); err != nil {
		vx := &netlink.Vxlan{
			LinkAttrs: netlink.LinkAttrs{Name: VXLANIfName},
			VxlanId:   vxlanVNI,
			Port:      vxlanPort,
			Learning:  false, // FDB is managed manually; no flooding
		}
		if err := netlink.LinkAdd(vx); err != nil {
			return fmt.Errorf("create %s: %w", VXLANIfName, err)
		}
	}

	link, err := netlink.LinkByName(VXLANIfName)
	if err != nil {
		return fmt.Errorf("find %s: %w", VXLANIfName, err)
	}

	// Pin the deterministic MAC so remote nodes can compute it from our pod
	// CIDR without any out-of-band signalling.
	if err := netlink.LinkSetHardwareAddr(link, mac); err != nil {
		return fmt.Errorf("set MAC on %s: %w", VXLANIfName, err)
	}

	// Assign the VTEP IP as a /32 host address so the kernel does not add a
	// connected route for the whole pod subnet on this interface.
	addr := &netlink.Addr{
		IPNet: &net.IPNet{IP: vtepIP, Mask: net.CIDRMask(32, 32)},
	}
	if err := netlink.AddrAdd(link, addr); err != nil && !isEEXISTErr(err) {
		return fmt.Errorf("assign VTEP IP to %s: %w", VXLANIfName, err)
	}

	if err := netlink.LinkSetUp(link); err != nil {
		return fmt.Errorf("bring up %s: %w", VXLANIfName, err)
	}
	return nil
}

// AddVXLANPeer programs the three kernel data structures required to reach
// pods on a remote node through the VXLAN overlay:
//
//  1. Route:  remotePodCIDR via remoteVTEPIP dev purenet-gw onlink
//     Sends traffic for the remote pod subnet out the tunnel, using the
//     remote VTEP IP as the explicit next-hop.
//
//  2. Neigh:  remoteVTEPIP → remoteVTEPMAC  (permanent ARP entry)
//     Resolves the next-hop MAC without issuing an ARP request (which would
//     fail on a VXLAN interface with learning disabled).
//
//  3. FDB:    remoteVTEPMAC → remoteNodeIP  (permanent bridge FDB entry)
//     Tells the VXLAN driver the outer UDP destination for that MAC.
func AddVXLANPeer(remotePodCIDR *net.IPNet, remoteNodeIP net.IP) error {
	link, err := netlink.LinkByName(VXLANIfName)
	if err != nil {
		return fmt.Errorf("find %s: %w", VXLANIfName, err)
	}

	remoteVTEPIP := remotePodCIDR.IP.To4()
	remoteMAC := VTEPMAC(remoteVTEPIP)

	// 1. Route with RTNH_F_ONLINK so the kernel accepts a gateway that lies
	//    outside the /32 address assigned to purenet-gw.
	if err := netlink.RouteAdd(&netlink.Route{
		LinkIndex: link.Attrs().Index,
		Dst:       remotePodCIDR,
		Gw:        remoteVTEPIP,
		Flags:     int(syscall.RTNH_F_ONLINK),
	}); err != nil && !isEEXISTErr(err) {
		return fmt.Errorf("add route %s: %w", remotePodCIDR, err)
	}

	// 2. Static ARP: remoteVTEPIP → remoteMAC.
	//    NeighSet is an upsert ("ip neigh replace … nud permanent").
	if err := netlink.NeighSet(&netlink.Neigh{
		LinkIndex:    link.Attrs().Index,
		State:        netlink.NUD_PERMANENT,
		IP:           remoteVTEPIP,
		HardwareAddr: remoteMAC,
	}); err != nil {
		return fmt.Errorf("add ARP entry for %s: %w", remoteVTEPIP, err)
	}

	// 3. FDB: remoteMAC → remoteNodeIP (outer UDP destination).
	//    Family AF_BRIDGE + NTF_SELF targets the bridge/VXLAN FDB.
	if err := netlink.NeighSet(&netlink.Neigh{
		LinkIndex:    link.Attrs().Index,
		Family:       syscall.AF_BRIDGE,
		State:        netlink.NUD_PERMANENT,
		Flags:        netlink.NTF_SELF,
		HardwareAddr: remoteMAC,
		IP:           remoteNodeIP.To4(),
	}); err != nil {
		return fmt.Errorf("add FDB entry for %s: %w", remoteNodeIP, err)
	}

	return nil
}

// DelVXLANPeer removes the route, ARP neighbour, and FDB entry installed by
// AddVXLANPeer. All deletions are best-effort: ENOENT is treated as success.
func DelVXLANPeer(remotePodCIDR *net.IPNet, remoteNodeIP net.IP) error {
	link, err := netlink.LinkByName(VXLANIfName)
	if err != nil {
		return fmt.Errorf("find %s: %w", VXLANIfName, err)
	}

	remoteVTEPIP := remotePodCIDR.IP.To4()
	remoteMAC := VTEPMAC(remoteVTEPIP)

	if err := netlink.RouteDel(&netlink.Route{
		LinkIndex: link.Attrs().Index,
		Dst:       remotePodCIDR,
	}); err != nil && !isENOENTErr(err) {
		return fmt.Errorf("del route %s: %w", remotePodCIDR, err)
	}

	if err := netlink.NeighDel(&netlink.Neigh{
		LinkIndex: link.Attrs().Index,
		IP:        remoteVTEPIP,
		Family:    netlink.FAMILY_V4,
	}); err != nil && !isENOENTErr(err) {
		return fmt.Errorf("del ARP entry for %s: %w", remoteVTEPIP, err)
	}

	if err := netlink.NeighDel(&netlink.Neigh{
		LinkIndex:    link.Attrs().Index,
		HardwareAddr: remoteMAC,
		IP:           remoteNodeIP.To4(),
		Family:       syscall.AF_BRIDGE,
		Flags:        netlink.NTF_SELF,
	}); err != nil && !isENOENTErr(err) {
		return fmt.Errorf("del FDB entry for %s: %w", remoteNodeIP, err)
	}

	return nil
}

func isEEXISTErr(err error) bool { return err != nil && err.Error() == "file exists" }
func isENOENTErr(err error) bool { return err != nil && err.Error() == "no such process" }
