//go:build linux

package network

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	"github.com/vishvananda/netlink"
	"sigs.k8s.io/knftables"
)

const hostVethPrefix = "veth"

// SetupVeth creates a veth pair: one end inside the container netns named
// ifName, and the other end in the host netns with a unique name derived from
// containerID.
//
// Returns the (host-side, container-side) link objects and any error.
func SetupVeth(ifName, containerID string, mtu int) (netlink.Link, netlink.Link, error) {
	contVeth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name:  ifName,
			Flags: net.FlagUp,
			MTU:   mtu,
		},
		PeerName: generateHostVethName(containerID),
	}

	if err := netlink.LinkAdd(contVeth); err != nil {
		return nil, nil, fmt.Errorf("failed to add veth pair: %w", err)
	}

	contIface, err := netlink.LinkByName(ifName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to look up container veth %q: %w", ifName, err)
	}

	hostIface, err := netlink.LinkByName(contVeth.PeerName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to look up host veth %q: %w", contVeth.PeerName, err)
	}

	return hostIface, contIface, nil
}

// ConfigureInterface assigns addr to the link identified by ifName inside the
// current network namespace and brings the interface up.
func ConfigureInterface(ifName string, addr *netlink.Addr) error {
	link, err := netlink.LinkByName(ifName)
	if err != nil {
		return fmt.Errorf("failed to find interface %q: %w", ifName, err)
	}

	if err := netlink.AddrAdd(link, addr); err != nil {
		return fmt.Errorf("failed to add address %v to %q: %w", addr, ifName, err)
	}

	if err := netlink.LinkSetUp(link); err != nil {
		return fmt.Errorf("failed to bring up interface %q: %w", ifName, err)
	}

	return nil
}

// AddDefaultRoute adds a default route via gw in the current network namespace.
func AddDefaultRoute(gw net.IP) error {
	_, defNet, _ := net.ParseCIDR("0.0.0.0/0")
	route := &netlink.Route{
		Dst: defNet,
		Gw:  gw,
	}
	if err := netlink.RouteAdd(route); err != nil {
		return fmt.Errorf("failed to add default route via %v: %w", gw, err)
	}
	return nil
}

// SetupHostRoute adds a /32 (or /128 for IPv6) host route on the host for
// podIP via the named host-side veth.
//
// Without this, the kernel has no route to reach the pod's IP from the host,
// so kubelet health probes and any other host→pod traffic is dropped.
func SetupHostRoute(hostVethName string, podIP net.IP) error {
	link, err := netlink.LinkByName(hostVethName)
	if err != nil {
		return fmt.Errorf("failed to find host veth %q: %w", hostVethName, err)
	}

	bits := 32
	if podIP.To4() == nil {
		bits = 128
	}
	route := &netlink.Route{
		LinkIndex: link.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
		Dst:       &net.IPNet{IP: podIP, Mask: net.CIDRMask(bits, bits)},
	}
	if err := netlink.RouteAdd(route); err != nil {
		return fmt.Errorf("failed to add host route %v dev %s: %w", podIP, hostVethName, err)
	}
	return nil
}

// EnableProxyARP enables proxy ARP on the named host-side veth.
//
// When the container ARPs for its default gateway (a virtual IP that does not
// exist on any interface), the host must respond so the pod can send packets.
// Without this, outbound traffic from the pod is dropped at the ARP stage.
func EnableProxyARP(vethName string) error {
	path := fmt.Sprintf("/proc/sys/net/ipv4/conf/%s/proxy_arp", vethName)
	if err := os.WriteFile(path, []byte("1"), 0644); err != nil {
		return fmt.Errorf("failed to enable proxy_arp on %s: %w", vethName, err)
	}
	return nil
}

// SetupMasquerade installs an nftables POSTROUTING MASQUERADE rule so that
// pod traffic leaving the node (destination outside podCIDR) is SNAT'd to the
// node's IP.
//
// Without this, pods can only be reached from the local node.  When a pod
// connects to a Kubernetes service on another node (e.g. kube-apiserver), the
// DNAT resolves to the remote node's IP but the source remains the pod IP.
// The remote node has no route back to the pod subnet and drops the response.
// MASQUERADE ensures the source is rewritten to the node IP so the reply
// takes the normal reverse path through conntrack.
//
// The operation is fully idempotent: the table and chain are created with
// tx.Add (no-op if they already exist), the chain is flushed, and the rule is
// (re)added — all in a single atomic nft transaction.
func SetupMasquerade(ctx context.Context, podCIDR string) error {
	nft, err := knftables.New(knftables.IPv4Family, "purenet")
	if err != nil {
		return fmt.Errorf("nftables not available: %w", err)
	}

	tx := nft.NewTransaction()

	// Create the table if it does not exist.
	tx.Add(&knftables.Table{
		Family: knftables.IPv4Family,
		Name:   "purenet",
	})

	// Create the base chain (no-op if it already exists), then flush it so
	// we always land in a known-good state after an agent restart.
	tx.Add(&knftables.Chain{
		Name:     "postrouting",
		Type:     knftables.PtrTo(knftables.NATType),
		Hook:     knftables.PtrTo(knftables.PostroutingHook),
		Priority: knftables.PtrTo(knftables.SNATPriority),
	})
	tx.Flush(&knftables.Chain{Name: "postrouting"})

	// MASQUERADE packets whose source is in podCIDR but whose destination is
	// outside it (i.e. leaving the node for an external address or another
	// node's IP).
	tx.Add(&knftables.Rule{
		Chain: "postrouting",
		Rule: knftables.Concat(
			"ip saddr", podCIDR,
			"ip daddr !=", podCIDR,
			"masquerade",
		),
	})

	if err := nft.Run(ctx, tx); err != nil {
		return fmt.Errorf("nftables masquerade setup failed: %w", err)
	}
	return nil
}

// DeleteVeth removes the veth pair by deleting the container-side interface;
// the kernel automatically removes the peer.
func DeleteVeth(ifName string) error {
	link, err := netlink.LinkByName(ifName)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to find interface %q for deletion: %w", ifName, err)
	}

	if err := netlink.LinkDel(link); err != nil {
		return fmt.Errorf("failed to delete interface %q: %w", ifName, err)
	}
	return nil
}

// generateHostVethName returns a unique, deterministic host-side veth name
// derived from the container ID.
//
// SHA-1(containerID) → hex string → first 11 chars → "veth" + 11 = 15 chars,
// which fits exactly within the Linux IFNAMSIZ limit (15 usable chars + NUL).
//
// NOTE: fmt.Sprintf("%.11x", byteSlice) would be wrong — the precision on %x
// for a []byte limits the number of *bytes* printed (= 22 hex chars), not
// characters.  hex.EncodeToString + slice gives exactly 11 characters.
func generateHostVethName(containerID string) string {
	h := sha1.New()
	h.Write([]byte(containerID))
	return hostVethPrefix + hex.EncodeToString(h.Sum(nil))[:11]
}

// isNotFound returns true when err signals that a network link was not found.
func isNotFound(err error) bool {
	return err != nil && err.Error() == "Link not found"
}
