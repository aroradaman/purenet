//go:build linux

package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/aroradaman/purenet/pkg/agent"
	"github.com/aroradaman/purenet/pkg/cni"
	"github.com/aroradaman/purenet/pkg/ipam"
	"github.com/aroradaman/purenet/pkg/network"
	"github.com/aroradaman/purenet/pkg/nodesync"
)

const (
	logFile     = "/var/log/purenet/purenet.log"
	cniConfPath = "/host/etc/cni/net.d/10-purenet.conf"
	ipamDataDir = "/var/lib/cni/networks/purenet"
)

func init() {
	klogFlags := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlags)

	if err := os.MkdirAll(filepath.Dir(logFile), 0755); err == nil {
		klogFlags.Set("log_file", logFile)
		klogFlags.Set("logtostderr", "false")
		// Mirror to stderr so `kubectl logs` also shows it.
		klogFlags.Set("alsologtostderr", "true")
	}

	level := os.Getenv("PURENET_LOG_V")
	if level == "" {
		level = "4"
	}
	klogFlags.Set("v", level)
}

func main() {
	defer klog.Flush()

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatal("NODE_NAME environment variable is required")
	}

	// ── Kubernetes client (in-cluster) ────────────────────────────────────────
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to build in-cluster config: %v", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// ── Node route sync ───────────────────────────────────────────────────────
	// Start the node informer. It will:
	//   1. Add a host route for each existing node's pod CIDR on startup.
	//   2. React to node add/update/delete events throughout the agent's life.
	//   3. Return our own node's pod CIDR so we can configure IPAM correctly.
	syncer := nodesync.New(nodeName, k8s, cniConfPath)
	ownPodCIDR, err := syncer.Run(ctx)
	if err != nil {
		klog.Fatalf("Node sync failed: %v", err)
	}

	// Write a minimal per-node CNI config (no IPAM section — allocation is
	// handled in-process by the agent).
	if err := syncer.WriteCNIConfig(); err != nil {
		klog.Fatalf("Failed to write per-node CNI config: %v", err)
	}
	klog.InfoS("CNI config written", "path", cniConfPath, "podCIDR", ownPodCIDR)

	// ── In-process IPAM ───────────────────────────────────────────────────────
	alloc, err := ipam.New(ownPodCIDR, ipamDataDir)
	if err != nil {
		klog.Fatalf("Failed to create IPAM allocator (podCIDR=%s): %v", ownPodCIDR, err)
	}

	// ── nftables masquerade ───────────────────────────────────────────────────
	// SNAT pod traffic leaving the node so that return packets can find their
	// way back.  Uses a dedicated "purenet" nftables table.
	if err := network.SetupMasquerade(ctx, ownPodCIDR); err != nil {
		klog.Fatalf("Failed to set up nftables masquerade (podCIDR=%s): %v", ownPodCIDR, err)
	}
	klog.InfoS("nftables masquerade configured", "podCIDR", ownPodCIDR)

	// ── gRPC server ───────────────────────────────────────────────────────────
	socketPath := cni.DefaultSocketPath

	if err := os.MkdirAll(filepath.Dir(socketPath), 0750); err != nil {
		klog.Fatalf("Failed to create socket directory: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		klog.Fatalf("Failed to listen on %s: %v", socketPath, err)
	}

	srv := grpc.NewServer()
	cni.RegisterCNIBackendServer(srv, agent.NewServer(alloc))

	klog.InfoS("purenet agent starting", "socket", socketPath, "podCIDR", ownPodCIDR)

	go func() {
		if err := srv.Serve(listener); err != nil {
			klog.Fatalf("gRPC server stopped: %v", err)
		}
	}()

	<-ctx.Done()
	klog.InfoS("Shutting down")
	srv.GracefulStop()
	klog.InfoS("purenet agent stopped")
}
