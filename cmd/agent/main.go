//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
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

type options struct {
	verbosity int
}

func newAgentCommand() *cobra.Command {
	opts := &options{}

	cmd := &cobra.Command{
		Use:          "purenet-agent",
		Short:        "purenet CNI agent — gRPC server for pod network management",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(opts)
		},
	}

	cmd.Flags().IntVarP(&opts.verbosity, "v", "v", 4,
		"log verbosity level (0=errors only, 4=debug)")

	return cmd
}

func main() {
	if err := newAgentCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func run(opts *options) error {
	// ── klog setup ────────────────────────────────────────────────────────────
	klogFlags := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlags)
	klogFlags.Set("v", fmt.Sprintf("%d", opts.verbosity)) //nolint:errcheck

	if err := os.MkdirAll(filepath.Dir(logFile), 0755); err == nil {
		klogFlags.Set("log_file", logFile)       //nolint:errcheck
		klogFlags.Set("logtostderr", "false")    //nolint:errcheck
		klogFlags.Set("alsologtostderr", "true") //nolint:errcheck
	}

	defer klog.Flush()

	// ── required env vars ─────────────────────────────────────────────────────
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME environment variable is required")
	}

	// ── Kubernetes client (in-cluster) ────────────────────────────────────────
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("build in-cluster config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
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
		return fmt.Errorf("node sync: %w", err)
	}

	// Write a minimal per-node CNI config (no IPAM section — allocation is
	// handled in-process by the agent).
	if err := syncer.WriteCNIConfig(); err != nil {
		return fmt.Errorf("write per-node CNI config: %w", err)
	}
	klog.InfoS("CNI config written", "path", cniConfPath, "podCIDR", ownPodCIDR)

	// ── In-process IPAM ───────────────────────────────────────────────────────
	alloc, err := ipam.New(ownPodCIDR, ipamDataDir)
	if err != nil {
		return fmt.Errorf("create IPAM allocator (podCIDR=%s): %w", ownPodCIDR, err)
	}

	// ── nftables masquerade ───────────────────────────────────────────────────
	if err := network.SetupMasquerade(ctx, ownPodCIDR); err != nil {
		return fmt.Errorf("set up nftables masquerade (podCIDR=%s): %w", ownPodCIDR, err)
	}
	klog.InfoS("nftables masquerade configured", "podCIDR", ownPodCIDR)

	// ── gRPC server ───────────────────────────────────────────────────────────
	socketPath := cni.DefaultSocketPath

	if err := os.MkdirAll(filepath.Dir(socketPath), 0750); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socketPath, err)
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
	return nil
}
