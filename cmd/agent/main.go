//go:build linux

package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/aroradaman/purenet/pkg/agent"
	"github.com/aroradaman/purenet/pkg/cni"
	"github.com/aroradaman/purenet/pkg/nodesync"
)

const (
	logFile     = "/var/log/purenet/purenet.log"
	cniConfPath = "/host/etc/cni/net.d/10-purenet.conf"
	healthzAddr = ":8081"
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

	// Overwrite the CNI config with the per-node IPAM subnet so host-local
	// allocates IPs from this node's unique cidr rather than a shared pool.
	if err := syncer.WriteCNIConfig(ownPodCIDR); err != nil {
		klog.Fatalf("Failed to write per-node CNI config (podCIDR=%s): %v", ownPodCIDR, err)
	}
	klog.InfoS("CNI config written", "path", cniConfPath, "podCIDR", ownPodCIDR)

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
	cni.RegisterCNIBackendServer(srv, agent.NewServer())

	klog.InfoS("purenet agent starting", "socket", socketPath, "podCIDR", ownPodCIDR)

	go func() {
		if err := srv.Serve(listener); err != nil {
			klog.Fatalf("gRPC server stopped: %v", err)
		}
	}()

	// ── Haiku healthz ─────────────────────────────────────────────────────────
	// /healthz returns a 5-7-5 reflecting the current node state.  Liveness
	// probes get a 200; the body is for the humans reading `curl` output.
	mux := http.NewServeMux()
	mux.Handle("/healthz", agent.HaikuHealthzHandler(nodeName))
	healthzSrv := &http.Server{
		Addr:              healthzAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		klog.InfoS("haiku healthz listening", "addr", healthzAddr)
		if err := healthzSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "healthz server stopped")
		}
	}()

	<-ctx.Done()
	klog.InfoS("Shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthzSrv.Shutdown(shutdownCtx)
	srv.GracefulStop()
	klog.InfoS("purenet agent stopped")
}
