//go:build linux

// purenet is a thin CNI shim.  Containerd fork-execs it on every pod
// lifecycle event.  It reads the CNI environment variables and stdin config,
// forwards the request over a Unix-domain gRPC socket to the long-running
// purenet agent, and writes the agent's response back to stdout.
//
// All actual network programming is done by the agent, not here.
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/version"
	bv "github.com/containernetworking/plugins/pkg/utils/buildversion"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/aroradaman/purenet/pkg/cni"
)

const dialTimeout = 10 * time.Second

func init() {
	runtime.LockOSThread()
}

func main() {
	skel.PluginMainFuncs(
		skel.CNIFuncs{
			Add:   cmdAdd,
			Del:   cmdDel,
			Check: cmdCheck,
		},
		version.All,
		bv.BuildString("purenet"),
	)
}

func cmdAdd(args *skel.CmdArgs) error {
	client, conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	resp, err := client.CmdAdd(ctx, buildRequest(args))
	if err := rpcError(err); err != nil {
		return err
	}
	if err := resp.AsError(); err != nil {
		return err
	}

	_, err = os.Stdout.Write(resp.Result)
	return err
}

func cmdDel(args *skel.CmdArgs) error {
	client, conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	resp, err := client.CmdDel(ctx, buildRequest(args))
	if err := rpcError(err); err != nil {
		return err
	}
	return resp.AsError()
}

func cmdCheck(args *skel.CmdArgs) error {
	client, conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	resp, err := client.CmdCheck(ctx, buildRequest(args))
	if err := rpcError(err); err != nil {
		return err
	}
	return resp.AsError()
}

// dial connects to the agent over the Unix socket.
func dial() (cni.CNIBackendClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"unix://"+cni.DefaultSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(cni.JSONCodec{})),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to purenet agent at %s: %w",
			cni.DefaultSocketPath, err)
	}
	return cni.NewCNIBackendClient(conn), conn, nil
}

// rpcError converts a gRPC transport error into a *types.Error with a proper
// CNI error code, mirroring Antrea's pkg/cni/client.go.
//
//   - Unimplemented → incompatible API version between shim and agent builds.
//   - Unavailable / DeadlineExceeded → transient; kubelet will retry.
//   - nil → no error.
//   - everything else → unknown RPC error.
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		return &types.Error{
			Code:    uint(cni.ErrorCodeIncompatibleAPIVersion),
			Msg:     "incompatible API version between purenet shim and agent",
			Details: err.Error(),
		}
	case codes.Unavailable, codes.DeadlineExceeded:
		return &types.Error{
			Code: uint(cni.ErrorCodeTryAgainLater),
			Msg:  err.Error(),
		}
	default:
		return &types.Error{
			Code: uint(cni.ErrorCodeUnknownRPCError),
			Msg:  err.Error(),
		}
	}
}

// buildRequest converts skel's args into a CNIRequest.
func buildRequest(args *skel.CmdArgs) *cni.CNIRequest {
	return &cni.CNIRequest{
		ContainerID: args.ContainerID,
		Netns:       args.Netns,
		IfName:      args.IfName,
		Args:        args.Args,
		Path:        args.Path,
		Config:      args.StdinData,
	}
}
