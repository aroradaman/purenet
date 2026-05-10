// Package cni defines the gRPC contract between the thin CNI binary shim
// (installed at /opt/cni/bin/purenet on every node) and the purenet agent
// (long-running DaemonSet pod).
//
// The service descriptor and client/server stubs are hand-written to avoid a
// protoc/buf dependency at build time.  Run `make proto` to regenerate from
// cni.proto once you have buf installed.
package cni

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

const (
	// DefaultSocketPath is the Unix domain socket the agent listens on and the
	// shim connects to.  Both share it via a hostPath volume mount.
	DefaultSocketPath = "/var/run/purenet/cni.sock"

	methodAdd   = "/cni.v1.CNIBackend/CmdAdd"
	methodDel   = "/cni.v1.CNIBackend/CmdDel"
	methodCheck = "/cni.v1.CNIBackend/CmdCheck"
)

// ErrorCode mirrors the CNI spec error codes so the container runtime can
// take the right action (e.g. retry on TryAgainLater).
type ErrorCode uint32

const (
	ErrorCodeUnknown                ErrorCode = 0
	ErrorCodeIncompatibleCNIVersion ErrorCode = 1
	ErrorCodeUnsupportedField       ErrorCode = 2
	ErrorCodeUnknownContainer       ErrorCode = 3
	ErrorCodeInvalidEnvVars         ErrorCode = 4
	ErrorCodeIOFailure              ErrorCode = 5
	ErrorCodeDecodingFailure        ErrorCode = 6
	ErrorCodeInvalidNetworkConfig   ErrorCode = 7
	ErrorCodeTryAgainLater          ErrorCode = 11
	ErrorCodeIPAMFailure            ErrorCode = 101
	ErrorCodeConfigInterfaceFailure ErrorCode = 102
	ErrorCodeCheckInterfaceFailure  ErrorCode = 103
	ErrorCodeUnknownRPCError        ErrorCode = 201
	ErrorCodeIncompatibleAPIVersion ErrorCode = 202
)

// ── Wire types ────────────────────────────────────────────────────────────────

// CNIRequest carries all the information containerd passes to the CNI binary.
type CNIRequest struct {
	ContainerID string `json:"container_id"`
	Netns       string `json:"netns"`
	IfName      string `json:"if_name"`
	Args        string `json:"args"`
	Path        string `json:"path"`
	Config      []byte `json:"config"` // raw stdin JSON
}

// CNIResponse is returned by the agent for every CNI command.
// On success ErrorCode is 0.  On failure ErrorCode is non-zero and
// ErrorMessage carries a human-readable description.
// Result holds the serialised CNI result JSON for ADD; it is empty for DEL/CHECK.
type CNIResponse struct {
	ErrorCode    ErrorCode `json:"error_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	Result       []byte    `json:"result,omitempty"`
}

// ── Server interface ──────────────────────────────────────────────────────────

// CNIBackendServer must be implemented by the agent.
type CNIBackendServer interface {
	CmdAdd(context.Context, *CNIRequest) (*CNIResponse, error)
	CmdDel(context.Context, *CNIRequest) (*CNIResponse, error)
	CmdCheck(context.Context, *CNIRequest) (*CNIResponse, error)
}

// UnimplementedCNIBackendServer provides safe defaults so that embedding it
// keeps the code forwards-compatible when new RPCs are added.
type UnimplementedCNIBackendServer struct{}

func (UnimplementedCNIBackendServer) CmdAdd(_ context.Context, _ *CNIRequest) (*CNIResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "CmdAdd not implemented")
}
func (UnimplementedCNIBackendServer) CmdDel(_ context.Context, _ *CNIRequest) (*CNIResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "CmdDel not implemented")
}
func (UnimplementedCNIBackendServer) CmdCheck(_ context.Context, _ *CNIRequest) (*CNIResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "CmdCheck not implemented")
}

// RegisterCNIBackendServer wires srv into gRPC server s.
func RegisterCNIBackendServer(s *grpc.Server, srv CNIBackendServer) {
	s.RegisterService(&cniBackendServiceDesc, srv)
}

// ── Client interface ──────────────────────────────────────────────────────────

// CNIBackendClient is the interface used by the thin CNI binary shim.
type CNIBackendClient interface {
	CmdAdd(ctx context.Context, req *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error)
	CmdDel(ctx context.Context, req *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error)
	CmdCheck(ctx context.Context, req *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error)
}

// NewCNIBackendClient creates a client that talks over conn.
func NewCNIBackendClient(conn grpc.ClientConnInterface) CNIBackendClient {
	return &cniBackendClient{conn}
}

type cniBackendClient struct{ cc grpc.ClientConnInterface }

func (c *cniBackendClient) CmdAdd(ctx context.Context, in *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error) {
	out := new(CNIResponse)
	return out, c.cc.Invoke(ctx, methodAdd, in, out, opts...)
}
func (c *cniBackendClient) CmdDel(ctx context.Context, in *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error) {
	out := new(CNIResponse)
	return out, c.cc.Invoke(ctx, methodDel, in, out, opts...)
}
func (c *cniBackendClient) CmdCheck(ctx context.Context, in *CNIRequest, opts ...grpc.CallOption) (*CNIResponse, error) {
	out := new(CNIResponse)
	return out, c.cc.Invoke(ctx, methodCheck, in, out, opts...)
}

// ── JSON codec ────────────────────────────────────────────────────────────────

// JSONCodec serialises gRPC messages as JSON instead of protobuf binary.
// When you adopt protoc/buf generated code you can drop this and let gRPC use
// its default protobuf codec.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (JSONCodec) Name() string                       { return "json" }

func init() {
	// Register the JSON codec globally.  The gRPC server then automatically
	// selects it when the shim sends Content-Type: application/grpc+json.
	encoding.RegisterCodec(JSONCodec{})
}

// ── gRPC service descriptor ───────────────────────────────────────────────────
// This is what protoc-gen-go-grpc would generate from cni.proto.

var cniBackendServiceDesc = grpc.ServiceDesc{
	ServiceName: "cni.v1.CNIBackend",
	HandlerType: (*CNIBackendServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "CmdAdd", Handler: _CmdAdd_Handler},
		{MethodName: "CmdDel", Handler: _CmdDel_Handler},
		{MethodName: "CmdCheck", Handler: _CmdCheck_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cni.proto",
}

func _CmdAdd_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(CNIRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CNIBackendServer).CmdAdd(ctx, req)
	}
	return interceptor(ctx, req,
		&grpc.UnaryServerInfo{Server: srv, FullMethod: methodAdd},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CNIBackendServer).CmdAdd(ctx, req.(*CNIRequest))
		})
}

func _CmdDel_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(CNIRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CNIBackendServer).CmdDel(ctx, req)
	}
	return interceptor(ctx, req,
		&grpc.UnaryServerInfo{Server: srv, FullMethod: methodDel},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CNIBackendServer).CmdDel(ctx, req.(*CNIRequest))
		})
}

func _CmdCheck_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(CNIRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CNIBackendServer).CmdCheck(ctx, req)
	}
	return interceptor(ctx, req,
		&grpc.UnaryServerInfo{Server: srv, FullMethod: methodCheck},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CNIBackendServer).CmdCheck(ctx, req.(*CNIRequest))
		})
}

// ErrorResponse is a helper that builds a failure CNIResponse.
func ErrorResponse(code ErrorCode, err error) *CNIResponse {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &CNIResponse{ErrorCode: code, ErrorMessage: msg}
}

// AsError converts a non-zero-code CNIResponse into a Go error, or nil on success.
func (r *CNIResponse) AsError() error {
	if r.ErrorCode == 0 {
		return nil
	}
	return fmt.Errorf("CNI error %d: %s", r.ErrorCode, r.ErrorMessage)
}
