# PureNet

A learning-grade CNI plugin for Kubernetes written in Go.  
It follows the same **shim → agent** architecture used by production CNIs like [Antrea](https://antrea.io), and implements host-gateway inter-node routing in the style of [kindnet](https://github.com/aojea/kindnet).

---

## Architecture

```
Node A                                          Node B
┌──────────────────────────────────┐           ┌──────────────────────────────────┐
│                                  │           │                                  │
│  containerd                      │           │  containerd                      │
│      │ fork-exec on pod event    │           │      │                           │
│      ▼                           │           │      ▼                           │
│  /opt/cni/bin/purenet  (shim)    │           │  /opt/cni/bin/purenet  (shim)    │
│      │ gRPC over Unix socket     │           │      │ gRPC over Unix socket     │
│      ▼                           │           │      ▼                           │
│  purenet-agent (DaemonSet pod)   │  routes   │  purenet-agent (DaemonSet pod)   │
│  ┌────────────────────────────┐  │◄─────────►│  ┌────────────────────────────┐  │
│  │ gRPC server                │  │           │  │ gRPC server                │  │
│  │  CmdAdd / CmdDel / CmdCheck│  │           │  │  CmdAdd / CmdDel / CmdCheck│  │
│  │                            │  │           │  │                            │  │
│  │ node-sync controller       │  │           │  │ node-sync controller       │  │
│  │  watches k8s Node objects  │  │           │  │  watches k8s Node objects  │  │
│  └────────────────────────────┘  │           │  └────────────────────────────┘  │
│                                  │           │                                  │
│  Pod A1  (10.244.0.2)            │           │  Pod B1  (10.244.1.2)            │
│  Pod A2  (10.244.0.3)            │           │  Pod B2  (10.244.1.3)            │
└──────────────────────────────────┘           └──────────────────────────────────┘
```

### Components

#### `cmd/purenet` — CNI shim (thin binary on every node)

containerd fork-execs this binary on every `ADD`, `DEL`, and `CHECK` event.  
It does no network programming itself — it only:

1. Reads the CNI environment variables and stdin JSON that containerd provides.
2. Dials the agent over a Unix-domain gRPC socket (`/var/run/purenet/cni.sock`).
3. Forwards the request to the agent.
4. Writes the agent's response to stdout (for `ADD`) and exits.

Short-lived. No state. If the agent is unreachable the shim returns `TryAgainLater` so kubelet retries.

#### `cmd/agent` — purenet agent (long-running DaemonSet pod)

The brain. Runs with `hostNetwork: true` and `privileged: true`. Responsible for:

| Responsibility | Detail |
|---|---|
| **gRPC server** | Handles `CmdAdd`, `CmdDel`, `CmdCheck` RPCs from the shim |
| **Veth management** | Creates/deletes a veth pair per pod; one end in the pod netns, one on the host |
| **IPAM** | Forks `host-local` with the per-node pod CIDR to allocate/release IPs |
| **Host routes** | Adds a `/32` route on the host for each pod IP so kubelet probes reach the pod |
| **Proxy ARP** | Enables `proxy_arp` on host veths so pods can ARP-resolve their default gateway |
| **Node sync** | Watches Kubernetes `Node` objects and programs inter-node routes (see below) |
| **CNI config** | Writes a per-node `/etc/cni/net.d/10-purenet.conf` with the correct IPAM subnet |

#### `pkg/nodesync` — host-gateway inter-node routing

Kubernetes assigns each node a unique pod CIDR (e.g. `/24` from a `/16` pool) via `node.spec.podCIDR`.
The node-sync controller uses a Kubernetes node informer to watch for node additions, updates, and deletions, and maintains host routes so pods can communicate across nodes:

```
# On node A (pod CIDR 10.244.0.0/24):
ip route add 10.244.1.0/24 via 172.18.0.4   # node B
ip route add 10.244.2.0/24 via 172.18.0.5   # node C

# On node B (pod CIDR 10.244.1.0/24):
ip route add 10.244.0.0/24 via 172.18.0.3   # node A
ip route add 10.244.2.0/24 via 172.18.0.5   # node C
```

This is the same approach as kindnet's host-gateway mode: no overlay, no NAT, source IPs preserved end-to-end.

#### `pkg/cni` — gRPC contract

Hand-written gRPC stubs and a JSON codec shared by both the shim and the agent. No `protoc` dependency at build time. Uses `application/grpc+json` on the wire — easy to inspect with standard tools.

---

## Pod ADD flow

```
containerd
    │
    │  fork-exec /opt/cni/bin/purenet
    │  env: CNI_COMMAND=ADD CNI_CONTAINERID=... CNI_NETNS=...
    │  stdin: { "type": "purenet", "ipam": { ... } }
    │
    ▼
purenet shim
    │  gRPC CmdAdd → /var/run/purenet/cni.sock
    ▼
purenet agent (CmdAdd)
    │
    ├─ 1. Open pod netns
    │
    ├─ 2. Inside pod netns:
    │       create veth pair (eth0 in pod, vethXXXXXXXXXXX on host)
    │       move host-side veth to host netns
    │
    ├─ 3. Bring up host veth
    │       enable proxy_arp on host veth
    │
    ├─ 4. Fork host-local IPAM (ADD)
    │       allocates IP from node's pod CIDR (e.g. 10.244.1.5/24)
    │       gateway = 10.244.1.1
    │
    ├─ 5. Add /32 host route:  10.244.1.5 dev vethXXXXXXXXXXX
    │
    └─ 6. Inside pod netns:
            assign IP to eth0 (10.244.1.5/24)
            add default route: 0.0.0.0/0 via 10.244.1.1
    │
    ▼
purenet shim
    │  write CNI result JSON to stdout
    ▼
containerd  ← pod is now network-ready
```

---

## Packet flow (cross-node, pod → service)

```
Pod A (10.244.0.5) on Node A          Node B                  kube-apiserver
        │                               │                           │
        │  dst: 10.96.0.1:443           │                           │
        │  default route via 10.244.0.1 │                           │
        │  ARP for 10.244.0.1 →         │                           │
        │    host veth responds         │                           │
        │  packet arrives at Node A     │                           │
        │                               │                           │
        │  kube-proxy DNAT:             │                           │
        │    10.96.0.1:443 → 172.18.0.4:6443                        │
        │                               │                           │
        │  host route (nodesync):       │                           │
        │    10.244.0.0/24 is local →   │                           │
        │    packet forwarded to 172.18.0.4 (Node B Docker IP)      │
        │                               │                           │
        │                        src: 10.244.0.5                    │
        │                        dst: 172.18.0.4:6443               │
        │                               │  forward to apiserver     │
        │                               │──────────────────────────►│
        │                               │                           │
        │                               │  reply to 10.244.0.5      │
        │                               │◄──────────────────────────│
        │                               │                           │
        │  nodesync route on Node B:    │                           │
        │    10.244.0.0/24 via 172.18.0.3 (Node A)                  │
        │                               │                           │
        │◄──────────────────────────────│                           │
reply reaches pod                       │
```

No NAT. The return packet uses the host-gateway route programmed by `nodesync`.

---

## Repository layout

```
purenet/
├── cmd/
│   ├── purenet/        # thin CNI shim (installed to /opt/cni/bin/)
│   └── agent/          # long-running gRPC server (DaemonSet main container)
├── pkg/
│   ├── cni/            # gRPC contract: CNIRequest/CNIResponse, client+server stubs
│   ├── config/         # CNI config JSON parsing (NetConf struct)
│   ├── network/        # Linux network primitives: veth, routes, proxy ARP
│   ├── agent/          # CmdAdd/Del/Check implementation, IPAM delegation
│   └── nodesync/       # Kubernetes node informer + host-gateway route manager
├── deploy/
│   ├── kind-config.yaml  # Kind cluster config (disableDefaultCNI, podSubnet)
│   ├── rbac.yaml         # ServiceAccount, ClusterRole, ClusterRoleBinding
│   └── daemonset.yaml    # purenet DaemonSet
├── hack/
│   └── install.sh        # init container script: copies binaries + config to host
├── config/
│   └── 10-purenet.conf   # initial CNI config template (agent overwrites per-node)
└── Dockerfile            # multi-stage: builder → cni-plugins → final (Alpine)
```

---

## Running on Kind

**Prerequisites:** `docker`, `kind`, `kubectl`, `go 1.26+`

```bash
# 1. Create the cluster (disables kindnet, assigns per-node pod CIDRs)
make kind-create

# 2. Build the image and deploy purenet
make docker-build kind-load kind-deploy

# Or all at once:
make kind-up

# Watch agent logs
kubectl -n kube-system logs -f -l app=purenet -c agent

# Verify nodes are Ready and pods are running
kubectl get nodes
kubectl get pods -A
```

**Tear down:**
```bash
make kind-down
```

---

## Key design decisions

**Shim/agent split** — containerd's CNI invocation model requires a fast, short-lived binary. Doing all network programming there makes debugging hard (no persistent logs, no state). Delegating to a long-running agent means you get `kubectl logs`, persistent klog output, and the ability to hold per-node state (like the node route map).

**Hand-written gRPC stubs** — avoids a `protoc`/`buf` dependency at build time. The `pkg/cni/cni.proto` file is the source of truth; run `make proto` to regenerate once you have `buf` installed.

**JSON over the wire** — using `application/grpc+json` instead of protobuf binary makes the gRPC traffic trivially inspectable with `grpcurl` or a packet capture.

**host-local IPAM delegation** — rather than implementing IP allocation from scratch, the agent forks the upstream `host-local` binary. The key detail is passing `ContainerID`, `NetNS`, and `IfName` explicitly via `invoke.Args` rather than relying on `os.Environ()` (which doesn't have these per-invocation variables in a long-running process).

**Host-gateway routing** — each node gets a unique `/24` from the cluster's `/16` pod CIDR. The `nodesync` controller maintains one `ip route` entry per remote node. No overlay, no NAT.
