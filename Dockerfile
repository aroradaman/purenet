# ─────────────────────────────────────────────
# Stage 1: Build both binaries
# ─────────────────────────────────────────────
FROM golang:1.26 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# purenet  – thin CNI shim invoked by containerd on every pod event.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /purenet ./cmd/purenet

# agent    – long-running gRPC server that does the actual network programming.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /agent ./cmd/agent

# ─────────────────────────────────────────────
# Stage 2: Final image
#
# /opt/cni/bin/   – CNI shim (installer copies to host at pod start)
# /usr/local/bin/ – the agent binary (run as the DaemonSet main container)
# ─────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache iptables

# CNI shim — copied to the host by install.sh.
COPY --from=builder /purenet /opt/cni/bin/purenet

# Agent binary — run directly by the DaemonSet pod.
COPY --from=builder /agent /usr/local/bin/agent

# CNI network config template.
COPY config/10-purenet.conf /etc/cni/net.d/10-purenet.conf

# Installer script (init container).
COPY hack/install.sh /install.sh
RUN chmod +x /install.sh

# Default: start the agent.
ENTRYPOINT ["/usr/local/bin/agent"]
