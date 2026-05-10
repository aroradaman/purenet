BIN_DIR     ?= $(CURDIR)/bin
CNI_BIN_DIR ?= /opt/cni/bin
SHIM        := purenet
AGENT       := agent

GO      := go
GOFLAGS := -trimpath
LDFLAGS := -s -w

IMAGE_NAME        ?= purenet
IMAGE_TAG         ?= dev
KIND_CLUSTER_NAME ?= purenet-dev

.PHONY: all build build-shim build-agent install uninstall test lint clean \
        docker-build \
        kind-create kind-load kind-deploy kind-up kind-down \
        proto help

all: build

## build: compile both the CNI shim and the agent into ./bin/
build: build-shim build-agent

## build-shim: compile only the CNI shim binary
build-shim:
	@mkdir -p $(BIN_DIR)
	GOOS=linux $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(SHIM) ./cmd/purenet

## build-agent: compile only the agent binary
build-agent:
	@mkdir -p $(BIN_DIR)
	GOOS=linux $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(AGENT) ./cmd/agent

## install: build and copy the CNI shim to the CNI bin directory
install: build-shim
	@mkdir -p $(CNI_BIN_DIR)
	install -m 0755 $(BIN_DIR)/$(SHIM) $(CNI_BIN_DIR)/$(SHIM)
	@echo "Installed $(SHIM) to $(CNI_BIN_DIR)/$(SHIM)"

## uninstall: remove the CNI shim from the CNI bin directory
uninstall:
	rm -f $(CNI_BIN_DIR)/$(SHIM)

## test: run unit tests
test:
	$(GO) test ./... -v -count=1

## lint: run golangci-lint (requires golangci-lint to be installed)
lint:
	golangci-lint run ./...

## proto: regenerate Go code from cni.proto (requires buf)
proto:
	buf generate

## clean: remove build artifacts
clean:
	rm -rf $(BIN_DIR)

# ── Docker ────────────────────────────────────────────────────────────────────

## docker-build: build the container image
docker-build:
	docker build \
		--build-arg TARGETARCH=$(shell go env GOARCH) \
		-t $(IMAGE_NAME):$(IMAGE_TAG) .

# ── kind ─────────────────────────────────────────────────────────────────────

## kind-create: create a kind cluster with the default CNI disabled
kind-create:
	kind create cluster \
		--name $(KIND_CLUSTER_NAME) \
		--config deploy/kind-config.yaml

## kind-load: load the container image into the kind cluster nodes
kind-load:
	kind load docker-image $(IMAGE_NAME):$(IMAGE_TAG) \
		--name $(KIND_CLUSTER_NAME)

## kind-deploy: apply RBAC and the DaemonSet manifest to the cluster
kind-deploy:
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/daemonset.yaml

## kind-up: create cluster, build image, load it, and deploy purenet (one shot)
kind-up: kind-create docker-build kind-load kind-deploy
	@echo ""
	@echo "Cluster ready.  Watch nodes:"
	@echo "  kubectl get nodes -w"
	@echo "Watch the agent:"
	@echo "  kubectl -n kube-system logs -f -l app=purenet -c agent"

## kind-down: destroy the kind cluster
kind-down:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

## help: print this help message
help:
	@grep -h '##' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
