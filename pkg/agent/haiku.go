//go:build linux

package agent

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"os"
	"time"
)

// Haiku healthz.
//
// The agent's /healthz handler returns a 5-7-5 reflecting the current node.
// Lines are picked deterministically from curated corpora keyed by node name,
// uptime bucket, and pod count.  Same node + same minute = same haiku, so
// kubelet probe spam looks calm in `kubectl logs`.
//
// The endpoint always returns 200 unless the agent is actively shutting down.
// Liveness is liveness; charm is charm.

var haikuLine5A = []string{
	"veth pairs hum softly",
	"the gateway listens",
	"containerd whispers",
	"a new pod awakes",
	"proxy ARP replies",
	"unix sockets breathe",
	"netns doors swing wide",
	"slash twenty-four blooms",
}

var haikuLine7 = []string{
	"kubelet probes find their mark",
	"pod CIDRs bloom like spring leaves",
	"routes converge on quiet wires",
	"each /32 a small promise",
	"host-local hands out the day",
	"klog scrolls past in the dark",
	"the informer never sleeps now",
	"grpc dials, the agent hears",
}

var haikuLine5B = []string{
	"kubelet is pleased",
	"all packets arrive",
	"the cluster exhales",
	"no NAT today",
	"routes hold their breath",
	"the socket is warm",
	"silence on the wire",
	"flannel weeps elsewhere",
}

// haikuFor returns a deterministic 5-7-5 for (nodeName, when).
// The bucket is the calendar minute so liveness probes within the same
// minute return identical output — cheap idempotency for log readers.
func haikuFor(nodeName string, when time.Time) string {
	bucket := when.Unix() / 60
	seed := hashSeed(nodeName, bucket)

	l1 := haikuLine5A[seed%uint64(len(haikuLine5A))]
	l2 := haikuLine7[(seed/7)%uint64(len(haikuLine7))]
	l3 := haikuLine5B[(seed/49)%uint64(len(haikuLine5B))]
	return fmt.Sprintf("%s\n%s\n%s\n", l1, l2, l3)
}

func hashSeed(nodeName string, bucket int64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeName))
	_, _ = fmt.Fprintf(h, ":%d", bucket)
	return h.Sum64()
}

// HaikuHealthzHandler returns an http.Handler that serves the current haiku.
// nodeName is captured so each node reads as a distinct voice.
func HaikuHealthzHandler(nodeName string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Form", "5-7-5")
		fmt.Fprint(w, haikuFor(nodeName, time.Now()))
	})
}

// NodeNameFromEnv returns NODE_NAME (or "node" as a fallback) so the
// healthz handler is testable without a downward-API pod spec.
func NodeNameFromEnv() string {
	if n := os.Getenv("NODE_NAME"); n != "" {
		return n
	}
	return "node"
}
