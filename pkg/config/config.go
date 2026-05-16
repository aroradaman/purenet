package config

import (
	"encoding/json"
	"fmt"

	"github.com/containernetworking/cni/pkg/types"
)

// NetConf extends the standard CNI NetConf with purenet-specific fields.
type NetConf struct {
	types.NetConf

	// MTU sets the MTU on the veth interface inside the container.
	// Defaults to 1500 if unset.
	MTU int `json:"mtu,omitempty"`
}

// LoadConf parses the raw CNI JSON configuration bytes into a NetConf.
func LoadConf(bytes []byte) (*NetConf, error) {
	conf := &NetConf{}
	if err := json.Unmarshal(bytes, conf); err != nil {
		return nil, fmt.Errorf("failed to parse network config: %w", err)
	}

	if conf.MTU == 0 {
		conf.MTU = 1500
	}

	return conf, nil
}
