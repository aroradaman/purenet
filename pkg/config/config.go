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

	// IPAM holds the configuration for the IP Address Management plugin.
	IPAM types.IPAM `json:"ipam"`
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

	if conf.IPAM.Type == "" {
		return nil, fmt.Errorf("IPAM configuration is required")
	}

	return conf, nil
}
