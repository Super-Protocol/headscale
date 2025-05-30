package dnetwork

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

type DNode struct {
	Host            string    `json:"host"`
	RaftPort        uint16    `json:"port"`
	Port            uint16    `json:"port"`
	LastAvailableAt time.Time `json:"last_available_at,omitempty"`
	banUntil        time.Time `json:"-"`
	banCount        uint64
}

func NewDNode(host string, port uint16, raftPort uint16, lastAvailableAt time.Time) *DNode {
	return &DNode{
		Host:            host,
		Port:            port,
		RaftPort:        raftPort,
		LastAvailableAt: lastAvailableAt,
		banUntil:        time.Unix(0, 0),
		banCount:        0,
	}
}

func (n DNode) SystemID() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

func (n DNode) IsLike(node *DNode) bool {
	return n.Host == node.Host && n.Port == node.Port
}

func (n DNode) ID() int64 {
	hash := md5.Sum([]byte(n.SystemID()))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

// MarshalJSON implements custom JSON marshaling for DNode
func (n DNode) MarshalJSON() ([]byte, error) {
	type Alias DNode
	return json.Marshal(&struct {
		Alias
		LastAvailableAt string `json:"last_available_at"`
	}{
		Alias:           Alias(n),
		LastAvailableAt: n.LastAvailableAt.Format(time.RFC3339),
	})
}

func MakeNodeSystemId(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}
