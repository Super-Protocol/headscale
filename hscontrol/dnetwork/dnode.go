package dnetwork

import (
	"fmt"
	"sync"
	"time"
)

type DNode struct {
	id              int64
	systemId        string
	Host            string    `json:"host"`
	Port            uint16    `json:"port"`
	LastAvailableAt time.Time `json:"last_available_at"`
	banUntil        time.Time
	banCount        uint64
}

var (
	nextId   int64 = 0
	nextIdMu sync.Mutex
)

func NewDNode(host string, port uint16, lastAvailableAt time.Time) *DNode {
	nextIdMu.Lock()
	nextId++
	id := nextId
	nextIdMu.Unlock()

	systemId := MakeNodeSystemId(host, port)
	return &DNode{
		id:              id,
		systemId:        systemId,
		Host:            host,
		Port:            port,
		LastAvailableAt: lastAvailableAt,
		banUntil:        time.Unix(0, 0),
		banCount:        0,
	}
}

func (n DNode) ID() int64 {
	return n.id
}

func MakeNodeSystemId(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}
