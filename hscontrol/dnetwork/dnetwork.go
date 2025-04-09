package dnetwork

import (
	"sync"
	"time"
)

type DNetwork struct {
	g   *DGraph
	gMu sync.RWMutex
}

func NewDNetwork() *DNetwork {
	return &DNetwork{g: NewDGraph()}
}

func (d *DNetwork) NodeIsAvailable(host string, port uint16) {
	node, ok := d.g.GetNodeByHostPort(host, port)
	d.gMu.Lock()
	defer d.gMu.Unlock()
	if !ok {
		dNode := NewDNode(host, port, time.Now())
		d.g.AddNode(*dNode)
	} else {
		node.LastAvailableAt = time.Now()
		node.banUntil = time.Unix(0, 0)
		node.banCount = 0
	}
}

func (d *DNetwork) NodeIsNotAvailable(host string, port uint16) {
	node, ok := d.g.GetNodeByHostPort(host, port)
	d.gMu.Lock()
	defer d.gMu.Unlock()
	if ok {
		banDuration := fibonacciBan(node.banCount)
		node.banUntil = time.Now().Add(banDuration)
	}
}

func fibonacciBan(n uint64) time.Duration {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	}

	a, b := uint64(0), uint64(1)
	for i := uint64(2); i <= n; i++ {
		a, b = b, a+b
	}
	return time.Duration(b) * time.Second
}
