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

func (d *DNetwork) SetNodeIsAvailable(host string, port uint16) {
	node, ok := d.g.GetNodeByHostPort(host, port)
	d.gMu.Lock()
	defer d.gMu.Unlock()
	if ok {
		node.LastAvailableAt = time.Now()
		node.banUntil = time.Unix(0, 0)
		node.banCount = 0
	}
}

func (d *DNetwork) SetNodeIsNotAvailable(host string, port uint16) {
	node, ok := d.g.GetNodeByHostPort(host, port)
	d.gMu.Lock()
	defer d.gMu.Unlock()
	if ok {
		banDuration := fibonacciBan(node.banCount)
		node.banUntil = time.Now().Add(banDuration)
	}
}

// GetAllNodes retrieves all nodes in the network.
func (d *DNetwork) GetAllNodes() []DNode {
	d.gMu.RLock()
	defer d.gMu.RUnlock()
	return d.g.GetNodes()
}

func (d *DNetwork) GetNodeOutgoingMeasurements(node DNode) map[DNode][]Measurement {
	d.gMu.RLock()
	defer d.gMu.RUnlock()

	outgoingMeasurements := make(map[DNode][]Measurement)
	nodes := d.g.g.From(node.ID())
	for nodes.Next() {
		toNode := nodes.Node().(DNode)
		edge, ok := d.g.GetEdge(node, toNode)
		if !ok {
			continue
		}
		edge.measurementsMu.RLock()
		measurements := make([]Measurement, 0, len(edge.measurements))
		for _, m := range edge.measurements {
			measurements = append(measurements, m)
		}
		edge.measurementsMu.RUnlock()
		outgoingMeasurements[toNode] = measurements
	}
	return outgoingMeasurements
}

func (d *DNetwork) GetNodeOutgoingMeasurementsByName(node DNode, measurementName string) map[DNode]Measurement {
	d.gMu.RLock()
	defer d.gMu.RUnlock()

	outgoingMeasurements := make(map[DNode]Measurement)
	nodes := d.g.g.From(node.ID())
	for nodes.Next() {
		toNode := nodes.Node().(DNode)
		edge, ok := d.g.GetEdge(node, toNode)
		if !ok {
			continue
		}
		edge.measurementsMu.RLock()
		if m, exists := edge.measurements[measurementName]; exists {
			outgoingMeasurements[toNode] = m
		}
		edge.measurementsMu.RUnlock()
	}
	return outgoingMeasurements
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
