package dnetwork

import (
	"crypto/md5"
	"encoding/binary"
	"gonum.org/v1/gonum/graph/simple"
	"sync"
)

type DGraph struct {
	g      *simple.UndirectedGraph
	gMutex sync.RWMutex
}

func NewDGraph() *DGraph {
	graph := simple.NewUndirectedGraph()

	return &DGraph{
		g: graph,
	}
}

func (n *DGraph) GetNodes() []DNode {
	n.gMutex.RLock()
	defer n.gMutex.RUnlock()

	nodes := n.g.Nodes()
	var result []DNode
	for nodes.Next() {
		if dnode, ok := nodes.Node().(DNode); ok {
			result = append(result, dnode)
		}
	}
	return result
}

func (n *DGraph) AddNode(node DNode) {
	n.gMutex.Lock()
	defer n.gMutex.Unlock()
	n.g.AddNode(node)
}

func (n *DGraph) RemoveNode(node DNode) {
	n.gMutex.Lock()
	defer n.gMutex.Unlock()
	n.g.RemoveNode(node.ID())
}

func (n *DGraph) GetNodeByHostPort(host string, port uint16) (DNode, bool) {
	systemId := MakeNodeSystemId(host, port)
	return n.GetNodeById(systemId)
}

func (n *DGraph) GetNodeById(systemId string) (DNode, bool) {
	n.gMutex.RLock()
	defer n.gMutex.RUnlock()

	hash := md5.Sum([]byte(systemId))
	graphId := int64(binary.BigEndian.Uint64(hash[:8]))

	node := n.g.Node(graphId)
	dnode, ok := node.(DNode)
	if !ok {
		return DNode{}, false
	}
	return dnode, true
}

func (n *DGraph) SetMeasurement(from DNode, to DNode, name string, val Measurement) {
	n.gMutex.Lock()
	defer n.gMutex.Unlock()
	edge := n.g.Edge(from.ID(), to.ID())
	if edge == nil {
		dEdge := NewDEdge(from, to)
		n.g.SetEdge(dEdge)
		dEdge.SetMeasurement(name, val)
	} else {
		dEdge, ok := edge.(*DEdge)
		if !ok {
			panic("unexpected edge type")
		}
		dEdge.SetMeasurement(name, val)
	}
}

func (n *DGraph) GetMeasurement(from DNode, to DNode, name string) (Measurement, bool) {
	n.gMutex.RLock()
	defer n.gMutex.RUnlock()
	edge := n.g.Edge(from.ID(), to.ID())
	if edge == nil {
		return Measurement{}, false
	} else {
		dEdge, ok := edge.(*DEdge)
		if !ok {
			panic("unexpected edge type")
		}
		m, ok := dEdge.GetMeasurement(name)
		if !ok {
			return Measurement{}, false
		}
		return m, true
	}
}

func (n *DGraph) GetMeasurements() map[string]map[string]map[string]Measurement {
	n.gMutex.RLock()
	defer n.gMutex.RUnlock()

	edges := n.g.Edges()
	result := make(map[string]map[string]map[string]Measurement)

	for edges.Next() {
		edge := edges.Edge()
		if dEdge, ok := edge.(*DEdge); ok {
			fromId := dEdge.from.SystemID()
			toId := dEdge.to.SystemID()

			measurements := dEdge.measurements

			if _, ok := result[fromId]; !ok {
				result[fromId] = make(map[string]map[string]Measurement)
			}
			if _, ok := result[fromId][toId]; !ok {
				result[fromId][toId] = make(map[string]Measurement)
			}

			for name, m := range measurements {
				result[fromId][toId][name] = m
			}
		}
	}

	return result
}

func (n *DGraph) GetEdge(from, to DNode) (*DEdge, bool) {
	n.gMutex.RLock()
	defer n.gMutex.RUnlock()
	edge := n.g.Edge(from.ID(), to.ID())
	if edge == nil {
		return nil, false
	}
	dEdge, ok := edge.(*DEdge)
	return dEdge, ok
}

func (n *DGraph) AddEdge(edge *DEdge) {
	n.gMutex.Lock()
	defer n.gMutex.Unlock()
	n.g.SetEdge(edge)
}
