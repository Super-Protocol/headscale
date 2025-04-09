package dnetwork

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type GossipInteraction struct {
	n         *DNetwork
	mu        sync.RWMutex
	localHost string
	localPort uint16
}

func NewGossipInteraction(localHost string, localPort uint16, network *DNetwork) *GossipInteraction {
	return &GossipInteraction{
		n:         network,
		localHost: localHost,
		localPort: localPort,
	}
}

func (g *GossipInteraction) GetInteractionCandidates(n uint64) []DNode {
	g.n.gMu.RLock()
	defer g.n.gMu.RUnlock()

	nodes := g.n.g.GetNodes()
	now := time.Now()
	var available []DNode
	for _, node := range nodes {
		if node.Host == g.localHost && node.Port == g.localPort {
			// Skip current node
			continue
		}
		if node.banUntil.Before(now) {
			available = append(available, node)
		}
	}

	rand.Shuffle(len(available), func(i, j int) {
		available[i], available[j] = available[j], available[i]
	})

	if int(n) > len(available) {
		n = uint64(len(available))
	}

	return available[:n]
}

func (g *GossipInteraction) HandleInfoReceived(receivedNodes []DNode, receivedMeasurements map[string]map[string]map[string]Measurement) {

	g.mu.Lock()
	defer g.mu.Unlock()

	for _, receivedNode := range receivedNodes {
		existingNode, exists := g.n.g.GetNodeByHostPort(receivedNode.Host, receivedNode.Port)
		if !exists {
			//spew.Dump(receivedNode)
			dNode := NewDNode(receivedNode.Host, receivedNode.Port, receivedNode.LastAvailableAt)
			g.n.g.AddNode(*dNode)
		} else {
			if receivedNode.LastAvailableAt.After(existingNode.LastAvailableAt) {
				existingNode.LastAvailableAt = receivedNode.LastAvailableAt
			}
		}
	}

	for fromHostPort, edges := range receivedMeasurements {
		//if fromHostPort == MakeNodeSystemId(g.localHost, g.localPort) {
		//	continue
		//}
		fromNode, fromExists := g.n.g.GetNodeById(fromHostPort)
		if !fromExists {
			println(fmt.Sprintf("from does not exist %s", fromHostPort))
			continue
		}
		for toHostPort, measurements := range edges {
			toNode, toExists := g.n.g.GetNodeById(toHostPort)
			if !toExists {
				println(fmt.Sprintf("to does not exist %s", toHostPort))
				continue
			}
			edge, edgeExists := g.n.g.GetEdge(fromNode, toNode)
			if !edgeExists {
				edge = NewDEdge(fromNode, toNode)
				g.n.g.AddEdge(edge)
			}
			for name, receivedMeasurement := range measurements {
				existingMeasurement, ok := edge.GetMeasurement(name)
				if !ok || receivedMeasurement.CreationTimeUnix > existingMeasurement.CreationTimeUnix {
					edge.SetMeasurement(name, receivedMeasurement)
				}
			}
		}
	}
}

func (g *GossipInteraction) GetInfo() (receivedNodes []DNode, receivedMeasurements map[string]map[string]map[string]Measurement) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := g.n.g.GetNodes()

	measurements := g.n.g.GetMeasurements()

	return nodes, measurements
}
