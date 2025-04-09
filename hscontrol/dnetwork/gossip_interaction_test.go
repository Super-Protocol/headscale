package dnetwork

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type HostPort struct {
	host string
	port uint16
}

func TestGossipInteractionWithRandomNetworks(t *testing.T) {
	// Configuration parameters
	const (
		numNetworks                = 100
		minNodesPerNetwork         = 1
		maxNodesPerNetwork         = 500
		numMeasurementThreads      = 1
		numGossipThreads           = 8
		measurementPhase           = 16 * time.Second
		gossipPhase                = 20 * time.Second
		interactionCandidatesCount = 1
	)

	// Generate a pool of random Host:Port combinations
	hostPortPool := generateHostPortPool(100)

	// Create networks with random nodes from the pool
	networks := make(map[HostPort]*DNetwork)
	networksMutex := sync.RWMutex{}

	// Create networks with random nodes
	for i := 0; i < numNetworks; i++ {
		network := NewDNetwork()
		idx := rand.Intn(len(hostPortPool))
		host, port := hostPortPool[idx].host, hostPortPool[idx].port
		node := NewDNode(host, port, time.Now())
		network.g.AddNode(*node)

		// Add random number of nodes to the network
		numNodes := rand.Intn(maxNodesPerNetwork-minNodesPerNetwork+1) + minNodesPerNetwork
		usedNodes := make(map[string]struct{})
		usedNodes[MakeNodeSystemId(host, port)] = struct{}{}
		for j := 0; j < numNodes; j++ {
			retries := 10
			for retries > 0 {
				idx := rand.Intn(len(hostPortPool))
				host, port := hostPortPool[idx].host, hostPortPool[idx].port
				key := MakeNodeSystemId(host, port)
				if _, exists := usedNodes[key]; exists {
					retries--
					continue
				}
				usedNodes[key] = struct{}{}
				node := NewDNode(host, port, time.Now())
				network.g.AddNode(*node)
				break
			}
		}

		networksMutex.Lock()
		networks[HostPort{host: host, port: port}] = network
		networksMutex.Unlock()
	}

	// Create a context with timeout for the first phase
	ctx1, cancel1 := context.WithTimeout(context.Background(), measurementPhase)
	defer cancel1()

	// Create a context with timeout for the second phase
	ctx2, cancel2 := context.WithTimeout(context.Background(), gossipPhase)
	defer cancel2()

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start goroutines to randomly modify measurements
	for i := 0; i < numMeasurementThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx1.Done():
					// First phase is over, stop modifying measurements
					return
				default:
					// Randomly select a network
					networksMutex.RLock()
					networkIDs := make([]HostPort, 0, len(networks))
					for id := range networks {
						networkIDs = append(networkIDs, id)
					}
					networksMutex.RUnlock()

					if len(networkIDs) == 0 {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					networkID := networkIDs[rand.Intn(len(networkIDs))]

					networksMutex.RLock()
					network := networks[networkID]
					networksMutex.RUnlock()

					// Get all nodes in the network
					network.gMu.RLock()
					nodes := network.g.GetNodes()
					network.gMu.RUnlock()

					if len(nodes) < 2 {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					fromNode, ok := network.g.GetNodeByHostPort(networkID.host, networkID.port)

					if !ok {
						panic("Own node not found in network")
					}

					toNode := nodes[rand.Intn(len(nodes))]
					for toNode.Host == fromNode.Host {
						toNode = nodes[rand.Intn(len(nodes))]
					}

					if fromNode.Host == toNode.Host {
						panic(fmt.Sprintf("fromNode.Host == toNode.Host"))
					}

					// Create a random measurement
					measurement := Measurement{
						Value:              rand.Int63(),
						CreationTimeUnix:   uint64(time.Now().Unix()),
						ExpirationTimeUnix: uint64(time.Now().Add(1 * time.Hour).Unix()),
					}

					// Set the measurement
					network.g.SetMeasurement(fromNode, toNode, fmt.Sprintf("measurement-%d", rand.Intn(5)), measurement)

					// Sleep a bit to avoid too much contention
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	// Start goroutines to perform gossip interactions
	for i := 0; i < numGossipThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			// This goroutine will continue running in both phases
			for {
				select {
				case <-ctx2.Done():
					// Second phase is over, stop gossip interactions
					return
				default:
					// Randomly select a network
					networksMutex.RLock()
					networkIDs := make([]HostPort, 0, len(networks))
					for id := range networks {
						networkIDs = append(networkIDs, id)
					}
					networksMutex.RUnlock()

					if len(networkIDs) == 0 {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					networkID := networkIDs[rand.Intn(len(networkIDs))]

					networksMutex.RLock()
					network := networks[networkID]
					networksMutex.RUnlock()

					// Create a gossip interaction
					gossip := NewGossipInteraction(networkID.host, networkID.port, network)

					// Get candidates for interaction
					candidates := gossip.GetInteractionCandidates(interactionCandidatesCount)

					if len(candidates) == 0 {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					// Get info from the current network
					nodes, measurements := gossip.GetInfo()

					// For each candidate, create a new gossip interaction and handle the info
					for _, candidate := range candidates {
						candidateNetworkID := HostPort{host: candidate.Host, port: candidate.Port}
						networksMutex.RLock()
						candidateNetwork := networks[candidateNetworkID]
						networksMutex.RUnlock()

						if candidateNetwork == nil {
							continue
						}

						// Create a gossip interaction for the candidate network
						candidateGossip := NewGossipInteraction(candidate.Host, candidate.Port, candidateNetwork)

						println(fmt.Sprintf("Spread from %s to %s", networkID.host, candidateNetworkID.host))

						// Handle the info received
						candidateGossip.HandleInfoReceived(nodes, measurements)
					}

					// Sleep a bit to avoid too much contention
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for the first phase to complete
	<-ctx1.Done()
	t.Log("First phase (measurement generation) completed")

	// Wait for the second phase to complete
	<-ctx2.Done()
	t.Log("Second phase (gossip synchronization) completed")

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify that all networks have synchronized
	verifyNetworkSynchronization(t, networks)
}

// Helper function to generate a pool of random Host:Port combinations
func generateHostPortPool(count int) []struct {
	host string
	port uint16
} {
	result := make([]struct {
		host string
		port uint16
	}, 0, count)

	used := make(map[string]struct{})

	for len(result) < count {
		ip := net.IPv4(
			byte(rand.Intn(256)),
			byte(rand.Intn(256)),
			byte(rand.Intn(256)),
			byte(rand.Intn(256)),
		)
		port := uint16(rand.Intn(65535-1024) + 1024)

		key := fmt.Sprintf("%s:%d", ip.String(), port)
		if _, exists := used[key]; exists {
			continue
		}
		used[key] = struct{}{}
		result = append(result, struct {
			host string
			port uint16
		}{
			host: ip.String(),
			port: port,
		})
	}

	return result
}

// Helper function to verify that all networks have synchronized
func verifyNetworkSynchronization(t *testing.T, networks map[HostPort]*DNetwork) {
	t.Log("Verifying network synchronization...")

	// Get the first network as a reference
	var referenceNetwork *DNetwork
	var referenceID HostPort

	for id, network := range networks {
		referenceNetwork = network
		referenceID = id
		break
	}

	if referenceNetwork == nil {
		t.Fatal("No reference network found")
	}

	// Get all nodes and measurements from the reference network
	referenceNetwork.gMu.RLock()
	referenceNodes := referenceNetwork.g.GetNodes()
	referenceMeasurements := referenceNetwork.g.GetMeasurements()
	referenceNetwork.gMu.RUnlock()

	// Create a map of node IDs for easier comparison
	referenceNodeMap := make(map[string]bool)
	for _, node := range referenceNodes {
		referenceNodeMap[MakeNodeSystemId(node.Host, node.Port)] = true
	}

	// Compare each network with the reference network
	for id, network := range networks {
		if id == referenceID {
			continue
		}

		t.Logf("Comparing network %s with reference network %s", id.host, referenceID.host)

		network.gMu.RLock()
		nodes := network.g.GetNodes()
		measurements := network.g.GetMeasurements()
		network.gMu.RUnlock()

		// Check that the number of nodes is the same
		assert.Equal(t, len(referenceNodes), len(nodes),
			"Network %s has different number of nodes than reference network", id)

		// Check that all nodes exist in both networks
		for _, node := range nodes {
			nodeID := MakeNodeSystemId(node.Host, node.Port)
			assert.True(t, referenceNodeMap[nodeID],
				"Node %s exists in network %s but not in reference network", nodeID, id)
		}

		// Check that all measurements are the same and log detailed differences (only differences based on id.host)
		if len(referenceMeasurements) != len(measurements) {
			normalizedRef := make(map[string]interface{})
			for k, v := range referenceMeasurements {
				normalizedRef[k] = v
			}
			normalizedMeasurements := make(map[string]interface{})
			for k, v := range measurements {
				normalizedMeasurements[k] = v
			}

			diffRef := make(map[string]interface{})
			for id, val := range normalizedRef {
				if _, exists := normalizedMeasurements[id]; !exists {
					diffRef[id] = val
				}
			}
			diffActual := make(map[string]interface{})
			for id, val := range normalizedMeasurements {
				if _, exists := normalizedRef[id]; !exists {
					diffActual[id] = val
				}
			}

			t.Errorf("Network %s has different measurements than reference network. Differences:\nIn reference only: %+v\nIn actual only: %+v", id.host, diffRef, diffActual)
		}
	}

	t.Log("All networks have synchronized successfully")
}
