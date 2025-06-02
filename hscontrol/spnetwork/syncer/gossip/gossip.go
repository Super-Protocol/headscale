package gossip

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"math/rand"
	"sync"
	"time"
)

type Gossip struct {
	Registry     common.EntityRegistry
	NodeRegistry *common.TypedRegistry[*entities.Node]
	Transport    SyncerTransport
	localNodeID  string
	mu           sync.Mutex
	syncInterval time.Duration
	stopChan     chan struct{}
	syncRunning  bool
}

func NewGossip(registry common.EntityRegistry, localNodeID string, transport SyncerTransport, syncInterval time.Duration) *Gossip {
	g := &Gossip{
		Registry:     registry,
		NodeRegistry: common.NewTypedRegistry[*entities.Node](registry, common.NodeEntityType),
		Transport:    transport,
		localNodeID:  localNodeID,
		syncInterval: syncInterval,
		stopChan:     make(chan struct{}),
	}
	return g
}

func (g *Gossip) Start() error {
	err := g.Transport.Start()
	if err != nil {
		return err
	}

	go g.syncLoop()

	return nil
}

func (g *Gossip) Stop() error {
	close(g.stopChan)
	return g.Transport.Stop()
}

func (g *Gossip) syncLoop() {
	ticker := time.NewTicker(g.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			g.runSyncIfNotRunning()
		case <-g.stopChan:
			return
		}
	}
}

func (g *Gossip) runSyncIfNotRunning() error {
	g.mu.Lock()
	if g.syncRunning {
		g.mu.Unlock()
		return nil
	}
	g.syncRunning = true
	g.mu.Unlock()

	go func() {
		nodes, err := g.NodeRegistry.GetAllEntities()
		if err != nil {
			log.Err(err)
			return
		}

		log.Debug().
			Str("node_id", g.localNodeID).
			Msgf("gossip all nodes: %v", nodes)

		if len(nodes) == 0 {
			g.mu.Lock()
			g.syncRunning = false
			g.mu.Unlock()
			return
		}

		var remoteNodes []*entities.Node
		for _, n := range nodes {
			if n.GetID() != g.localNodeID {
				remoteNodes = append(remoteNodes, n)
			}
		}

		log.Debug().
			Str("node_id", g.localNodeID).
			Msgf("gossip remoteNodes: %v", remoteNodes)

		if len(remoteNodes) == 0 {
			g.mu.Lock()
			g.syncRunning = false
			g.mu.Unlock()
			return
		}

		randomIndex := rand.Intn(len(remoteNodes))
		selectedNode := remoteNodes[randomIndex]

		log.Debug().
			Str("node_id", g.localNodeID).
			Msgf("gossip selectedNode: %v", selectedNode)

		_ = g.Transport.Sync(selectedNode)

		g.mu.Lock()
		g.syncRunning = false
		g.mu.Unlock()
	}()
	return nil
}
