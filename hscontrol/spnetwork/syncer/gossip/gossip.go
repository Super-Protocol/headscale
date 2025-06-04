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
	entityRegistry *common.EntityRegistry
	Transport      SyncerTransport
	localNodeID    string
	mu             sync.Mutex
	syncInterval   time.Duration
	stopChan       chan struct{}
	syncRunning    bool
}

func NewGossip(entityRegistry *common.EntityRegistry, localNodeID string, transport SyncerTransport, syncInterval time.Duration) *Gossip {
	g := &Gossip{
		entityRegistry: entityRegistry,
		Transport:      transport,
		localNodeID:    localNodeID,
		syncInterval:   syncInterval,
		stopChan:       make(chan struct{}),
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
		nodes, err := g.entityRegistry.Node.GetAllEntities()
		if err != nil {
			log.Err(err).
				Str("error", "error getting nodes").
				Msg("failed to get nodes")
			g.mu.Lock()
			g.syncRunning = false
			g.mu.Unlock()
			return
		}

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

		if len(remoteNodes) == 0 {
			g.mu.Lock()
			g.syncRunning = false
			g.mu.Unlock()
			return
		}

		randomIndex := rand.Intn(len(remoteNodes))
		selectedNode := remoteNodes[randomIndex]

		_ = g.Transport.Sync(selectedNode)

		g.mu.Lock()
		g.syncRunning = false
		g.mu.Unlock()
	}()
	return nil
}
