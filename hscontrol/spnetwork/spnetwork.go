package spnetwork

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/consensus"
	"github.com/juanfont/headscale/hscontrol/spnetwork/grouping"
	"github.com/juanfont/headscale/hscontrol/spnetwork/measurer"
	"github.com/juanfont/headscale/hscontrol/spnetwork/syncer"
	g "github.com/juanfont/headscale/hscontrol/spnetwork/syncer/gossip"
	"sync"
	"time"
)

type SPNetwork struct {
	LocalNode    *entities.Node
	Syncer       syncer.Syncer
	Consensus    consensus.Consensus
	Measurer     measurer.Measurer
	Grouping     grouping.Grouping
	nodeRegistry *common.TypedRegistry[*entities.Node]
	registry     common.EntityRegistry
	mu           sync.Mutex
	running      bool
}

type PkiConfig struct {
	CertFile string
	KeyFile  string
	CaFile   string
}

func NewSPNetwork(localNode *entities.Node, bootstrapNodes []*entities.Node, pkiConfig PkiConfig) (*SPNetwork, error) {
	syncerRegistry := common.NewMemoryEntityRegistry()
	nodeRegistry := common.NewTypedRegistry[*entities.Node](syncerRegistry, common.NodeEntityType)
	err := nodeRegistry.StoreEntity(localNode)
	if err != nil {
		return nil, err
	}

	for _, n := range bootstrapNodes {
		err := nodeRegistry.StoreEntity(n)
		if err != nil {
			return nil, err
		}
	}
	host, _ := localNode.GetHost()
	port, _ := localNode.GetGossipPort()
	transportConfig := g.GrpcTransportConfig{
		ListenHost: host,
		ListenPort: int(port),
		EnableTLS:  true,
		CertFile:   pkiConfig.CertFile,
		KeyFile:    pkiConfig.KeyFile,
		CaFile:     pkiConfig.CaFile,
	}
	syncerTransport, err := g.NewGrpcTransport(syncerRegistry, localNode, transportConfig)
	if err != nil {
		return nil, err
	}
	s := g.NewGossip(syncerRegistry, localNode.ID, syncerTransport, time.Duration(1)*time.Second)

	udpPingPort, _ := localNode.GetUdpPingPort()
	m, err := measurer.NewUDPPingMeasurerWithDefaults(syncerRegistry, localNode, host, int(udpPingPort))
	if err != nil {
		return nil, err
	}

	n := &SPNetwork{
		LocalNode:    localNode,
		Syncer:       s,
		Measurer:     m,
		nodeRegistry: nodeRegistry,
		registry:     syncerRegistry,
	}
	return n, nil
}

func (n *SPNetwork) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return fmt.Errorf("network is already running")
	}

	n.running = true

	err := n.Syncer.Start()
	if err != nil {
		return err
	}

	err = n.Measurer.Start()
	if err != nil {
		return err
	}

	//err = n.Consensus.Join()
	//if err != nil {
	//	return err
	//}
	//
	//
	//err = n.Grouping.Start()
	//if err != nil {
	//	return err
	//}

	return nil
}

func (n *SPNetwork) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return fmt.Errorf("network is already stopped")
	}

	//err := n.Grouping.Stop()
	//if err != nil {
	//	return err
	//}
	//
	//err = n.Consensus.Leave()
	//if err != nil {
	//	return err
	//}
	err := n.Measurer.Stop()
	if err != nil {
		return err
	}

	err = n.Syncer.Stop()
	if err != nil {
		return err
	}

	n.running = false
	return nil
}
