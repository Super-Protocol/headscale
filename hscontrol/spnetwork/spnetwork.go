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
	LocalNode *entities.Node
	Syncer    syncer.Syncer
	Consensus consensus.Consensus
	Measurer  measurer.Measurer
	Grouping  grouping.Grouping
	registry  *common.EntityRegistry
	mu        sync.Mutex
	running   bool
}

type PkiConfig struct {
	CertFile string
	KeyFile  string
	CaFile   string
}

func NewSPNetwork(registry *common.EntityRegistry, localNode *entities.Node, pkiConfig PkiConfig) (*SPNetwork, error) {
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
	syncerTransport, err := g.NewGrpcTransport(registry, localNode, transportConfig)
	if err != nil {
		return nil, err
	}
	s := g.NewGossip(registry, localNode.ID, syncerTransport, time.Duration(100)*time.Millisecond)

	c, err := consensus.NewDeterministicConsensus(s, registry, localNode, time.Duration(200)*time.Millisecond)

	if err != nil {
		return nil, err
	}

	//udpPingPort, _ := localNode.GetUdpPingPort()
	//m, err := measurer.NewUDPPingMeasurerWithDefaults(registry, localNode, host, int(udpPingPort))
	m, err := measurer.NewMonkeyMeasurer(registry, localNode, measurer.MonkeyMeasurerConfig{
		MeasureInterval:            time.Duration(500) * time.Millisecond,
		NewValueProbability:        0.05,
		NodeUnavailableProbability: 0.01,
	})
	if err != nil {
		return nil, err
	}

	grouping, err := grouping.NewDeterministicGrouping(registry, localNode, time.Duration(1)*time.Second)
	if err != nil {
		return nil, err
	}

	n := &SPNetwork{
		LocalNode: localNode,
		Syncer:    s,
		Measurer:  m,
		Grouping:  grouping,
		Consensus: c,
		registry:  registry,
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

	err = n.Consensus.Start()
	if err != nil {
		return err
	}

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

	err := n.Consensus.Stop()
	if err != nil {
		return err
	}

	//err := n.Grouping.Stop()
	//if err != nil {
	//	return err
	//}

	err = n.Measurer.Stop()
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
