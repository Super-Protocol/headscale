package spnetwork

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/consensus"
	"github.com/juanfont/headscale/hscontrol/spnetwork/grouping"
	"github.com/juanfont/headscale/hscontrol/spnetwork/measurer"
	"github.com/juanfont/headscale/hscontrol/spnetwork/syncer"
	"sync"
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

func NewSPNetworkManaged(registry *common.EntityRegistry,
	localNode *entities.Node,
	syncer syncer.Syncer,
	consensus consensus.Consensus,
	measurer measurer.Measurer,
	grouping grouping.Grouping) (*SPNetwork, error) {

	n := &SPNetwork{
		LocalNode: localNode,
		Syncer:    syncer,
		Measurer:  measurer,
		Grouping:  grouping,
		Consensus: consensus,
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

	err = n.Grouping.Start()
	if err != nil {
		return err
	}

	return nil
}

func (n *SPNetwork) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return fmt.Errorf("network is already stopped")
	}

	err := n.Grouping.Stop()
	if err != nil {
		return err
	}

	err = n.Consensus.Stop()
	if err != nil {
		return err
	}

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
