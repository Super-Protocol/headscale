package gossip

import "github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"

type SyncerTransport interface {
	Start() error
	Stop() error
	Sync(targetNode *entities.Node) error
	GetSyncCoef() float32
}

type TransportConfig struct {
	ListenHost string
	ListenPort int
	EnableTLS  bool
	CertFile   string
	KeyFile    string
	CaFile     string
}

type TransportType string
