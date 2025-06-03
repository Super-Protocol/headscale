package measurer

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"time"
)

// NewUDPPingMeasurerWithDefaults создает UDPPingMeasurer с настройками по умолчанию
func NewUDPPingMeasurerWithDefaults(registry common.EntityRegistry, localNode *entities.Node, listenHost string, listenPort int) (*UDPPingMeasurer, error) {
	config := UDPPingMeasurerConfig{
		ListenHost:      listenHost,
		ListenPort:      listenPort,
		MeasureInterval: 1 * time.Second, // Интервал измерений по умолчанию
	}

	log.Info().
		Str("node_id", localNode.GetID()).
		Str("listen_host", listenHost).
		Int("listen_port", listenPort).
		Msg("creating UDP ping measurer with default settings")

	return NewUDPPingMeasurer(registry, localNode, config)
}
