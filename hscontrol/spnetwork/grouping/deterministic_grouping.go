package grouping

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"time"
)

type DeterministicGrouping struct {
	Registry            common.EntityRegistry
	NodeRegistry        *common.TypedRegistry[*entities.Node]
	MeasurementRegistry *common.TypedRegistry[*entities.Measurement]
	GroupRegistry       *common.TypedRegistry[*entities.Group]
	groupingInterval    time.Duration
}

func NewDeterministicGrouping(registry common.EntityRegistry, localNode *entities.Node, groupingInterval time.Duration) (*DeterministicGrouping, error) {
	nodeRegistry := common.NewTypedRegistry[*entities.Node](registry, common.NodeEntityType)

	return &DeterministicGrouping{
		Registry: registry,
	}
}
