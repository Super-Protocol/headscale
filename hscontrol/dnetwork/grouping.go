package dnetwork

import (
	"fmt"
	pb "github.com/juanfont/headscale/gen/go/dnetwork/v1"
	"github.com/rs/zerolog/log"
	"slices"
	"sort"
	"sync"
	"time"
)

// GroupConfig defines the configuration for a group
type GroupConfig struct {
	Name     string          `json:"name"` // Group name
	Size     GroupSize       `json:"size"` // Min and max size of the group
	Criteria []GroupCriteria `json:"measurements"`
}

// GroupSize defines the size constraints for a group
type GroupSize struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// GroupCriteria defines a single measurement condition for grouping
type GroupCriteria struct {
	Name      string `json:"name"`      // Measurement name (e.g., latency, bandwidth)
	Condition string `json:"condition"` // Condition (e.g., min, max, lt, gt, etc.)
	Value     *int64 `json:"value"`     // Optional value for comparison (nil for min/max)
}

type DNetworkGrouping struct {
	mainNode     DNode
	targetGroups []GroupConfig
	n            *DNetwork
	challengeMu  sync.RWMutex
}

func NewDNetworkGrouping(n *DNetwork, mainNode DNode, targetGroups []GroupConfig) *DNetworkGrouping {
	return &DNetworkGrouping{
		n:            n,
		mainNode:     mainNode,
		targetGroups: targetGroups,
	}
}

func (d *DNetworkGrouping) ResolveGroupJoinRequest(fromNode DNode, groupName string, groupId int64, nodesConnected []string) bool {
	// find all group measurements of main node
	// check if requesting node is in candidates list
	// check my info about group size (it may differ from requesting node)
	// accept request if everything is ok

	d.challengeMu.RLock()
	defer d.challengeMu.RUnlock()
	idx := slices.IndexFunc(d.targetGroups, func(g GroupConfig) bool {
		return g.Name == groupName
	})

	if idx == -1 {
		log.Debug().Msgf("Current node doesn't have intention to join group %s", groupName)
		// We don't have intention to join this group
		return false
	}

	targetGroup := d.targetGroups[idx]

	localNodes := d.GetNodesOfGroup(groupName)
	localNodesConnected := make([]string, len(localNodes))
	for i, node := range localNodes {
		localNodesConnected[i] = node.SystemID()
	}

	unique := make(map[string]struct{})
	for _, id := range localNodesConnected {
		unique[id] = struct{}{}
	}
	for _, id := range nodesConnected {
		unique[id] = struct{}{}
	}
	uniqueCount := len(unique)

	println("Unique count: ", uniqueCount)

	if uniqueCount > targetGroup.Size.Max {
		log.Debug().Msgf("Can't join to group %s (%d) because merging groups produce more than Max count of nodes", groupName, groupId)
		return false
	}

	if len(localNodes) >= targetGroup.Size.Max {
		log.Debug().Msgf("Group %s already has maximum count of nodes (%d)", groupName, len(localNodes))
		return false
	}

	if d.IsGroupFulfilled(targetGroup) {
		log.Debug().Msgf("Group already have enough nodes %s", groupName)
		return false
	}

	candidates, err := d.GetGroupingCandidates(targetGroup)

	if err != nil {
		log.Debug().Msgf("Can't fulfill group candidates for group %s", groupName)
		// At this moment we can't see we can build needed group with someone
		return false
	}

	log.Debug().Msgf("Current candidates for group %s: %v", groupName, candidates)

	cIdx := slices.IndexFunc(candidates, func(node DNode) bool {
		return node.IsLike(&fromNode)
	})

	if cIdx == -1 {
		log.Debug().Msgf("Requesting node is not in our candidates list for group %s", groupName)
		// Requesting node is not in our candidates list
		return false
	}

	currentGroups := d.n.GetNodeOutgoingMeasurementsByName(d.mainNode, getMeasurementNameForGroup(groupName))

	canJoin := true
	for _, group := range currentGroups {
		if group.Value != 0 {
			canJoin = false
			break
		}
	}

	if !canJoin {
		log.Debug().Msgf("Can't join to group %s (%d) because current node already joined in group", groupName, groupId)
		return false
	}

	return true

}

func (d *DNetworkGrouping) GetNodesOfGroup(groupName string) []DNode {

	groupMeasurementName := getMeasurementNameForGroup(groupName)
	visited := make(map[DNode]bool)
	var result []DNode

	var dfs func(node DNode)
	dfs = func(node DNode) {
		if visited[node] {
			return
		}
		visited[node] = true

		measurements := d.n.GetNodeOutgoingMeasurementsByName(node, groupMeasurementName)
		for targetNode, m := range measurements {
			if m.Value == 0 {
				continue
			}
			if !visited[targetNode] {
				result = append(result, targetNode)
				dfs(targetNode)
			}
		}
	}

	dfs(d.mainNode)
	return result
}

func (d *DNetworkGrouping) IsGroupFulfilled(group GroupConfig) bool {
	currentNodes := d.GetNodesOfGroup(group.Name)
	return len(currentNodes) == group.Size.Max
}

// GetGroupingCandidates selects nodes that meet the group criteria
func (d *DNetworkGrouping) GetGroupingCandidates(group GroupConfig) ([]DNode, error) {
	d.challengeMu.RLock()
	defer d.challengeMu.RUnlock()
	n := d.n
	mainNode := d.mainNode

	nodes := n.GetAllNodes()

	groupNodes := d.GetNodesOfGroup(group.Name)

	candidates := slices.DeleteFunc(append([]DNode(nil), nodes...), func(node DNode) bool {
		if node.IsLike(&mainNode) {
			return true
		}
		idx := slices.IndexFunc(groupNodes, func(n DNode) bool {
			return n == node
		})
		// Node already in this group
		if idx != -1 {
			return true
		}
		hasAllMeasurements := true
		for _, criteria := range group.Criteria {
			_, exists := n.g.GetMeasurement(mainNode, node, criteria.Name)
			if !exists {
				log.Debug().Msgf("Node %s doesn't have measurement %s", node.SystemID(), criteria.Name)
				hasAllMeasurements = false
				break
			}
		}
		return !hasAllMeasurements
	})

	for _, criteria := range group.Criteria {
		switch criteria.Condition {
		case "min":
			sort.SliceStable(candidates, func(i, j int) bool {
				m1, exists1 := n.g.GetMeasurement(mainNode, candidates[i], criteria.Name)
				m2, exists2 := n.g.GetMeasurement(mainNode, candidates[j], criteria.Name)

				if !exists1 {
					return false
				}
				if !exists2 {
					return true
				}
				return m1.Value < m2.Value
			})
		case "max":
			sort.SliceStable(candidates, func(i, j int) bool {
				m1, exists1 := n.g.GetMeasurement(mainNode, candidates[i], criteria.Name)
				m2, exists2 := n.g.GetMeasurement(mainNode, candidates[j], criteria.Name)

				if !exists1 {
					return false
				}
				if !exists2 {
					return true
				}
				return m1.Value > m2.Value
			})

		default:
			// Filter nodes based on other conditions (e.g., lt, gt, eq, etc.)
			var filtered []DNode
			for _, node := range candidates {
				m, exists := n.g.GetMeasurement(mainNode, node, criteria.Name)
				if !exists {
					continue
				}
				if evaluateCondition(m.Value, criteria.Condition, *criteria.Value) {
					filtered = append(filtered, node)
				}
			}
			candidates = filtered
		}
	}

	currentGroupSize := len(groupNodes) + 1

	maxToAdd := group.Size.Max - currentGroupSize
	if maxToAdd <= 0 {
		return nil, fmt.Errorf("group already has enough nodes: %d", group.Size.Max)
	}

	// Apply size constraints
	if len(candidates)+1 < group.Size.Min {
		return nil, fmt.Errorf("not enough nodes to satisfy minimum group size: required %d, found %d", group.Size.Min, len(candidates))
	}
	//if len(candidates) > group.Size.Max {
	//	candidates = candidates[:group.Size.Max] // Trim to max size
	//}

	return candidates, nil
}

// evaluateCondition evaluates a condition against a measurement value
func evaluateCondition(value int64, condition string, target int64) bool {
	switch condition {
	case "lt":
		return value < target
	case "gt":
		return value > target
	case "lte":
		return value <= target
	case "gte":
		return value >= target
	case "eq":
		return value == target
	case "neq":
		return value != target
	default:
		return false
	}
}

func getMeasurementNameForGroup(groupName string) string {
	return fmt.Sprintf("group:%s", groupName)
}

// convertProtoToGroupConfig converts a protobuf GroupConfig to a Go GroupConfig struct
func convertProtoToGroupConfig(protoConfig *pb.GroupConfig) GroupConfig {
	size := GroupSize{
		Min: int(protoConfig.Size.Min),
		Max: int(protoConfig.Size.Max),
	}

	measurements := make([]GroupCriteria, len(protoConfig.Measurements))
	for i, protoCriteria := range protoConfig.Measurements {
		measurements[i] = GroupCriteria{
			Name:      protoCriteria.Name,
			Condition: protoCriteria.Condition,
			Value:     &protoCriteria.Value,
		}
	}

	return GroupConfig{
		Name:     protoConfig.Name,
		Size:     size,
		Criteria: measurements,
	}
}

func (d *DNetworkGrouping) addNodeToGroup(node DNode, groupName string, groupId int64) {
	d.n.g.SetMeasurement(d.mainNode, node, getMeasurementNameForGroup(groupName), Measurement{
		Value:            groupId,
		CreationTimeUnix: uint64(time.Now().Unix()),
	})
}
func (d *DNetworkGrouping) addNodeToGroupById(id string, groupName string, groupId int64) {
	node, ok := d.n.g.GetNodeById(id)
	if ok {
		d.n.g.SetMeasurement(d.mainNode, node, getMeasurementNameForGroup(groupName), Measurement{
			Value:            groupId,
			CreationTimeUnix: uint64(time.Now().Unix()),
		})
	} else {
		panic("addNodeToGroupById for unknown node: " + id)
	}
}

func (d *DNetworkGrouping) removeNodeFromGroup(node DNode, groupName string) {
	d.n.g.SetMeasurement(d.mainNode, node, getMeasurementNameForGroup(groupName), Measurement{
		Value:            0,
		CreationTimeUnix: uint64(time.Now().Unix()),
	})
}
