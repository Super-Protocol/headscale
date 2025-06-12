package grouping

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/consensus"
	"github.com/rs/zerolog/log"
	"math"
	"sort"
	"sync"
	"time"
)

const NodeAliveTimeoutSecs = 3600

type DeterministicGrouping struct {
	registry         *common.EntityRegistry
	localNode        *entities.Node
	leaderSource     consensus.LeaderSource
	groupingInterval time.Duration
	mu               sync.Mutex
	running          bool
	stopChan         chan struct{}
	groupingRunning  bool
}

func NewDeterministicGrouping(registry *common.EntityRegistry, localNode *entities.Node, leaderSource consensus.LeaderSource, groupingInterval time.Duration) (*DeterministicGrouping, error) {
	return &DeterministicGrouping{
		registry:         registry,
		localNode:        localNode,
		leaderSource:     leaderSource,
		groupingInterval: groupingInterval,
		stopChan:         make(chan struct{}),
	}, nil
}

func (dg *DeterministicGrouping) processGoal(goal *entities.GroupGoal) error {
	dg.mu.Lock()
	defer dg.mu.Unlock()
	if dg.leaderSource.IsLeader() {
		existingGroups, err := dg.registry.GetGroupsByGoal(goal.GetID())
		if err != nil {
			return err
		}
		// Check the health of existing groups
		for _, group := range existingGroups {
			// Exclude dead nodes
			participantsCopy := make([]entities.Participant, len(group.Participants))
			copy(participantsCopy, group.Participants)
			for _, participant := range participantsCopy {
				if !dg.registry.IsNodeAlive(participant.ID, goal.InactivityTimeout) {
					group.RemoveParticipant(participant.ID)
					log.Debug().
						Str("group_id", group.GetID()).
						Str("participant_id", participant.ID).
						Msg("removed participant that is not alive")
				}
			}
			// Exclude nodes that no longer meet the conditions
			var participantIDs []string
			for _, participant := range participantsCopy {
				participantIDs = append(participantIDs, participant.ID)
			}
			commonSuitableNodes, _, _, hasEnoughNodes := dg.registry.GetCommonNodesWithCapabilityForGoal(goal.GetID(), participantIDs, math.MaxInt)
			if !hasEnoughNodes {
				// Delete the group completely?
				err := dg.registry.Group.DeleteEntity(group.GetID())
				if err != nil {
					return err
				}
				break
			}
			for _, participant := range participantsCopy {
				// Check if the participant is in the list of suitable nodes
				found := false
				for _, suitableNode := range commonSuitableNodes {
					if suitableNode.GetID() == participant.ID {
						found = true
						break
					}
				}

				// If the participant is not found in the list of suitable nodes, remove it from the group
				if !found {
					group.RemoveParticipant(participant.ID)
					log.Debug().
						Str("group_id", group.GetID()).
						Str("participant_id", participant.ID).
						Msg("removed participant that is no longer suitable for this group")
				}
			}
			// Check group size and remove excess participants
			if len(group.Participants) > goal.MaxGroupSize {
				participantsCopy := make([]entities.Participant, len(group.Participants))
				copy(participantsCopy, group.Participants)

				// Sort participants by join date (from newest to oldest)
				sort.Slice(participantsCopy, func(i, j int) bool {
					return participantsCopy[i].JoinDateUnix > participantsCopy[j].JoinDateUnix
				})

				// Remove the participant that was added most recently
				if len(participantsCopy) > 0 {
					participantToRemove := participantsCopy[0]
					group.RemoveParticipant(participantToRemove.ID)
					log.Debug().
						Str("group_id", group.GetID()).
						Str("participant_id", participantToRemove.ID).
						Int64("join_date", participantToRemove.JoinDateUnix).
						Msg("removed most recently added participant to maintain maximum group size")
				}
			}
			// Check group size and add new participants
			if len(group.Participants) < goal.MaxGroupSize {
				_, _, commonNonGroupedNodes, _ := dg.registry.GetCommonNodesWithCapabilityForGoal(goal.GetID(), participantIDs, goal.MaxGroupSize*2)
				maxCountToAdd := goal.MaxGroupSize - len(group.Participants)
				for i := 0; i < maxCountToAdd && i < len(commonNonGroupedNodes); i++ {
					group.AddParticipant(commonNonGroupedNodes[i].GetID())
				}
			}
		}
		// Try to form new groups
		nonGroupedNodes, err := dg.registry.GetNonGroupedNodesForGoal(goal.GetID())
		if err != nil {
			log.Error().
				Err(err).
				Str("goal_id", goal.GetID()).
				Msg("error getting non-grouped nodes")
			return err
		}

		if len(nonGroupedNodes) == 0 {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Msg("no non-grouped nodes available for creating new groups")
			return nil
		}

		// Check that there are enough nodes to create at least one group
		if len(nonGroupedNodes) < goal.GetMinGroupSize() {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Int("non_grouped_nodes", len(nonGroupedNodes)).
				Int("min_required", goal.GetMinGroupSize()).
				Msg("not enough non-grouped nodes to create a group")
			return nil
		}

		// Get IDs of all non-grouped nodes
		nonGroupedNodeIDs := make([]string, len(nonGroupedNodes))
		for i, node := range nonGroupedNodes {
			nonGroupedNodeIDs[i] = node.GetID()
		}

		// Execute GetCommonNodesWithCapabilityForGoal for the list of non-grouped nodes
		_, _, commonNonGroupedNodes, hasEnough := dg.registry.GetCommonNodesWithCapabilityForGoal(
			goal.GetID(),
			nonGroupedNodeIDs,
			math.MaxInt)

		log.Debug().
			Str("goal_id", goal.GetID()).
			Int("non_grouped_nodes", len(nonGroupedNodes)).
			Int("common_non_grouped_nodes", len(commonNonGroupedNodes)).
			Bool("has_enough_nodes", hasEnough).
			Msg("received list of compatible non-grouped nodes")

		// Check that there are enough nodes to create at least one group
		if len(commonNonGroupedNodes) < goal.GetMinGroupSize() {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Int("common_non_grouped_nodes", len(commonNonGroupedNodes)).
				Int("min_required", goal.GetMinGroupSize()).
				Msg("not enough compatible nodes to create a group")
			return nil
		}

		// Split the commonNonGroupedNodes list into subgroups of size goal.MaxGroupSize
		maxGroupSize := goal.GetMaxGroupSize()
		minGroupSize := goal.GetMinGroupSize()
		numGroups := (len(commonNonGroupedNodes) + maxGroupSize - 1) / maxGroupSize

		for i := 0; i < numGroups; i++ {
			// Define start and end indexes for the current subgroup
			startIdx := i * maxGroupSize
			endIdx := startIdx + maxGroupSize
			if endIdx > len(commonNonGroupedNodes) {
				endIdx = len(commonNonGroupedNodes)
			}

			// Check that the subgroup is large enough (not less than MinGroupSize)
			if endIdx-startIdx < minGroupSize {
				log.Debug().
					Str("goal_id", goal.GetID()).
					Int("nodes_in_subgroup", endIdx-startIdx).
					Int("min_required", minGroupSize).
					Msg("skipping group creation - insufficient subgroup size")
				continue
			}

			// Create a new group
			newGroup := entities.NewGroup()
			newGroup.SetGoal(goal.GetID())

			// Add nodes to the group
			for j := startIdx; j < endIdx; j++ {
				newGroup.AddParticipant(commonNonGroupedNodes[j].GetID())
			}

			// Save the group in the registry
			_, err := dg.registry.Group.StoreEntity(newGroup)
			if err != nil {
				log.Error().
					Err(err).
					Str("goal_id", goal.GetID()).
					Str("group_id", newGroup.GetID()).
					Msg("error saving new group")
				continue
			}

			log.Info().
				Str("goal_id", goal.GetID()).
				Str("group_id", newGroup.GetID()).
				Int("participants", len(newGroup.GetParticipants())).
				Msg("created new group from non-grouped nodes")
		}

		// Remove groups whose size is less than the minimum
		// GetGroupsByGoal already returns only non-deleted groups
		existingGroups, err = dg.registry.GetGroupsByGoal(goal.GetID())
		if err != nil {
			log.Error().
				Err(err).
				Str("goal_id", goal.GetID()).
				Msg("error getting groups for minimum size check")
			return err
		}

		for _, group := range existingGroups {
			// Check the group size and remove it if it's less than the minimum
			if len(group.GetParticipants()) < goal.GetMinGroupSize() {
				log.Info().
					Str("goal_id", goal.GetID()).
					Str("group_id", group.GetID()).
					Int("participants", len(group.GetParticipants())).
					Int("min_required", goal.GetMinGroupSize()).
					Msg("removing group with insufficient number of participants")

				// Mark the group as deleted
				group.MarkDeleted()

				// And save the changes in the registry
				_, err := dg.registry.Group.StoreEntity(group)
				if err != nil {
					log.Error().
						Err(err).
						Str("goal_id", goal.GetID()).
						Str("group_id", group.GetID()).
						Msg("error when removing group with insufficient number of participants")
				}
			}
		}
	}
	return nil
}

// Start initiates the grouping process
func (dg *DeterministicGrouping) Start() error {
	dg.mu.Lock()
	defer dg.mu.Unlock()

	if dg.running {
		log.Warn().Str("node_id", dg.localNode.GetID()).Msg("grouping already running")
		return fmt.Errorf("grouping already running")
	}

	log.Info().
		Str("node_id", dg.localNode.GetID()).
		Msg("starting deterministic grouping")

	go dg.groupingLoop()

	dg.running = true
	log.Info().
		Str("node_id", dg.localNode.GetID()).
		Msg("deterministic grouping started")

	return nil
}

// Stop terminates the grouping process
func (dg *DeterministicGrouping) Stop() error {
	dg.mu.Lock()
	defer dg.mu.Unlock()

	if !dg.running {
		log.Warn().Str("node_id", dg.localNode.GetID()).Msg("grouping already stopped")
		return fmt.Errorf("grouping already stopped")
	}

	log.Info().Str("node_id", dg.localNode.GetID()).Msg("stopping deterministic grouping")

	close(dg.stopChan)

	dg.running = false
	log.Info().Str("node_id", dg.localNode.GetID()).Msg("deterministic grouping stopped")

	return nil
}

// groupingLoop periodically processes all grouping goals
func (dg *DeterministicGrouping) groupingLoop() {
	ticker := time.NewTicker(dg.groupingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := dg.runGroupingIfNotRunning()
			if err != nil {
				return
			}
		case <-dg.stopChan:
			return
		}
	}
}

// runGroupingIfNotRunning starts the grouping process if it's not already running
func (dg *DeterministicGrouping) runGroupingIfNotRunning() error {
	dg.mu.Lock()
	if dg.groupingRunning {
		dg.mu.Unlock()
		log.Debug().
			Str("node_id", dg.localNode.GetID()).
			Msg("skipping grouping run as previous is still in progress")
		return nil
	}
	dg.groupingRunning = true
	dg.mu.Unlock()

	go func() {
		defer func() {
			dg.mu.Lock()
			dg.groupingRunning = false
			dg.mu.Unlock()
		}()

		// Get all grouping goals
		goals, err := dg.registry.GroupGoal.GetAllEntities()
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", dg.localNode.GetID()).
				Msg("error getting group goals")
			return
		}

		if len(goals) == 0 {
			return
		}

		// Process each goal
		for _, goal := range goals {
			err := dg.processGoal(goal)
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", dg.localNode.GetID()).
					Str("goal_id", goal.GetID()).
					Msg("error processing group goal")
			}
		}
	}()

	return nil
}

func getDeterministicLeader(nodes []*entities.Node) *entities.Node {
	if len(nodes) == 0 {
		return nil
	}

	leader := nodes[0]
	for _, node := range nodes {
		if node.GetID() < leader.GetID() {
			leader = node
		}
	}

	return leader
}
