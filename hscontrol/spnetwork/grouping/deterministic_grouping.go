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
		// Проверяем здоровье текущих групп
		for _, group := range existingGroups {
			// Исключаем мертвые ноды
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
			// Исключаем ноды которые больше не удовлетворяют условиям
			var participantIDs []string
			for _, participant := range participantsCopy {
				participantIDs = append(participantIDs, participant.ID)
			}
			commonSuitableNodes, _, _, hasEnoughNodes := dg.registry.GetCommonNodesWithCapabilityForGoal(goal.GetID(), participantIDs, math.MaxInt)
			if !hasEnoughNodes {
				// Удаляем группу совсем?
				err := dg.registry.Group.DeleteEntity(group.GetID())
				if err != nil {
					return err
				}
				break
			}
			for _, participant := range participantsCopy {
				// Проверяем, находится ли участник в списке подходящих нод
				found := false
				for _, suitableNode := range commonSuitableNodes {
					if suitableNode.GetID() == participant.ID {
						found = true
						break
					}
				}

				// Если участник не найден в списке подходящих нод, удаляем его из группы
				if !found {
					group.RemoveParticipant(participant.ID)
					log.Debug().
						Str("group_id", group.GetID()).
						Str("participant_id", participant.ID).
						Msg("removed participant that is no longer suitable for this group")
				}
			}
			// Проверяем размеры группы и удаляем лишние
			if len(group.Participants) > goal.MaxGroupSize {
				participantsCopy := make([]entities.Participant, len(group.Participants))
				copy(participantsCopy, group.Participants)

				// Сортируем участников по дате добавления (от новых к старым)
				sort.Slice(participantsCopy, func(i, j int) bool {
					return participantsCopy[i].JoinDateUnix > participantsCopy[j].JoinDateUnix
				})

				// Удаляем участника, который был добавлен последним
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
			// Проверяем размеры группы и добавляем новые
			if len(group.Participants) < goal.MaxGroupSize {
				_, _, commonNonGroupedNodes, _ := dg.registry.GetCommonNodesWithCapabilityForGoal(goal.GetID(), participantIDs, goal.MaxGroupSize*2)
				maxCountToAdd := goal.MaxGroupSize - len(group.Participants)
				for i := 0; i < maxCountToAdd && i < len(commonNonGroupedNodes); i++ {
					group.AddParticipant(commonNonGroupedNodes[i].GetID())
				}
			}
		}
		// Пытаемся собрать новые группы
		nonGroupedNodes, err := dg.registry.GetNonGroupedNodesForGoal(goal.GetID())
		if err != nil {
			log.Error().
				Err(err).
				Str("goal_id", goal.GetID()).
				Msg("ошибка при получении несгруппированных нод")
			return err
		}

		if len(nonGroupedNodes) == 0 {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Msg("нет несгруппированных нод для создания новых групп")
			return nil
		}

		// Проверяем, что есть достаточно нод для возможности создания хотя бы одной группы
		if len(nonGroupedNodes) < goal.GetMinGroupSize() {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Int("non_grouped_nodes", len(nonGroupedNodes)).
				Int("min_required", goal.GetMinGroupSize()).
				Msg("недостаточно несгруппированных нод для создания группы")
			return nil
		}

		// Получаем ID всех несгруппированных нод
		nonGroupedNodeIDs := make([]string, len(nonGroupedNodes))
		for i, node := range nonGroupedNodes {
			nonGroupedNodeIDs[i] = node.GetID()
		}

		// Выполняем GetCommonNodesWithCapabilityForGoal для списка несгруппированных нод
		_, _, commonNonGroupedNodes, hasEnough := dg.registry.GetCommonNodesWithCapabilityForGoal(
			goal.GetID(),
			nonGroupedNodeIDs,
			math.MaxInt)

		log.Debug().
			Str("goal_id", goal.GetID()).
			Int("non_grouped_nodes", len(nonGroupedNodes)).
			Int("common_non_grouped_nodes", len(commonNonGroupedNodes)).
			Bool("has_enough_nodes", hasEnough).
			Msg("получен список совместимых несгруппированных нод")

		// Проверяем, что есть достаточно нод для создания хотя бы одной группы
		if len(commonNonGroupedNodes) < goal.GetMinGroupSize() {
			log.Debug().
				Str("goal_id", goal.GetID()).
				Int("common_non_grouped_nodes", len(commonNonGroupedNodes)).
				Int("min_required", goal.GetMinGroupSize()).
				Msg("недостаточно совместимых нод для создания группы")
			return nil
		}

		// Разделяем список commonNonGroupedNodes на подгруппы размером goal.MaxGroupSize
		maxGroupSize := goal.GetMaxGroupSize()
		minGroupSize := goal.GetMinGroupSize()
		numGroups := (len(commonNonGroupedNodes) + maxGroupSize - 1) / maxGroupSize

		for i := 0; i < numGroups; i++ {
			// Определяем индексы начала и конца для текущей подгруппы
			startIdx := i * maxGroupSize
			endIdx := startIdx + maxGroupSize
			if endIdx > len(commonNonGroupedNodes) {
				endIdx = len(commonNonGroupedNodes)
			}

			// Проверяем, что подгруппа достаточно большая (не меньше MinGroupSize)
			if endIdx-startIdx < minGroupSize {
				log.Debug().
					Str("goal_id", goal.GetID()).
					Int("nodes_in_subgroup", endIdx-startIdx).
					Int("min_required", minGroupSize).
					Msg("пропуск создания группы - недостаточный размер подгруппы")
				continue
			}

			// Создаем новую группу
			newGroup := entities.NewGroup()
			newGroup.SetGoal(goal.GetID())

			// Добавляем ноды в группу
			for j := startIdx; j < endIdx; j++ {
				newGroup.AddParticipant(commonNonGroupedNodes[j].GetID())
			}

			// Сохраняем группу в реестре
			_, err := dg.registry.Group.StoreEntity(newGroup)
			if err != nil {
				log.Error().
					Err(err).
					Str("goal_id", goal.GetID()).
					Str("group_id", newGroup.GetID()).
					Msg("ошибка при сохранении новой группы")
				continue
			}

			log.Info().
				Str("goal_id", goal.GetID()).
				Str("group_id", newGroup.GetID()).
				Int("participants", len(newGroup.GetParticipants())).
				Msg("создана новая группа из несгруппированных нод")
		}

		// Удаляем группы у которых размер меньше минимального
		// GetGroupsByGoal уже возвращает только неудаленные группы
		existingGroups, err = dg.registry.GetGroupsByGoal(goal.GetID())
		if err != nil {
			log.Error().
				Err(err).
				Str("goal_id", goal.GetID()).
				Msg("ошибка при получении групп для проверки минимального размера")
			return err
		}

		for _, group := range existingGroups {
			// Проверяем размер группы и удаляем, если он меньше минимального
			if len(group.GetParticipants()) < goal.GetMinGroupSize() {
				log.Info().
					Str("goal_id", goal.GetID()).
					Str("group_id", group.GetID()).
					Int("participants", len(group.GetParticipants())).
					Int("min_required", goal.GetMinGroupSize()).
					Msg("удаление группы с недостаточным количеством участников")

				// Помечаем группу как удаленную
				group.MarkDeleted()

				// И сохраняем изменения в реестре
				_, err := dg.registry.Group.StoreEntity(group)
				if err != nil {
					log.Error().
						Err(err).
						Str("goal_id", goal.GetID()).
						Str("group_id", group.GetID()).
						Msg("ошибка при удалении группы с недостаточным количеством участников")
				}
			}
		}
	}
	return nil
}

// Start запускает процесс группировки
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

// Stop останавливает процесс группировки
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

// groupingLoop периодически обрабатывает все цели группировки
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

// runGroupingIfNotRunning запускает процесс группировки, если он еще не запущен
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

		// Получаем все цели группировки
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

		// Обрабатываем каждую цель
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
