package grouping

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"sort"
	"sync"
	"time"
)

type DeterministicGrouping struct {
	registry         common.EntityRegistry
	localNode        *entities.Node
	groupingInterval time.Duration
	mu               sync.Mutex
}

func NewDeterministicGrouping(registry common.EntityRegistry, localNode *entities.Node, groupingInterval time.Duration) (*DeterministicGrouping, error) {

	return &DeterministicGrouping{
		registry:         registry,
		localNode:        localNode,
		groupingInterval: groupingInterval,
	}, nil
}

func (dg *DeterministicGrouping) processGoal(goal *entities.GroupGoal) error {
	dg.mu.Lock()
	defer dg.mu.Unlock()
	nodeId := dg.localNode.GetID()
	goalId := goal.GetID()
	nodeGoalGroups := dg.registry.GetNodeGroupsByGoal(goalId, nodeId)
	nodeInGoalGroup := len(nodeGoalGroups) > 0
	group := entities.NewGroup()
	group.SetGoal(goalId)
	group.AddParticipant(nodeId)
	if nodeInGoalGroup {
		for i, group := range nodeGoalGroups {
			participants := make([]*entities.Node, 0, len(group.Participants))
			for _, participantID := range group.Participants {
				p, err := dg.registry.Node.GetEntity(participantID)
				if err != nil {
					return err
				}
				participants = append(participants, p)
			}
			leader := getDeterministicLeader(participants)
			if leader.GetID() == nodeId {
				// Мы лидеры, так что руководим этой группой

				// Проверка что все ноды состоят только в 1 группе для цели, если нет - кикаем
				// Проверка что количество групп не превышает допустимое
				// Проверка, что все ноды живы
				// Проверка, можно ли добавить какую-то еще ноду в группу, если есть место

				// Надо еще записывать время добавления ноды в группу для того чтобы можно было выдерживать паузу
				// для разрешения конфликтов
			}
		}
	} else {
		groupingCandidates, groupedCandidates, nonGroupedCandidates, ok := dg.registry.GetNodesWithCapabilityForGoal(goalId, nodeId)
		if !ok {
			return nil
		}
		candidatesCountEnough := len(groupingCandidates) >= goal.MinGroupSize
		if candidatesCountEnough {
			// Уже есть ноды с группами, поэтому сначала пытаемся подключить к существующей
			groupsMap := make(map[string]*entities.Group)
			for _, candidate := range groupedCandidates {
				groups, err := dg.registry.GetNodeGroupsByGoal(goalId, candidate.GetID())
				if err != nil {
					return err
				}
				for _, group := range groups {
					groupsMap[group.GetID()] = group
				}
			}
			var uniqueGroups []*entities.Group
			for _, group := range groupsMap {
				uniqueGroups = append(uniqueGroups, group)
			}
			sort.Slice(uniqueGroups, func(i, j int) bool {
				return len(uniqueGroups[i].GetParticipants()) < len(uniqueGroups[j].GetParticipants())
			})

			if len(uniqueGroups) > 0 {
				existingGroups := uniqueGroups[0]
				// Есть незаполненная группа
				if len(existingGroups.GetParticipants()) < goal.GetMaxGroupSize() {
					// По идее лидер нас сам должен увидеть и подключить к группе
					// Поэтому ничего не делаем
					return nil
				} else { // Все группы заполнены - тогда смотрим среди остальных кандидатов мы лидеры?
					leader := getDeterministicLeader(nonGroupedCandidates)
					if leader != nil && leader.GetID() == nodeId {
						err := dg.registry.Group.StoreEntity(group)
						if err != nil {
							return err
						}
					}
				}
			}
		} else {
			return nil
		}
	}
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
