package common

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"math"
	"sort"
	"time"
)

type EntityRegistry struct {
	baseRegistry Registry
	Node         *TypedRegistry[*entities.Node]
	Measurement  *TypedRegistry[*entities.Measurement]
	Group        *TypedRegistry[*entities.Group]
	GroupGoal    *TypedRegistry[*entities.GroupGoal]
}

// NewEntityRegistry creates a new instance of Registry
func NewEntityRegistry(baseRegistry Registry) *EntityRegistry {
	return &EntityRegistry{
		baseRegistry: baseRegistry,
		Node:         NewTypedRegistry[*entities.Node](baseRegistry, NodeEntityType),
		Measurement:  NewTypedRegistry[*entities.Measurement](baseRegistry, MeasurementEntityType),
		Group:        NewTypedRegistry[*entities.Group](baseRegistry, GroupEntityType),
		GroupGoal:    NewTypedRegistry[*entities.GroupGoal](baseRegistry, GroupGoalEntityType),
	}
}

// GetNodeLastSeenTime returns the time when the node with the specified ID was last online.
// The time is determined as the time of the last measurement with the LatencyClass type, in which
// the node with the passed ID is the target.
// If there are no such measurements, a zero time and an error are returned.
func (er *EntityRegistry) GetNodeLastSeenTime(nodeID string) (time.Time, error) {
	// Get all measurements from the registry
	measurements, err := er.Measurement.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", nodeID).
			Msg("error getting measurements")
		return time.Time{}, fmt.Errorf("error getting measurements: %w", err)
	}

	var lastSeenUnix int64 = 0
	found := false

	// Iterate through all measurements and look for the latest one where nodeID is the target and type is LatencyClass
	for _, measurement := range measurements {
		if measurement.Target == nodeID &&
			measurement.Type == entities.LatencyClass &&
			!measurement.IsDeleted() &&
			measurement.DateUnix > lastSeenUnix {
			lastSeenUnix = measurement.DateUnix
			found = true
		}
	}

	if !found {
		return time.Time{}, fmt.Errorf("measurements for node %s not found", nodeID)
	}

	// Convert Unix timestamp to time.Time
	lastSeenTime := time.Unix(lastSeenUnix, 0)

	log.Debug().
		Str("node_id", nodeID).
		Time("last_seen", lastSeenTime).
		Msg("found the time when the node was last online")

	return lastSeenTime, nil
}

// IsNodeAlive checks if the node with the specified ID is considered alive based on the timeout.
// It returns true if the node was seen within the specified timeout period, false otherwise.
// If there's an error retrieving the last seen time, it returns false.
func (er *EntityRegistry) IsNodeAlive(nodeID string, timeoutSecs int) bool {
	lastSeenTime, err := er.GetNodeLastSeenTime(nodeID)
	if err != nil {
		return false
	}

	timeoutDuration := time.Duration(timeoutSecs) * time.Second
	isAlive := time.Since(lastSeenTime) <= timeoutDuration

	log.Debug().
		Str("node_id", nodeID).
		Int("timeout_secs", timeoutSecs).
		Time("last_seen", lastSeenTime).
		Bool("is_alive", isAlive).
		Msg("node alive status checked")

	return isAlive
}

// GetNodesWithCapabilityForGoal возвращает кортеж, содержащий:
// 1. Все подходящие ноды
// 2. Список нод в группах с данной целью
// 3. Список нод, не входящих в группы с данной целью
// А также булево значение, указывающее, достаточно ли найденных нод
// для формирования группы (не меньше MinGroupSize).
func (er *EntityRegistry) GetNodesWithCapabilityForGoal(goalId string, nodeId string) ([]*entities.Node, []*entities.Node, []*entities.Node, bool) {
	// Получаем GroupGoal по goalId
	groupGoal, err := er.GroupGoal.GetEntity(goalId)
	if err != nil {
		log.Error().
			Err(err).
			Str("goal_id", goalId).
			Msg("error getting group goal")
		return nil, nil, nil, false
	}

	if groupGoal.IsDeleted() {
		log.Error().
			Str("goal_id", goalId).
			Msg("group goal is deleted")
		return nil, nil, nil, false
	}

	// Получаем ноду по nodeId
	node, err := er.Node.GetEntity(nodeId)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", nodeId).
			Msg("error getting node")
		return nil, nil, nil, false
	}

	if node.IsDeleted() {
		log.Error().
			Str("node_id", nodeId).
			Msg("node is deleted")
		return nil, nil, nil, false
	}

	// Получаем все ноды
	allNodes, err := er.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Msg("error getting all nodes")
		return nil, nil, nil, false
	}

	// Критерии для фильтрации нод
	criteria := groupGoal.GetDimensionCriteria()
	if len(criteria) == 0 {
		log.Warn().
			Str("goal_id", goalId).
			Msg("group goal has no dimension criteria")
		return nil, nil, nil, false
	}

	// Получаем информацию о нодах, которые уже состоят в группах с данной целью
	groupedNodes := make(map[string]bool)

	// Получаем все группы
	groups, err := er.Group.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Msg("error getting all groups")
		return nil, nil, nil, false
	}

	// Собираем список нод, которые уже состоят в группах с данной целью
	for _, group := range groups {
		if group.IsDeleted() {
			continue
		}

		if group.GetGoal() == goalId {
			for _, participantId := range group.GetParticipants() {
				groupedNodes[participantId] = true
			}
		}
	}

	log.Debug().
		Str("goal_id", goalId).
		Int("grouped_nodes", len(groupedNodes)).
		Msg("found nodes already in groups with this goal")

	// Создаем слайс для хранения подходящих нод и их показателей соответствия
	type nodeWithScore struct {
		node  *entities.Node
		score float64
	}
	candidateNodes := make([]nodeWithScore, 0)

	// Получаем все измерения для последующей фильтрации
	measurements, err := er.Measurement.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Msg("error getting measurements")
		return nil, nil, nil, false
	}

	// Итерируемся по всем нодам, исключая исходную и удаленные
	for _, candidateNode := range allNodes {
		// Пропускаем текущую ноду
		if candidateNode.GetID() == nodeId || candidateNode.IsDeleted() {
			continue
		}

		// Проверяем соответствие каждому критерию
		matchesAllCriteria := true
		totalScore := 0.0

		for _, criterion := range criteria {
			// Поиск подходящих измерений для текущего критерия между двумя нодами
			var relevantMeasurements []*entities.Measurement
			for _, m := range measurements {
				if m.IsDeleted() {
					continue
				}
				if m.Type == criterion.Type &&
					((m.Owner == nodeId && m.Target == candidateNode.GetID()) ||
						(m.Owner == candidateNode.GetID() && m.Target == nodeId)) {
					relevantMeasurements = append(relevantMeasurements, m)
				}
			}

			// Если нет измерений для данного критерия, считаем, что нода не подходит
			if len(relevantMeasurements) == 0 {
				matchesAllCriteria = false
				break
			}

			// Находим самое свежее измерение
			var latestMeasurement *entities.Measurement
			for _, m := range relevantMeasurements {
				if latestMeasurement == nil || m.DateUnix > latestMeasurement.DateUnix {
					latestMeasurement = m
				}
			}

			if latestMeasurement == nil {
				matchesAllCriteria = false
				break
			}

			// Проверяем соответствие критерию в зависимости от типа условия
			switch criterion.Condition {
			case entities.ConditionLessThan:
				if len(criterion.Values) > 0 && latestMeasurement.Value >= criterion.Values[0] {
					matchesAllCriteria = false
				} else {
					// Для сортировки: чем меньше значение, тем лучше соответствие
					score := 1.0 / (1.0 + latestMeasurement.Value)
					totalScore += score
				}

			case entities.ConditionGreaterThan:
				if len(criterion.Values) > 0 && latestMeasurement.Value <= criterion.Values[0] {
					matchesAllCriteria = false
				} else {
					// Для сортировки: чем больше значение, тем лучше соответствие
					score := latestMeasurement.Value
					totalScore += score
				}

			case entities.ConditionBetween:
				if len(criterion.Values) >= 2 &&
					(latestMeasurement.Value < criterion.Values[0] || latestMeasurement.Value > criterion.Values[1]) {
					matchesAllCriteria = false
				} else {
					// Для сортировки: чем ближе к середине диапазона, тем лучше соответствие
					if len(criterion.Values) >= 2 {
						midpoint := (criterion.Values[0] + criterion.Values[1]) / 2
						distance := math.Abs(latestMeasurement.Value - midpoint)
						maxDistance := (criterion.Values[1] - criterion.Values[0]) / 2
						score := 1.0 - (distance / maxDistance)
						totalScore += score
					}
				}

			case entities.ConditionMin, entities.ConditionMax:
				// Для min/max просто сохраняем значение, сортировка будет выполнена позже
				totalScore += latestMeasurement.Value
			}

			// Если не соответствует хотя бы одному критерию, пропускаем эту ноду
			if !matchesAllCriteria {
				break
			}
		}

		// Если нода соответствует всем критериям, добавляем ее в кандидаты
		if matchesAllCriteria {
			candidateNodes = append(candidateNodes, nodeWithScore{
				node:  candidateNode,
				score: totalScore,
			})
		}
	}

	// Если нет подходящих нод или их меньше минимального размера группы
	minGroupSize := groupGoal.GetMinGroupSize()
	if len(candidateNodes) < minGroupSize-1 { // -1 потому что текущая нода тоже в группе
		log.Debug().
			Str("goal_id", goalId).
			Str("node_id", nodeId).
			Int("required_min", minGroupSize).
			Int("found", len(candidateNodes)+1). // +1 включая текущую ноду
			Msg("not enough nodes found for group")
		return nil, nil, nil, false
	}

	// Сортируем ноды в зависимости от критериев
	hasSortCriteria := false
	for _, criterion := range criteria {
		if criterion.Condition == entities.ConditionMin || criterion.Condition == entities.ConditionMax {
			hasSortCriteria = true
			break
		}
	}

	if hasSortCriteria {
		// Сортируем по критериям min/max
		for _, criterion := range criteria {
			if criterion.Condition == entities.ConditionMin {
				sort.Slice(candidateNodes, func(i, j int) bool {
					return candidateNodes[i].score < candidateNodes[j].score
				})
			} else if criterion.Condition == entities.ConditionMax {
				sort.Slice(candidateNodes, func(i, j int) bool {
					return candidateNodes[i].score > candidateNodes[j].score
				})
			}
		}
	} else {
		// По умолчанию сортируем по убыванию общего соответствия
		sort.Slice(candidateNodes, func(i, j int) bool {
			return candidateNodes[i].score > candidateNodes[j].score
		})
	}

	// Ограничиваем количество возвращаемых нод максимальным размером группы
	maxGroupSize := groupGoal.GetMaxGroupSize()
	maxResult := maxGroupSize - 1 // -1 потому что текущая нода тоже в группе
	if maxResult > len(candidateNodes) {
		maxResult = len(candidateNodes)
	}

	// Формируем результаты
	allSuitableNodes := make([]*entities.Node, maxResult)
	groupedSuitableNodes := make([]*entities.Node, 0)
	nonGroupedSuitableNodes := make([]*entities.Node, 0)

	for i := 0; i < maxResult; i++ {
		node := candidateNodes[i].node
		allSuitableNodes[i] = node

		if groupedNodes[node.GetID()] {
			groupedSuitableNodes = append(groupedSuitableNodes, node)
		}
	}

	for i := 0; i < len(candidateNodes) && len(nonGroupedSuitableNodes) < maxResult; i++ {
		node := candidateNodes[i].node
		if groupedNodes[node.GetID()] {
			continue
		}
		nonGroupedSuitableNodes = append(nonGroupedSuitableNodes, node)
	}

	log.Debug().
		Str("goal_id", goalId).
		Str("node_id", nodeId).
		Int("all_suitable", len(allSuitableNodes)).
		Int("grouped", len(groupedSuitableNodes)).
		Int("non_grouped", len(nonGroupedSuitableNodes)).
		Msg("found candidate nodes for group")

	return allSuitableNodes, groupedSuitableNodes, nonGroupedSuitableNodes, true
}

// GetNodeGroupsByGoal возвращает список групп, у которых цель соответствует переданному goalID
// и в участниках которых есть указанная нода с nodeID.
func (er *EntityRegistry) GetNodeGroupsByGoal(goalID, nodeID string) ([]*entities.Group, error) {
	// Получаем все группы
	groups, err := er.Group.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", nodeID).
			Str("goal_id", goalID).
			Msg("ошибка при получении всех групп")
		return nil, fmt.Errorf("ошибка при получении всех групп: %w", err)
	}

	var result []*entities.Group

	// Проверяем каждую группу
	for _, group := range groups {
		// Пропускаем удаленные группы
		if group.IsDeleted() {
			continue
		}

		// Проверяем соответствие цели
		if group.GetGoal() != goalID {
			continue
		}

		// Проверяем, есть ли нода среди участников группы
		isParticipant := false
		for _, participantID := range group.GetParticipants() {
			if participantID == nodeID {
				isParticipant = true
				break
			}
		}

		if isParticipant {
			result = append(result, group)
		}
	}

	log.Debug().
		Str("node_id", nodeID).
		Str("goal_id", goalID).
		Int("found_groups", len(result)).
		Msg("найдены группы для ноды и цели")

	return result, nil
}
