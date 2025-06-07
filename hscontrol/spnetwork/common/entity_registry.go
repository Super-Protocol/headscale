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
	BaseRegistry     Registry
	Node             *TypedRegistry[*entities.Node]
	Measurement      *TypedRegistry[*entities.Measurement]
	Group            *TypedRegistry[*entities.Group]
	GroupGoal        *TypedRegistry[*entities.GroupGoal]
	Vote             *TypedRegistry[*entities.Vote]
	VoteRequest      *TypedRegistry[*entities.VoteRequest]
	LeadershipResign *TypedRegistry[*entities.LeadershipResign]
}

// NewEntityRegistry creates a new instance of Registry
func NewEntityRegistry(baseRegistry Registry) *EntityRegistry {
	return &EntityRegistry{
		BaseRegistry:     baseRegistry,
		Node:             NewTypedRegistry[*entities.Node](baseRegistry, NodeEntityType),
		Measurement:      NewTypedRegistry[*entities.Measurement](baseRegistry, MeasurementEntityType),
		Group:            NewTypedRegistry[*entities.Group](baseRegistry, GroupEntityType),
		GroupGoal:        NewTypedRegistry[*entities.GroupGoal](baseRegistry, GroupGoalEntityType),
		Vote:             NewTypedRegistry[*entities.Vote](baseRegistry, VoteEntityType),
		VoteRequest:      NewTypedRegistry[*entities.VoteRequest](baseRegistry, VoteRequestEntityType),
		LeadershipResign: NewTypedRegistry[*entities.LeadershipResign](baseRegistry, LeadershipResignEntityType),
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
func (er *EntityRegistry) IsNodeAlive(nodeID string, timeoutSecs int64) bool {
	lastSeenTime, err := er.GetNodeLastSeenTime(nodeID)
	if err != nil {
		return false
	}

	timeoutDuration := time.Duration(timeoutSecs) * time.Second
	isAlive := time.Since(lastSeenTime) <= timeoutDuration

	log.Debug().
		Str("node_id", nodeID).
		Int64("timeout_secs", timeoutSecs).
		Dur("time_since_seen", time.Since(lastSeenTime)).
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
func (er *EntityRegistry) GetNodesWithCapabilityForGoal(goalId string, nodeId string, maxCount int) ([]*entities.Node, []*entities.Node, []*entities.Node, bool) {
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

	// Сортируем ноды по идентификатору для обеспечения стабильного порядка
	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].GetID() < allNodes[j].GetID()
	})

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
			for _, participant := range group.GetParticipants() {
				groupedNodes[participant.ID] = true
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
		// Текущую ноду добавляем сразу
		if candidateNode.GetID() == nodeId {
			candidateNodes = append(candidateNodes, nodeWithScore{
				node:  candidateNode,
				score: math.MaxFloat64, // Максимальный приоритет для текущей ноды
			})
			continue
		}
		// Пропускаем те удаленные
		if candidateNode.IsDeleted() || !er.IsNodeAlive(candidateNode.GetID(), groupGoal.InactivityTimeout) {
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
	maxGroupSize := maxCount
	maxResult := maxGroupSize
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

// GetGroupsByGoal возвращает список групп, у которых цель соответствует переданному goalID.
func (er *EntityRegistry) GetGroupsByGoal(goalID string) ([]*entities.Group, error) {
	// Получаем все группы
	groups, err := er.Group.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
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

		result = append(result, group)
	}

	log.Debug().
		Str("goal_id", goalID).
		Int("found_groups", len(result)).
		Msg("найдены группы для цели")

	return result, nil
}

// GetNodeGroupsByGoal возвращает список групп, у которых цель соответствует переданному goalID
// и в участниках которых есть указанная нода с nodeID.
func (er *EntityRegistry) GetNodeGroupsByGoal(goalID, nodeID string) ([]*entities.Group, error) {
	// Получаем все группы по цели
	groups, err := er.GetGroupsByGoal(goalID)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", nodeID).
			Str("goal_id", goalID).
			Msg("ошибка при получении групп по цели")
		return nil, err
	}

	var result []*entities.Group

	// Фильтруем группы по наличию ноды среди участников
	for _, group := range groups {
		// Проверяем, есть ли нода среди участников группы
		isParticipant := false
		for _, participant := range group.GetParticipants() {
			if participant.ID == nodeID {
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

// HasLeadershipResign проверяет, существует ли уже отказ от лидерства для указанного запроса на голосование
// Возвращает true, если отказ от лидерства существует, иначе false
func (er *EntityRegistry) HasLeadershipResign(voteRequestId string) bool {
	resigns, err := er.LeadershipResign.GetAllEntities()
	if err != nil {
		return false
	}

	for _, resign := range resigns {
		if !resign.IsDeleted() && resign.GetVoteRequestID() == voteRequestId {
			return true
		}
	}

	return false
}

// HasActiveVoteRequest проверяет наличие активного запроса на голосование с указанными параметрами
// Возвращает существующий запрос и true, если такой запрос найден, иначе nil и false
// Активным считается запрос, у которого не достигнут кворум и не истек таймаут
func (er *EntityRegistry) HasActiveVoteRequest(kind entities.VoteKind, target string) (*entities.VoteRequest, bool) {
	voteRequests, err := er.VoteRequest.GetAllEntities()
	if err != nil {
		return nil, false
	}

	currentTime := time.Now().Unix()

	// Фильтруем подходящие запросы на голосование
	var filteredRequests []*entities.VoteRequest
	for _, vr := range voteRequests {
		if vr.GetKind() == kind &&
			vr.GetTarget() == target &&
			!vr.IsDeleted() {
			filteredRequests = append(filteredRequests, vr)
		}
	}

	if len(filteredRequests) == 0 {
		return nil, false
	}

	// Сортируем по времени создания в обратном порядке (самые новые в начале)
	sort.Slice(filteredRequests, func(i, j int) bool {
		return filteredRequests[i].GetDateUnix() > filteredRequests[j].GetDateUnix()
	})

	req := filteredRequests[0]
	return req, req.IsActive(currentTime) && !req.IsQuorumReached()
}

// HasVoteFromNodeForRequest проверяет, голосовала ли уже нода за конкретный запрос
// Возвращает true, если нода уже голосовала, иначе false
func (er *EntityRegistry) HasVoteFromNodeForRequest(requestID string, voterID string) bool {
	votes, err := er.Vote.GetAllEntities()
	if err != nil {
		return false
	}

	for _, vote := range votes {
		if !vote.IsDeleted() && vote.GetRequestID() == requestID && vote.GetVoter() == voterID {
			return true
		}
	}

	return false
}

// GetNonGroupedNodesForGoal возвращает список нод, которые еще не входят ни в одну группу
// для указанной цели группировки.
func (er *EntityRegistry) GetNonGroupedNodesForGoal(goalID string) ([]*entities.Node, error) {
	// Получаем все активные ноды
	allNodes, err := er.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("goal_id", goalID).
			Msg("ошибка при получении всех нод")
		return nil, fmt.Errorf("ошибка при получении всех нод: %w", err)
	}

	// Получаем все группы для данной цели
	// Метод GetGroupsByGoal уже отфильтровывает удаленные группы
	groupsByGoal, err := er.GetGroupsByGoal(goalID)
	if err != nil {
		log.Error().
			Err(err).
			Str("goal_id", goalID).
			Msg("ошибка при получении групп по цели")
		return nil, fmt.Errorf("ошибка при получении групп по цели: %w", err)
	}

	// Создаем мапу нод, которые уже в группах с данной целью
	nodesInGroups := make(map[string]bool)
	for _, group := range groupsByGoal {
		// Дополнительная проверка, хотя GetGroupsByGoal уже отфильтровывает удаленные группы
		if group.IsDeleted() {
			continue
		}
		for _, participant := range group.GetParticipants() {
			nodesInGroups[participant.ID] = true
		}
	}

	// Фильтруем ноды, которые еще не в группах и не удалены
	var nonGroupedNodes []*entities.Node
	for _, node := range allNodes {
		if !node.IsDeleted() && !nodesInGroups[node.GetID()] {
			nonGroupedNodes = append(nonGroupedNodes, node)
		}
	}

	log.Debug().
		Str("goal_id", goalID).
		Int("all_nodes", len(allNodes)).
		Int("non_grouped_nodes", len(nonGroupedNodes)).
		Msg("найдены ноды, не входящие в группы с данной целью")

	return nonGroupedNodes, nil
}

// GetCommonNodesWithCapabilityForGoal возвращает пересечение результатов вызова GetNodesWithCapabilityForGoal
// для каждой ноды из переданного списка nodeIDs.
// Возвращает кортеж, содержащий:
// 1. Подходящие ноды, которые удовлетворяют всем заданным нодам из списка
// 2. Список нод в группах с данной целью
// 3. Список нод, не входящих в группы с данной целью
// А также булево значение, указывающее, достаточно ли найденных нод
func (er *EntityRegistry) GetCommonNodesWithCapabilityForGoal(goalID string, nodeIDs []string, maxCount int) ([]*entities.Node, []*entities.Node, []*entities.Node, bool) {
	if len(nodeIDs) == 0 {
		log.Error().
			Str("goal_id", goalID).
			Msg("пустой список nodeIDs")
		return nil, nil, nil, false
	}

	// Если передана только одна нода, просто вызываем существующий метод
	if len(nodeIDs) == 1 {
		return er.GetNodesWithCapabilityForGoal(goalID, nodeIDs[0], maxCount)
	}

	// Создаем мапу для хранения счетчиков для каждой ноды
	// Ключ - ID ноды, значение - количество раз, когда нода подходит для группировки
	nodeCounters := make(map[string]int)

	// Создаем мапы для отслеживания нод, которые находятся в группах и которые не в группах
	inGroupNodes := make(map[string]bool)
	notInGroupNodes := make(map[string]bool)

	// Получаем все ноды, удовлетворяющие критериям для каждой ноды из списка
	for _, nodeID := range nodeIDs {
		allSuitableNodes, groupedNodes, nonGroupedNodes, success := er.GetNodesWithCapabilityForGoal(goalID, nodeID, maxCount)

		if !success {
			log.Error().
				Str("goal_id", goalID).
				Str("node_id", nodeID).
				Msg("не удалось получить подходящие ноды")
			continue
		}

		// Увеличиваем счетчик для каждой подходящей ноды
		for _, node := range allSuitableNodes {
			nodeCounters[node.GetID()]++
		}

		// Отмечаем ноды, которые находятся в группах
		for _, node := range groupedNodes {
			inGroupNodes[node.GetID()] = true
		}

		// Отмечаем ноды, которые не находятся в группах
		for _, node := range nonGroupedNodes {
			notInGroupNodes[node.GetID()] = true
		}
	}

	// Получаем все ноды для формирования результата
	allNodes, err := er.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Msg("ошибка при получении всех нод")
		return nil, nil, nil, false
	}

	// Формируем результаты
	var commonSuitableNodes []*entities.Node
	var commonGroupedNodes []*entities.Node
	var commonNonGroupedNodes []*entities.Node

	// Находим ноды, которые подходят для всех нод из списка (их счетчик равен длине списка)
	targetCount := len(nodeIDs)
	for _, node := range allNodes {
		if node.IsDeleted() {
			continue
		}

		// Проверяем, подходит ли нода для всех исходных нод
		count, exists := nodeCounters[node.GetID()]
		if exists && count == targetCount {
			commonSuitableNodes = append(commonSuitableNodes, node)

			// Распределяем по спискам в группах/не в группах
			if inGroupNodes[node.GetID()] {
				commonGroupedNodes = append(commonGroupedNodes, node)
			}
			if notInGroupNodes[node.GetID()] {
				commonNonGroupedNodes = append(commonNonGroupedNodes, node)
			}
		}
	}

	// Ограничиваем количество результатов
	if len(commonSuitableNodes) > maxCount {
		commonSuitableNodes = commonSuitableNodes[:maxCount]
	}
	if len(commonGroupedNodes) > maxCount {
		commonGroupedNodes = commonGroupedNodes[:maxCount]
	}
	if len(commonNonGroupedNodes) > maxCount {
		commonNonGroupedNodes = commonNonGroupedNodes[:maxCount]
	}

	// Получаем GroupGoal по goalId для проверки минимального размера группы
	groupGoal, err := er.GroupGoal.GetEntity(goalID)
	if err != nil {
		log.Error().
			Err(err).
			Str("goal_id", goalID).
			Msg("ошибка при получении group goal")
		return commonSuitableNodes, commonGroupedNodes, commonNonGroupedNodes, false
	}

	minGroupSize := groupGoal.GetMinGroupSize()
	hasEnoughNodes := len(commonSuitableNodes) >= (minGroupSize - len(nodeIDs))

	log.Debug().
		Str("goal_id", goalID).
		Int("node_ids_count", len(nodeIDs)).
		Int("all_suitable", len(commonSuitableNodes)).
		Int("grouped", len(commonGroupedNodes)).
		Int("non_grouped", len(commonNonGroupedNodes)).
		Bool("has_enough", hasEnoughNodes).
		Msg("найдены общие подходящие ноды")

	return commonSuitableNodes, commonGroupedNodes, commonNonGroupedNodes, hasEnoughNodes
}
