package grouping

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type DeterministicGrouping struct {
	registry         *common.EntityRegistry
	localNode        *entities.Node
	groupingInterval time.Duration
	mu               sync.Mutex
	running          bool
	stopChan         chan struct{}
	groupingRunning  bool
}

func NewDeterministicGrouping(registry *common.EntityRegistry, localNode *entities.Node, groupingInterval time.Duration) (*DeterministicGrouping, error) {
	return &DeterministicGrouping{
		registry:         registry,
		localNode:        localNode,
		groupingInterval: groupingInterval,
		stopChan:         make(chan struct{}),
	}, nil
}

func (dg *DeterministicGrouping) processGoal(goal *entities.GroupGoal) error {
	dg.mu.Lock()
	defer dg.mu.Unlock()
	//nodeId := dg.localNode.GetID()
	//goalId := goal.GetID()
	//nodeGoalGroups, err := dg.registry.GetNodeGroupsByGoal(goalId, nodeId)
	//if err != nil {
	//	return err
	//}
	//nodeInGoalGroup := len(nodeGoalGroups) > 0
	//newGroup := entities.NewGroup()
	//newGroup.SetGoal(goalId)
	//newGroup.AddParticipant(nodeId)

	/*
			Новый алгоритм:
			1. Внедряем оценку отсталости от сети - ведем статистику какой средний процент новых данных мы получили за последние N итераций.
				Надо подумать что взять за N, но можно например число нод в сети.
			2. Операции по группировке, голосование и прочее осуществляем только если оценка отсталости не более X
			3. Операции по группировки осуществляет глобальный лидер сети
			4. Лидер сети определяется неявным голосованием - нужна новая сущность Vote с типом network_leadership. А также LeadershipRequest. У Request есть таймаут.
			5. Лидером считается тот за кого отдано 2/3 голосов нод за отведенный таймаут. Мы просто смотрим с конца список vote отсортированный по датам и лидер та нода которая первой наберет 2/3 голосов
		       При этом старый лидер должен принять результаты голосования и сложить полномочия, когда кворум будет достигнут отправкой LeadershipResign.
	*/
	//if nodeInGoalGroup {
	//	// Управляем группами если мы там лидеры
	//	for _, group := range nodeGoalGroups {
	//		participantNodes := make([]*entities.Node, 0, len(group.Participants))
	//		for _, p := range group.Participants {
	//			p, err := dg.registry.Node.GetEntity(p.ID)
	//			if err != nil {
	//				return err
	//			}
	//			participantNodes = append(participantNodes, p)
	//		}
	//		leader := getDeterministicLeader(participantNodes)
	//		if leader.GetID() == nodeId {
	//			// Мы лидеры, так что руководим этой группой
	//
	//			// Проверяем участников нашей группы
	//			deadNodes := make([]*entities.Node, 0)
	//			for _, participant := range participantNodes {
	//				// Проверяем жива ли нода
	//				if !dg.registry.IsNodeAlive(participant.GetID(), goal.InactivityTimeout) {
	//					deadNodes = append(deadNodes, participant)
	//				}
	//				// Проверка что все ноды состоят только в 1 группе для цели, если нет - кикаем
	//				participantGoalGroups, err := dg.registry.GetNodeGroupsByGoal(goalId, participant.GetID())
	//				if err != nil {
	//					return err
	//				}
	//				if len(participantGoalGroups) > 1 {
	//					// Отсортируем группы по тому когда текущий участник к ней присоединился
	//					sort.Slice(participantGoalGroups, func(i, j int) bool {
	//						participantI, foundI := participantGoalGroups[i].GetParticipantByID(participant.GetID())
	//						participantJ, foundJ := participantGoalGroups[j].GetParticipantByID(participant.GetID())
	//
	//						// Если участник не найден в какой-то группе, считаем, что он присоединился давно
	//						if !foundI {
	//							return false
	//						}
	//						if !foundJ {
	//							return true
	//						}
	//
	//						// Сортируем в обратном порядке (от большего времени к меньшему)
	//						return participantI.JoinDateUnix > participantJ.JoinDateUnix
	//					})
	//
	//					// Удаляем участника из первой группы (той, к которой он присоединился последней) если это та самая группа, где я лидер
	//					if len(participantGoalGroups) > 0 && participantGoalGroups[0].GetID() == group.GetID() {
	//						participantGoalGroups[0].RemoveParticipant(participant.GetID())
	//					}
	//				}
	//			}
	//
	//			// Удаляем мертвые ноды
	//			for _, deadNode := range deadNodes {
	//				group.RemoveParticipant(deadNode.GetID())
	//			}
	//
	//			// Удаляем ноды если их больше чем нужно
	//			if len(group.GetParticipants()) > goal.GetMaxGroupSize() {
	//				participants := group.GetParticipants()
	//				countToDelete := len(participants) - goal.GetMaxGroupSize()
	//				for i := 0; i < countToDelete; i++ {
	//					participantToDelete := participants[len(participants)-i-1]
	//					group.RemoveParticipant(participantToDelete.ID)
	//				}
	//			}
	//
	//			// Проверка, можно ли добавить какую-то еще ноду в группу, если есть место
	//			// Проверка, можно ли добавить какую-то еще ноду в группу, если есть место
	//			if len(group.GetParticipants()) < goal.GetMaxGroupSize() {
	//				// Получаем всех не сгруппированных кандидатов с точки зрения текущей ноды
	//				// (здесь nonGroupedCandidates уже отфильтрован GetNodesWithCapabilityForGoal)
	//				_, _, nonGroupedCandidates, ok := dg.registry.GetNodesWithCapabilityForGoal(goalId, nodeId, 100000)
	//				if ok {
	//					addCount := goal.GetMaxGroupSize() - len(group.GetParticipants())
	//					addedCount := 0
	//					for i := 0; i < len(nonGroupedCandidates) && addedCount < addCount; i++ {
	//						candidateToAdd := nonGroupedCandidates[i]
	//
	//						// Проверяем, что кандидат еще не в группе
	//						isAlreadyParticipant := false
	//						for _, p := range group.GetParticipants() {
	//							if p.ID == candidateToAdd.GetID() {
	//								isAlreadyParticipant = true
	//								break
	//							}
	//						}
	//						if isAlreadyParticipant {
	//							continue // Пропускаем, если уже в группе
	//						}
	//
	//						// Получаем список кандидатов с точки зрения потенциального нового участника
	//						// для текущей цели группировки (goalId).
	//						// Мы передаем ID потенциального кандидата (candidateToAdd.GetID()) в качестве nodeId
	//						// для функции GetNodesWithCapabilityForGoal.
	//						// Это позволит узнать, кого candidateToAdd считает подходящими для этой цели.
	//						_, _, potentialCandidateViewOfNonGrouped, potentialCandidateViewOk := dg.registry.GetNodesWithCapabilityForGoal(goalId, candidateToAdd.GetID(), 100000)
	//
	//						if !potentialCandidateViewOk {
	//							log.Debug().
	//								Str("goal_id", goalId).
	//								Str("candidate_id", candidateToAdd.GetID()).
	//								Msg("potential candidate does not have enough capability for this goal or an error occurred")
	//							continue // Если сам кандидат не может найти себе группу или ошибка, пропускаем
	//						}
	//
	//						// Создаем мапу для быстрого поиска нод в представлении потенциального кандидата
	//						potentialCandidateViewMap := make(map[string]bool)
	//						for _, n := range potentialCandidateViewOfNonGrouped {
	//							potentialCandidateViewMap[n.GetID()] = true
	//						}
	//
	//						// Проверяем, содержатся ли все текущие участники группы (кроме самой текущей ноды-лидера
	//						// и самого потенциального кандидата) в списке кандидатов потенциального участника
	//						allCurrentParticipantsCompatible := true
	//						for _, currentParticipant := range group.GetParticipants() {
	//							// Исключаем саму ноду-лидера и потенциального кандидата из проверки,
	//							// так как они уже учтены в GetNodesWithCapabilityForGoal или будут добавлены.
	//							if currentParticipant.ID == dg.localNode.GetID() || currentParticipant.ID == candidateToAdd.GetID() {
	//								continue
	//							}
	//							if _, exists := potentialCandidateViewMap[currentParticipant.ID]; !exists {
	//								allCurrentParticipantsCompatible = false
	//								log.Debug().
	//									Str("goal_id", goalId).
	//									Str("candidate_id", candidateToAdd.GetID()).
	//									Str("incompatible_participant_id", currentParticipant.ID).
	//									Msg("potential candidate does not see all current group participants as compatible")
	//								break
	//							}
	//						}
	//
	//						if allCurrentParticipantsCompatible {
	//							group.AddParticipant(candidateToAdd.GetID())
	//							addedCount++
	//							log.Debug().
	//								Str("goal_id", goalId).
	//								Str("group_id", group.GetID()).
	//								Str("node_id", dg.localNode.GetID()).
	//								Str("added_participant_id", candidateToAdd.GetID()).
	//								Msg("added new participant to group after compatibility check")
	//						}
	//					}
	//				}
	//			}
	//
	//		} else {
	//			// Проверяем жив ли вообще лидер и если мы без лидера с наименьшим id
	//			// то берем лидерство на себя, удаляем лидера из группы
	//			if !dg.registry.IsNodeAlive(leader.GetID(), goal.InactivityTimeout) {
	//				participantNodesWithoutLeader := make([]*entities.Node, 0, len(group.Participants)-1)
	//				for _, p := range participantNodes {
	//					if p.GetID() != leader.GetID() {
	//						participantNodesWithoutLeader = append(participantNodesWithoutLeader, p)
	//					}
	//				}
	//				leaderCandidate := getDeterministicLeader(participantNodesWithoutLeader)
	//				if leaderCandidate.GetID() == nodeId {
	//					// Нагло удаляем старого лидера из группы и занимаем его место
	//					group.RemoveParticipant(leader.GetID())
	//				}
	//			}
	//		}
	//	}
	//} else {
	//	bestCandidates, groupedCandidates, nonGroupedCandidates, ok := dg.registry.GetNodesWithCapabilityForGoal(goalId, nodeId,)
	//	if !ok {
	//		return nil
	//	}
	//	candidatesCountEnough := len(bestCandidates) >= goal.MinGroupSize
	//	if candidatesCountEnough {
	//		// Уже есть ноды с группами, поэтому сначала пытаемся подключить к существующей
	//		groupsMap := make(map[string]*entities.Group)
	//		for _, candidate := range groupedCandidates {
	//			groups, err := dg.registry.GetNodeGroupsByGoal(goalId, candidate.GetID())
	//			if err != nil {
	//				return err
	//			}
	//			for _, group := range groups {
	//				groupsMap[group.GetID()] = group
	//			}
	//		}
	//		uniqueGroups := make([]*entities.Group, 0)
	//		for _, group := range groupsMap {
	//			uniqueGroups = append(uniqueGroups, group)
	//		}
	//		sort.Slice(uniqueGroups, func(i, j int) bool {
	//			return len(uniqueGroups[i].GetParticipants()) < len(uniqueGroups[j].GetParticipants())
	//		})
	//
	//		if len(uniqueGroups) > 0 {
	//			existingGroups := uniqueGroups[0]
	//			// Есть незаполненная группа
	//			if len(existingGroups.GetParticipants()) < goal.GetMaxGroupSize() {
	//				// По идее лидер нас сам должен увидеть и подключить к группе
	//				// Поэтому ничего не делаем
	//				return nil
	//			} else { // Все группы заполнены - тогда смотрим среди остальных кандидатов мы лидеры?
	//				leader := getDeterministicLeader(nonGroupedCandidates)
	//				if leader != nil && leader.GetID() == nodeId {
	//					_, err := dg.registry.Group.StoreEntity(newGroup)
	//					if err != nil {
	//						return err
	//					}
	//				}
	//			}
	//		} else {
	//			// Если существующих групп мы не видим среди списка кандидатов, то если мы лидер потенциальной групп
	//			// мы просто создаем свою
	//			leadedCandidate := getDeterministicLeader(append(nonGroupedCandidates, dg.localNode))
	//			if leadedCandidate != nil && leadedCandidate.GetID() == nodeId {
	//				_, err := dg.registry.Group.StoreEntity(newGroup)
	//				if err != nil {
	//					return err
	//				}
	//			}
	//		}
	//	} else {
	//		return nil
	//	}
	//}

	// Общесетевые решения
	// Проверка что количество групп не превышает допустимое
	// Я могу быть лидером среди всех групп, имея наименьший ID и могу принять решение по удалению своей группы как лишней
	// ToDo
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
