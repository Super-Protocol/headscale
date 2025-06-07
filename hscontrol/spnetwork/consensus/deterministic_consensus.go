package consensus

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/syncer"
	"github.com/rs/zerolog/log"
	"sort"
	"sync"
	"time"
)

// DeterministicConsensus реализует механизм консенсуса на основе детерминированного
// определения лидера сети с подтверждением через голосование.
type DeterministicConsensus struct {
	syncer                 syncer.Syncer
	registry               *common.EntityRegistry
	localNode              *entities.Node
	syncInterval           time.Duration
	leaderAliveTimeoutSecs int64
	isLeader               bool
	mu                     sync.RWMutex
	running                bool
	stopChan               chan struct{}
	consensusRunning       bool
}

// NewDeterministicConsensus создает новый экземпляр DeterministicConsensus
func NewDeterministicConsensus(
	syncer syncer.Syncer,
	registry *common.EntityRegistry,
	localNode *entities.Node,
	syncInterval time.Duration,
	leaderAliveTimeoutSecs int64,
) (*DeterministicConsensus, error) {
	return &DeterministicConsensus{
		syncer:                 syncer,
		registry:               registry,
		localNode:              localNode,
		syncInterval:           syncInterval,
		leaderAliveTimeoutSecs: leaderAliveTimeoutSecs,
		isLeader:               false,
		stopChan:               make(chan struct{}),
		consensusRunning:       false,
	}, nil
}

// Start запускает процесс консенсуса
func (dc *DeterministicConsensus) Start() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dc.running {
		log.Warn().Str("node_id", dc.localNode.GetID()).Msg("consensus already running")
		return fmt.Errorf("consensus already running")
	}

	log.Info().
		Str("node_id", dc.localNode.GetID()).
		Msg("starting deterministic consensus")

	go dc.consensusLoop()

	dc.running = true
	log.Info().
		Str("node_id", dc.localNode.GetID()).
		Msg("deterministic consensus started")

	return nil
}

// Stop останавливает процесс консенсуса
func (dc *DeterministicConsensus) Stop() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if !dc.running {
		log.Warn().Str("node_id", dc.localNode.GetID()).Msg("consensus already stopped")
		return fmt.Errorf("consensus already stopped")
	}

	log.Info().Str("node_id", dc.localNode.GetID()).Msg("stopping deterministic consensus")

	close(dc.stopChan)

	dc.running = false
	log.Info().Str("node_id", dc.localNode.GetID()).Msg("deterministic consensus stopped")

	return nil
}

// IsLeader возвращает признак того, является ли текущая нода лидером сети
func (dc *DeterministicConsensus) IsLeader() bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	currentLeader, _, err := dc.determineCurrentLeader()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", dc.localNode.GetID()).
			Msg("error determining current leader")
		return false
	}
	if currentLeader == nil {
		return false
	}
	return currentLeader.GetID() == dc.localNode.GetID()
}

// setLeaderState устанавливает статус лидерства для ноды
func (dc *DeterministicConsensus) setLeaderState(isLeader bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.isLeader = isLeader
}

// consensusLoop периодически выполняет процесс консенсуса
func (dc *DeterministicConsensus) consensusLoop() {
	ticker := time.NewTicker(dc.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := dc.runConsensusIfNotRunning()
			if err != nil {
				log.Error().Err(err).Msg("error running consensus process")
			}
		case <-dc.stopChan:
			return
		}
	}
}

// runConsensusIfNotRunning запускает процесс консенсуса, если он еще не запущен
func (dc *DeterministicConsensus) runConsensusIfNotRunning() error {
	dc.mu.Lock()
	if dc.consensusRunning {
		dc.mu.Unlock()
		log.Debug().
			Str("node_id", dc.localNode.GetID()).
			Msg("skipping consensus run as previous is still in progress")
		return nil
	}
	dc.consensusRunning = true
	dc.mu.Unlock()

	go func() {
		defer func() {
			dc.mu.Lock()
			dc.consensusRunning = false
			dc.mu.Unlock()
		}()

		// Проверяем уровень синхронизации
		syncCoef := dc.syncer.GetSyncCoef()
		if syncCoef < 0.98 {
			log.Debug().
				Str("node_id", dc.localNode.GetID()).
				Float32("sync_coef", syncCoef).
				Msg("skipping consensus process due to low sync coefficient")
			dc.setLeaderState(false)
			return
		}

		// Определяем текущего лидера
		currentLeader, voteRequest, err := dc.determineCurrentLeader()
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", dc.localNode.GetID()).
				Msg("error determining current leader")
			return
		}

		if currentLeader != nil && currentLeader.GetID() == dc.localNode.GetID() {
			dc.setLeaderState(true)
		}

		// Определяем детерминистически кто должен быть лидером (нода с наименьшим ID)
		shouldBeLeader, err := dc.determineShouldBeLeader()
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", dc.localNode.GetID()).
				Msg("error determining who should be leader")
			return
		}

		// Если текущая нода должна быть лидером
		if shouldBeLeader != nil && shouldBeLeader.GetID() == dc.localNode.GetID() {
			// Если мы не являемся текущим лидером, инициируем голосование
			if currentLeader == nil || currentLeader.GetID() != dc.localNode.GetID() {
				voteRequest, hasActive := dc.registry.HasActiveVoteRequest(entities.VoteKindNetworkLeadership, dc.localNode.GetID())
				if !hasActive {
					err = dc.initiateLeadershipVote()
					if err != nil {
						log.Error().
							Err(err).
							Str("node_id", dc.localNode.GetID()).
							Msg("error initiating leadership vote")
					}
				} else {
					dc.checkQuorumReached(voteRequest)
				}
			} else {
				// Мы уже лидер
				dc.setLeaderState(true)
			}
		} else {
			// Мы не должны быть лидером
			dc.setLeaderState(false)
		}

		// Проверяем наличие активных голосований и голосуем при необходимости
		err = dc.checkAndVoteForLeadership(shouldBeLeader)
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", dc.localNode.GetID()).
				Msg("error checking and voting for leadership")
		}

		// Если мы текущий лидер, но есть более свежее голосование за другого
		if currentLeader != nil && currentLeader.GetID() == dc.localNode.GetID() && voteRequest != nil {
			newerVoteRequests, err := dc.getNewerVoteRequests(voteRequest)
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", dc.localNode.GetID()).
					Str("vote_request_id", voteRequest.GetID()).
					Msg("error getting newer vote requests")
				return
			}

			for _, newVoteRequest := range newerVoteRequests {
				if newVoteRequest.GetTarget() != dc.localNode.GetID() {
					// Если есть более новое голосование за другую ноду, слагаем полномочия
					if !dc.registry.HasLeadershipResign(voteRequest.GetID()) {
						resign := entities.NewLeadershipResign(dc.localNode.GetID(), voteRequest.GetID())
						_, err = dc.registry.LeadershipResign.StoreEntity(resign)
						if err != nil {
							log.Error().
								Err(err).
								Str("node_id", dc.localNode.GetID()).
								Str("vote_request_id", voteRequest.GetID()).
								Msg("error storing leadership resign")
						} else {
							log.Info().
								Str("node_id", dc.localNode.GetID()).
								Str("vote_request_id", voteRequest.GetID()).
								Str("new_leader", newVoteRequest.GetTarget()).
								Msg("resigned from leadership due to newer vote")

							dc.setLeaderState(false)
						}
					}
					break
				}
			}
		}
	}()

	return nil
}

// determineCurrentLeader определяет текущего лидера сети на основе флага достижения кворума
func (dc *DeterministicConsensus) determineCurrentLeader() (*entities.Node, *entities.VoteRequest, error) {
	// Получаем все запросы на голосование
	voteRequests, err := dc.registry.VoteRequest.GetAllEntities()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting vote requests: %w", err)
	}

	// Фильтруем запросы на голосование для сетевого лидерства
	leadershipVoteRequests := make([]*entities.VoteRequest, 0)
	for _, vr := range voteRequests {
		if vr.GetKind() == entities.VoteKindNetworkLeadership && !vr.IsDeleted() {
			leadershipVoteRequests = append(leadershipVoteRequests, vr)
		}
	}

	// Сортируем по дате создания в обратном порядке (самые поздние вверху)
	sort.Slice(leadershipVoteRequests, func(i, j int) bool {
		return leadershipVoteRequests[i].GetDateUnix() > leadershipVoteRequests[j].GetDateUnix()
	})

	// Проверяем все запросы на голосование, начиная с самых новых
	for _, voteRequest := range leadershipVoteRequests {
		// Проверяем флаг достижения кворума
		if voteRequest.IsQuorumReached() {
			// Проверяем, есть ли resignations для предыдущего лидера
			// или если таймаут предыдущего лидера истек
			targetNode, err := dc.registry.Node.GetEntity(voteRequest.GetTarget())
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", voteRequest.GetTarget()).
					Msg("error getting target node")
				continue
			}

			if targetNode.IsDeleted() {
				continue
			}

			// Проверяем resignation для предыдущего лидера, если он был
			previousLeader, previousVoteRequest, err := dc.findPreviousLeader(voteRequest)
			if err != nil {
				log.Error().
					Err(err).
					Str("vote_request_id", voteRequest.GetID()).
					Msg("error finding previous leader")
				continue
			}

			if previousLeader != nil {
				// Проверяем, есть ли resignation от предыдущего лидера
				hasResigned := dc.hasLeaderResigned(previousLeader.GetID(), previousVoteRequest.GetID())

				// Или истек ли таймаут предыдущего лидера
				timeoutExpired := false
				if previousLeader != nil {
					timeoutExpired = !dc.registry.IsNodeAlive(previousLeader.GetID(), voteRequest.GetTimeoutSecs())
				}

				if !hasResigned && !timeoutExpired {
					continue // Предыдущий лидер еще не сложил полномочия и таймаут не истек
				}
			}

			// Нашли текущего лидера
			return targetNode, voteRequest, nil
		}
	}

	return nil, nil, nil // Лидер не найден
}

// findPreviousLeader находит предыдущего лидера перед указанным запросом на голосование
func (dc *DeterministicConsensus) findPreviousLeader(currentVoteRequest *entities.VoteRequest) (*entities.Node, *entities.VoteRequest, error) {
	// Получаем все запросы на голосование
	voteRequests, err := dc.registry.VoteRequest.GetAllEntities()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting vote requests: %w", err)
	}

	// Фильтруем запросы на голосование для сетевого лидерства
	leadershipVoteRequests := make([]*entities.VoteRequest, 0)
	for _, vr := range voteRequests {
		if vr.GetKind() == entities.VoteKindNetworkLeadership && !vr.IsDeleted() &&
			vr.GetID() != currentVoteRequest.GetID() &&
			vr.GetDateUnix() < currentVoteRequest.GetDateUnix() {
			leadershipVoteRequests = append(leadershipVoteRequests, vr)
		}
	}

	if len(leadershipVoteRequests) == 0 {
		return nil, nil, nil
	}

	// Сортируем по дате создания в обратном порядке (самые поздние вверху)
	sort.Slice(leadershipVoteRequests, func(i, j int) bool {
		return leadershipVoteRequests[i].GetDateUnix() > leadershipVoteRequests[j].GetDateUnix()
	})

	// Проверяем все запросы на голосование, начиная с самых новых
	for _, voteRequest := range leadershipVoteRequests {
		// Проверяем флаг достижения кворума
		if voteRequest.IsQuorumReached() {
			targetNode, err := dc.registry.Node.GetEntity(voteRequest.GetTarget())
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", voteRequest.GetTarget()).
					Msg("error getting target node")
				continue
			}

			if targetNode.IsDeleted() {
				continue
			}

			return targetNode, voteRequest, nil
		}
	}

	return nil, nil, nil // Предыдущий лидер не найден
}

// findVoteRequestForLeader находит запрос на голосование для указанного лидера
func (dc *DeterministicConsensus) findVoteRequestForLeader(leaderID string) (*entities.VoteRequest, error) {
	// Получаем все запросы на голосование
	voteRequests, err := dc.registry.VoteRequest.GetAllEntities()
	if err != nil {
		return nil, fmt.Errorf("error getting vote requests: %w", err)
	}

	// Фильтруем запросы на голосование для сетевого лидерства с указанным лидером
	leadershipVoteRequests := make([]*entities.VoteRequest, 0)
	for _, vr := range voteRequests {
		if vr.GetKind() == entities.VoteKindNetworkLeadership && !vr.IsDeleted() && vr.GetTarget() == leaderID {
			leadershipVoteRequests = append(leadershipVoteRequests, vr)
		}
	}

	if len(leadershipVoteRequests) == 0 {
		return nil, nil
	}

	// Сортируем по дате создания в обратном порядке (самые поздние вверху)
	sort.Slice(leadershipVoteRequests, func(i, j int) bool {
		return leadershipVoteRequests[i].GetDateUnix() > leadershipVoteRequests[j].GetDateUnix()
	})

	// Получаем все голоса
	votes, err := dc.registry.Vote.GetAllEntities()
	if err != nil {
		return nil, fmt.Errorf("error getting votes: %w", err)
	}

	// Получаем все ноды
	nodes, err := dc.registry.Node.GetAllEntities()
	if err != nil {
		return nil, fmt.Errorf("error getting nodes: %w", err)
	}

	// Подсчитываем активные ноды
	activeNodes := 0
	for _, node := range nodes {
		if !node.IsDeleted() {
			activeNodes++
		}
	}

	// Проверяем все запросы на голосование, начиная с самых новых
	for _, voteRequest := range leadershipVoteRequests {
		// Подсчитываем голоса за этот запрос
		votesCount := 0
		for _, vote := range votes {
			if vote.GetRequestID() == voteRequest.GetID() && !vote.IsDeleted() {
				votesCount++
			}
		}

		// Проверяем, есть ли кворум (2/3 от общего числа нод)
		requiredVotes := int(float64(activeNodes) * 2.0 / 3.0)
		if votesCount >= requiredVotes {
			return voteRequest, nil
		}
	}

	return nil, nil // Запрос на голосование не найден
}

// hasLeaderResigned проверяет, сложил ли лидер свои полномочия
func (dc *DeterministicConsensus) hasLeaderResigned(leaderID string, voteRequestID string) bool {
	resignations, err := dc.registry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("leader_id", leaderID).
			Msg("error getting leadership resignations")
		return false
	}

	for _, resign := range resignations {
		if !resign.IsDeleted() && resign.GetOwner() == leaderID && resign.GetVoteRequestID() == voteRequestID {
			return true
		}
	}

	return false
}

// GetLeaderID returns the ID of the current leader and a boolean indicating if a leader exists
func (c *DeterministicConsensus) GetLeaderID() (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	leaderNode, _, err := c.determineCurrentLeader()
	if err != nil || leaderNode == nil {
		return "", false
	}

	return leaderNode.GetID(), true
}

// determineShouldBeLeader определяет, какая нода должна быть лидером
func (dc *DeterministicConsensus) determineShouldBeLeader() (*entities.Node, error) {
	// Получаем все ноды
	nodes, err := dc.registry.Node.GetAllEntities()
	if err != nil {
		return nil, fmt.Errorf("error getting nodes: %w", err)
	}

	if len(nodes) == 0 {
		return nil, nil
	}

	// Находим ноду с наименьшим ID среди доступных нод
	var shouldBeLeader *entities.Node
	for _, node := range nodes {
		if node.IsDeleted() {
			continue
		}

		// Проверяем, что нода доступна (жива)
		if !dc.registry.IsNodeAlive(node.GetID(), dc.leaderAliveTimeoutSecs) {
			continue
		}

		if shouldBeLeader == nil || node.GetID() < shouldBeLeader.GetID() {
			shouldBeLeader = node
		}
	}

	return shouldBeLeader, nil
}

// initiateLeadershipVote инициирует голосование за лидерство текущей ноды
func (dc *DeterministicConsensus) initiateLeadershipVote() error {
	now := time.Now().Unix()

	// Проверяем, существует ли уже активный запрос на голосование за текущую ноду
	existingRequest, hasActive := dc.registry.HasActiveVoteRequest(entities.VoteKindNetworkLeadership, dc.localNode.GetID())
	if hasActive {
		log.Debug().
			Str("node_id", dc.localNode.GetID()).
			Str("request_id", existingRequest.GetID()).
			Msg("active vote request already exists, skipping creation of new request")

		// Проверяем кворум для существующего запроса
		dc.checkQuorumReached(existingRequest)
		return nil
	}

	// Создаем новый запрос на голосование
	voteRequest := entities.NewVoteRequest(entities.VoteKindNetworkLeadership, dc.localNode.GetID(), now)

	// Сохраняем запрос на голосование
	_, err := dc.registry.VoteRequest.StoreEntity(voteRequest)
	if err != nil {
		return fmt.Errorf("error storing vote request: %w", err)
	}

	// Сразу же создаем голос от текущей ноды за свой запрос
	vote := entities.NewVoteForRequest(voteRequest, 1, dc.localNode.GetID())
	_, err = dc.registry.Vote.StoreEntity(vote)
	if err != nil {
		return fmt.Errorf("error storing self vote: %w", err)
	}

	log.Info().
		Str("node_id", dc.localNode.GetID()).
		Str("vote_request_id", voteRequest.GetID()).
		Msg("initiated leadership vote")

	// Проверяем кворум для нового запроса
	dc.checkQuorumReached(voteRequest)

	return nil
}

// getNewerVoteRequests возвращает более новые запросы на голосование, чем указанный
func (dc *DeterministicConsensus) getNewerVoteRequests(voteRequest *entities.VoteRequest) ([]*entities.VoteRequest, error) {
	// Получаем все запросы на голосование
	voteRequests, err := dc.registry.VoteRequest.GetAllEntities()
	if err != nil {
		return nil, fmt.Errorf("error getting vote requests: %w", err)
	}

	// Фильтруем более новые запросы на голосование
	newerVoteRequests := make([]*entities.VoteRequest, 0)
	for _, vr := range voteRequests {
		if vr.GetKind() == entities.VoteKindNetworkLeadership &&
			!vr.IsDeleted() &&
			vr.GetDateUnix() > voteRequest.GetDateUnix() {
			newerVoteRequests = append(newerVoteRequests, vr)
		}
	}

	// Сортируем по дате создания
	sort.Slice(newerVoteRequests, func(i, j int) bool {
		return newerVoteRequests[i].GetDateUnix() < newerVoteRequests[j].GetDateUnix()
	})

	return newerVoteRequests, nil
}

// checkAndVoteForLeadership проверяет активные голосования и голосует при необходимости
func (dc *DeterministicConsensus) checkAndVoteForLeadership(shouldBeLeader *entities.Node) error {
	if shouldBeLeader == nil {
		return nil
	}

	// Получаем все запросы на голосование
	voteRequests, err := dc.registry.VoteRequest.GetAllEntities()
	if err != nil {
		return fmt.Errorf("error getting vote requests: %w", err)
	}

	// Получаем все голоса
	votes, err := dc.registry.Vote.GetAllEntities()
	if err != nil {
		return fmt.Errorf("error getting votes: %w", err)
	}

	// Создаем мапу для быстрой проверки, голосовали ли мы уже
	votedRequests := make(map[string]bool)
	for _, vote := range votes {
		if vote.GetKind() == string(entities.VoteKindNetworkLeadership) &&
			!vote.IsDeleted() &&
			dc.localNode.GetID() == vote.GetTarget() {
			votedRequests[vote.GetRequestID()] = true
		}
	}

	// Проверяем все запросы на голосование
	for _, voteRequest := range voteRequests {
		if voteRequest.GetKind() != entities.VoteKindNetworkLeadership || voteRequest.IsDeleted() {
			continue
		}

		// Уже голосовали?
		if dc.registry.HasVoteFromNodeForRequest(voteRequest.GetID(), dc.localNode.GetID()) {
			continue
		}

		// Голосуем только если цель голосования - тот, кто должен быть лидером
		if voteRequest.GetTarget() == shouldBeLeader.GetID() {
			vote := entities.NewVoteForRequest(voteRequest, 1, dc.localNode.GetID())
			_, err = dc.registry.Vote.StoreEntity(vote)
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", dc.localNode.GetID()).
					Str("vote_request_id", voteRequest.GetID()).
					Str("target_id", voteRequest.GetTarget()).
					Msg("error storing vote")
				continue
			}

			log.Info().
				Str("node_id", dc.localNode.GetID()).
				Str("vote_request_id", voteRequest.GetID()).
				Str("target_id", voteRequest.GetTarget()).
				Msg("voted for leadership")
		}

		// Если мы являемся целью голосования (target), проверяем достижение кворума
		if voteRequest.GetTarget() == dc.localNode.GetID() {
			dc.checkQuorumReached(voteRequest)
		}
	}

	return nil
}

// checkQuorumReached проверяет достижение кворума и устанавливает флаг, если кворум достигнут
func (dc *DeterministicConsensus) checkQuorumReached(voteRequest *entities.VoteRequest) {
	// Если кворум уже достигнут, ничего не делаем
	if voteRequest.IsQuorumReached() {
		return
	}

	// Получаем все голоса
	votes, err := dc.registry.Vote.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("vote_request_id", voteRequest.GetID()).
			Msg("error getting votes for quorum check")
		return
	}

	// Получаем все ноды
	nodes, err := dc.registry.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("vote_request_id", voteRequest.GetID()).
			Msg("error getting nodes for quorum check")
		return
	}

	// Подсчитываем активные ноды
	activeNodes := 0
	for _, node := range nodes {
		if !node.IsDeleted() && dc.registry.IsNodeAlive(node.GetID(), dc.leaderAliveTimeoutSecs) {
			activeNodes++
		}
	}

	// Подсчитываем голоса за этот запрос
	votesCount := 0
	for _, vote := range votes {
		if vote.GetRequestID() == voteRequest.GetID() && !vote.IsDeleted() {
			votesCount++
		}
	}

	// Проверяем, есть ли кворум (2/3 от общего числа нод)
	requiredVotes := int(float64(activeNodes) * 2.0 / 3.0)
	if votesCount >= requiredVotes {
		// Устанавливаем флаг достижения кворума
		voteRequest.SetQuorumReached(true)

		// Сохраняем изменения в реестре
		_, err := dc.registry.VoteRequest.StoreEntity(voteRequest)
		if err != nil {
			log.Error().
				Err(err).
				Str("vote_request_id", voteRequest.GetID()).
				Msg("error updating vote request with quorum reached flag")
			return
		}

		log.Info().
			Str("node_id", dc.localNode.GetID()).
			Str("vote_request_id", voteRequest.GetID()).
			Int("votes_count", votesCount).
			Int("required_votes", requiredVotes).
			Msg("quorum reached and flag set")
	}
}
