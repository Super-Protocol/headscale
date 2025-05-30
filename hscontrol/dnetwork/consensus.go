package dnetwork

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	pb "github.com/juanfont/headscale/gen/go/dnetwork/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConsensusCommand представляет команду для консенсуса
type ConsensusCommand struct {
	Op    string      `json:"op"`    // тип операции (например, "add_node", "vote")
	Key   string      `json:"key"`   // ключ (например, ID группы)
	Value interface{} `json:"value"` // значение (может быть любым типом)
}

// GroupVoteRequest представляет запрос на голосование для группы
type GroupVoteRequest struct {
	GroupName     string   `json:"group_name"`     // имя группы
	GroupID       int64    `json:"group_id"`       // ID группы
	CandidateNode DNode    `json:"candidate_node"` // нода-кандидат
	NodesInGroup  []string `json:"nodes_in_group"` // системные ID нод в группе
	ProposingNode DNode    `json:"proposing_node"` // нода, предлагающая включение
}

// GroupVoteResponse представляет ответ на голосование для группы
type GroupVoteResponse struct {
	Approved bool   `json:"approved"` // одобрено ли предложение
	Message  string `json:"message"`  // сообщение о причине отказа
}

// GroupState хранит состояние группы в консенсусе
type GroupState struct {
	Name      string          `json:"name"`       // имя группы
	GroupID   int64           `json:"group_id"`   // ID группы
	MinSize   int             `json:"min_size"`   // минимальный размер группы
	MaxSize   int             `json:"max_size"`   // максимальный размер группы
	Votes     map[string]bool `json:"votes"`      // текущие голоса (системный ID ноды -> голос)
	VoteCount int             `json:"vote_count"` // количество полученных голосов
	Decided   bool            `json:"decided"`    // принято ли решение
	Accepted  bool            `json:"accepted"`   // одобрено ли предложение
}

// ConsensusStore реализует raft.FSM для хранения состояния консенсуса
type ConsensusStore struct {
	mu       sync.RWMutex
	groups   map[string]*GroupState // имя группы -> состояние группы
	mainNode DNode                  // основная нода
	grouping *DNetworkGrouping
}

// NewConsensusStore создает новый хранилище консенсуса
func NewConsensusStore(mainNode DNode, grouping *DNetworkGrouping) *ConsensusStore {
	return &ConsensusStore{
		groups:   make(map[string]*GroupState),
		mainNode: mainNode,
		grouping: grouping,
	}
}

// Apply применяет лог Raft к консенсусному хранилищу
func (c *ConsensusStore) Apply(log *raft.Log) interface{} {
	var cmd ConsensusCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("ошибка при декодировании команды: %s", err)
	}

	switch cmd.Op {
	case "create_group":
		return c.applyCreateGroup(cmd.Key, cmd.Value)
	case "add_node":
		return c.applyAddNode(cmd.Key, cmd.Value)
	case "vote":
		return c.applyVote(cmd.Key, cmd.Value)
	default:
		return fmt.Errorf("неизвестная операция: %s", cmd.Op)
	}
}

// applyCreateGroup создает новую группу в хранилище
func (c *ConsensusStore) applyCreateGroup(groupName string, value interface{}) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации данных группы: %s", err)
	}

	var groupConfig GroupConfig
	if err := json.Unmarshal(data, &groupConfig); err != nil {
		return fmt.Errorf("ошибка при десериализации данных группы: %s", err)
	}

	c.groups[groupName] = &GroupState{
		Name:      groupName,
		GroupID:   time.Now().UnixNano(),
		MinSize:   groupConfig.Size.Min,
		MaxSize:   groupConfig.Size.Max,
		Votes:     make(map[string]bool),
		VoteCount: 0,
		Decided:   false,
		Accepted:  false,
	}

	return nil
}

// applyAddNode добавляет ноду в группу
func (c *ConsensusStore) applyAddNode(groupName string, value interface{}) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации данных ноды: %s", err)
	}

	var nodeID string
	if err := json.Unmarshal(data, &nodeID); err != nil {
		return fmt.Errorf("ошибка при десериализации ID ноды: %s", err)
	}

	group, exists := c.groups[groupName]
	if !exists {
		return fmt.Errorf("applyAddNode группа не найдена: %s", groupName)
	}

	groupNodes := c.grouping.GetNodesOfGroup(groupName)

	// Проверяем, что нода еще не в группе
	for _, node := range groupNodes {
		if node.SystemID() == nodeID {
			return fmt.Errorf("нода уже в группе: %s", nodeID)
		}
	}

	log.Debug().Msgf("applyAddNode group.Nodes: %d. Max size: %d", len(groupNodes), group.MaxSize)

	// Проверяем, что размер группы не превышает максимальный
	if len(groupNodes) >= group.MaxSize {
		return fmt.Errorf("группа уже достигла максимального размера: %d", group.MaxSize)
	}

	c.grouping.addNodeToGroupById(nodeID, groupName, 1)

	return nil
}

// applyVote применяет голос к голосованию
func (c *ConsensusStore) applyVote(voteID string, value interface{}) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации данных голосования: %s", err)
	}

	var vote struct {
		GroupName string `json:"group_name"`
		NodeID    string `json:"node_id"`
		Approved  bool   `json:"approved"`
	}
	if err := json.Unmarshal(data, &vote); err != nil {
		return fmt.Errorf("ошибка при десериализации голоса: %s", err)
	}

	group, exists := c.groups[vote.GroupName]
	if !exists {
		return fmt.Errorf("applyVote группа не найдена: %s", vote.GroupName)
	}

	// Если решение уже принято, игнорируем голос
	if group.Decided {
		return nil
	}

	// Проверяем, что нода голосует только один раз
	if _, voted := group.Votes[vote.NodeID]; voted {
		return fmt.Errorf("нода уже голосовала: %s", vote.NodeID)
	}

	// Добавляем голос
	group.Votes[vote.NodeID] = vote.Approved
	group.VoteCount++

	groupNodes := c.grouping.GetNodesOfGroup(vote.GroupName)

	// Проверяем, достигнут ли консенсус
	if group.VoteCount >= len(groupNodes) {
		group.Decided = true

		// Подсчитываем голоса
		approvedCount := 0
		for _, approved := range group.Votes {
			if approved {
				approvedCount++
			}
		}

		// Решение принято, если все голоса положительные
		group.Accepted = approvedCount == len(groupNodes)
	}

	return nil
}

// Snapshot возвращает снимок текущего состояния для восстановления
func (c *ConsensusStore) Snapshot() (raft.FSMSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Копируем текущее состояние для снимка
	groups := make(map[string]*GroupState)
	for name, group := range c.groups {
		groupCopy := *group
		groups[name] = &groupCopy
	}

	return &ConsensusSnapshot{groups: groups}, nil
}

// Restore восстанавливает состояние из снимка
func (c *ConsensusStore) Restore(rc io.ReadCloser) error {
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var groups map[string]*GroupState
	if err := json.Unmarshal(data, &groups); err != nil {
		return err
	}

	c.mu.Lock()
	c.groups = groups
	c.mu.Unlock()

	return nil
}

// ConsensusSnapshot представляет снимок состояния консенсуса
type ConsensusSnapshot struct {
	groups map[string]*GroupState
}

// Persist сохраняет снимок в постоянное хранилище
func (s *ConsensusSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.groups)
	if err != nil {
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release освобождает ресурсы снимка
func (s *ConsensusSnapshot) Release() {}

// ConsensusManager управляет Raft и обеспечивает консенсус
type ConsensusManager struct {
	raft        *raft.Raft
	store       *ConsensusStore
	nodeID      string
	mainNode    DNode
	dataDir     string
	networkPort uint16
	raftPort    uint16
	mu          sync.RWMutex
	certFile    string // server certificate file
	keyFile     string // server key file
	caCertPath  string // CA certificate file (for mutual TLS authentication)
	grouping    *DNetworkGrouping
}

// NewConsensusManager создает новый менеджер консенсуса
func NewConsensusManager(mainNode DNode, grouping *DNetworkGrouping, dataDir string, networkPort uint16, raftPort uint16, certFile string,
	keyFile string,
	caCertPath string) (*ConsensusManager, error) {
	nodeID := mainNode.SystemID()

	// Создаем директорию для данных Raft, если она не существует
	raftDir := filepath.Join(dataDir, "raft", nodeID)
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("ошибка при создании директории для Raft: %s", err)
	}

	// Создаем хранилище консенсуса
	store := NewConsensusStore(mainNode, grouping)

	// Создаем конфигурацию Raft
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// Создаем транспорт для Raft
	addr := fmt.Sprintf(":%d", raftPort)
	advertiseAddr := fmt.Sprintf("%s:%d", mainNode.Host, raftPort)
	transport, err := raft.NewTCPTransport(addr, &net.TCPAddr{IP: net.ParseIP(mainNode.Host), Port: int(raftPort)}, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании транспорта Raft: %s", err)
	}

	// Создаем снимки и хранилище логов
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании хранилища снимков: %s", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании хранилища логов: %s", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании стабильного хранилища: %s", err)
	}

	// Создаем Raft
	r, err := raft.NewRaft(config, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании Raft: %s", err)
	}

	// Создаем конфигурацию для первоначального кластера
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeID),
				Address: raft.ServerAddress(advertiseAddr),
			},
		},
	}

	// Применяем конфигурацию
	future := r.BootstrapCluster(configuration)
	if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
		return nil, fmt.Errorf("ошибка при инициализации кластера Raft: %s", err)
	}

	return &ConsensusManager{
		raft:        r,
		store:       store,
		nodeID:      nodeID,
		mainNode:    mainNode,
		dataDir:     dataDir,
		raftPort:    raftPort,
		networkPort: networkPort,
		certFile:    certFile,
		keyFile:     keyFile,
		caCertPath:  caCertPath,
		grouping:    grouping,
	}, nil
}

// JoinCluster присоединяет эту ноду к существующему Raft кластеру
func (c *ConsensusManager) JoinCluster(leaderAddr string) error {
	if c.raft.State() == raft.Leader {
		return fmt.Errorf("уже лидер, не может присоединиться к кластеру")
	}

	// Подключаемся к лидеру для присоединения к кластеру
	conn, err := net.DialTimeout("tcp", leaderAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("ошибка при подключении к лидеру: %s", err)
	}
	defer conn.Close()

	// Формируем запрос на присоединение
	nodeAddr := fmt.Sprintf("%s:%d", c.mainNode.Host, c.networkPort)
	joinRequest := map[string]string{
		"node_id": c.nodeID,
		"address": nodeAddr,
	}

	data, err := json.Marshal(joinRequest)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации запроса на присоединение: %s", err)
	}

	// Отправляем запрос на присоединение
	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("ошибка при отправке запроса на присоединение: %s", err)
	}

	return nil
}

// AddNodeToCluster добавляет новую ноду в Raft кластер
func (c *ConsensusManager) AddNodeToCluster(nodeID, addr string) error {
	if c.raft.State() != raft.Leader {
		return fmt.Errorf("не лидер, не может добавить ноду в кластер")
	}

	serverID := raft.ServerID(nodeID)
	serverAddr := raft.ServerAddress(addr)

	// Получаем текущую конфигурацию
	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("ошибка при получении конфигурации: %s", err)
	}

	// Проверяем, есть ли уже эта нода в кластере
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == serverID {
			if server.Address == serverAddr {
				// Нода уже в кластере с правильным адресом
				return nil
			}
			// Нода в кластере, но с другим адресом, обновляем
			future := c.raft.RemoveServer(serverID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("ошибка при удалении сервера с ID %s: %s", nodeID, err)
			}
			break
		}
	}

	// Добавляем ноду в кластер
	future := c.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("ошибка при добавлении ноды %s в кластер: %s", nodeID, err)
	}

	return nil
}

// IsNodeInCluster проверяет, является ли узел с указанным идентификатором частью кластера
func (cm *ConsensusManager) IsNodeInCluster(nodeID string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	configFuture := cm.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Error().Err(err).Msg("Ошибка при получении конфигурации Raft")
		return false
	}

	for _, server := range configFuture.Configuration().Servers {
		if string(server.ID) == nodeID {
			return true
		}
	}

	return false
}

// CreateGroup создает новую группу в консенсусе
func (c *ConsensusManager) CreateGroup(groupConfig GroupConfig) error {
	if c.raft.State() != raft.Leader {
		leader := c.raft.Leader()
		if leader == "" {
			return fmt.Errorf("нет лидера в кластере")
		}
		return fmt.Errorf("не лидер, не может создать группу, текущий лидер: %s", leader)
	}

	cmd := ConsensusCommand{
		Op:    "create_group",
		Key:   groupConfig.Name,
		Value: groupConfig,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации команды: %s", err)
	}

	// Применяем команду через Raft
	future := c.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("ошибка при применении команды: %s", err)
	}

	// Проверяем результат
	response := future.Response()
	if response != nil {
		return fmt.Errorf("ошибка при создании группы: %v", response)
	}

	return nil
}

func (cm *ConsensusManager) WaitForLeadership(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-time.After(time.Until(deadline)):
			return fmt.Errorf("таймаут ожидания становления лидером или обнаружения лидера")
		case <-ticker.C:
			if cm.raft.State() == raft.Leader {
				return nil
			}
		}
	}
}

// AddNodeToGroup добавляет ноду в группу
func (c *ConsensusManager) AddNodeToGroup(groupName string, nodeID string) error {
	if c.raft.State() != raft.Leader {
		leader := c.raft.Leader()
		if leader == "" {
			return fmt.Errorf("нет лидера в кластере")
		}
		return fmt.Errorf("не лидер, не может добавить ноду в группу, текущий лидер: %s", leader)
	}

	// Проверяем, существует ли группа
	c.store.mu.RLock()
	group, exists := c.store.groups[groupName]
	c.store.mu.RUnlock()

	if !exists {
		return fmt.Errorf("AddNodeToGroup группа не найдена: %s", groupName)
	}

	groupNodes := c.grouping.GetNodesOfGroup(groupName)

	// Проверяем, что размер группы не превышает максимальный
	if len(groupNodes) >= group.MaxSize {
		return fmt.Errorf("группа уже достигла максимального размера: %d", group.MaxSize)
	}

	cmd := ConsensusCommand{
		Op:    "add_node",
		Key:   groupName,
		Value: nodeID,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации команды: %s", err)
	}

	// Применяем команду через Raft
	future := c.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("ошибка при применении команды: %s", err)
	}

	// Проверяем результат
	response := future.Response()
	if response != nil {
		return fmt.Errorf("ошибка при добавлении ноды в группу: %v", response)
	}

	return nil
}

// ProposeVote предлагает голосование для добавления ноды в группу
func (c *ConsensusManager) ProposeVote(groupName string, candidateNode DNode) (*GroupVoteResponse, error) {
	// Проверяем, существует ли группа
	c.store.mu.RLock()
	group, exists := c.store.groups[groupName]
	if !exists {
		c.store.mu.RUnlock()
		return nil, fmt.Errorf("ProposeVote группа не найдена: %s", groupName)
	}

	groupNodes := c.grouping.GetNodesOfGroup(groupName)

	// Проверяем, что размер группы не превышает максимальный
	if len(groupNodes) >= group.MaxSize {
		c.store.mu.RUnlock()
		return &GroupVoteResponse{
			Approved: false,
			Message:  fmt.Sprintf("группа уже достигла максимального размера: %d", group.MaxSize),
		}, nil
	}

	nodesInGroup := make([]string, len(groupNodes))
	for i, groupNode := range groupNodes {
		nodesInGroup[i] = groupNode.SystemID()
	}
	c.store.mu.RUnlock()

	// Создаем запрос на голосование
	voteRequest := GroupVoteRequest{
		GroupName:     groupName,
		GroupID:       group.GroupID,
		CandidateNode: candidateNode,
		NodesInGroup:  nodesInGroup,
		ProposingNode: c.mainNode,
	}

	// Если мы лидер, сразу голосуем за предложение
	if c.raft.State() == raft.Leader {
		vote := struct {
			GroupName string `json:"group_name"`
			NodeID    string `json:"node_id"`
			Approved  bool   `json:"approved"`
		}{
			GroupName: groupName,
			NodeID:    c.nodeID,
			Approved:  true,
		}

		voteID := fmt.Sprintf("%s-%s", groupName, c.nodeID)
		cmd := ConsensusCommand{
			Op:    "vote",
			Key:   voteID,
			Value: vote,
		}

		data, err := json.Marshal(cmd)
		if err != nil {
			return nil, fmt.Errorf("ошибка при сериализации голоса: %s", err)
		}

		future := c.raft.Apply(data, 10*time.Second)
		if err := future.Error(); err != nil {
			return nil, fmt.Errorf("ошибка при применении голоса: %s", err)
		}
	}

	// Отправляем запрос на голосование всем нодам в группе
	responses := make(chan *GroupVoteResponse, len(nodesInGroup))
	var wg sync.WaitGroup

	for _, nodeID := range nodesInGroup {
		if nodeID == c.nodeID {
			// Это мы сами, уже проголосовали выше
			continue
		}

		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			// Получаем информацию о ноде из её ID
			var targetNode DNode

			// Парсинг nodeID для получения хоста и порта
			// В большинстве случаев ID ноды имеет формат host:port
			parts := strings.Split(nodeID, ":")
			if len(parts) == 2 {
				// Предполагаем формат ID ноды как host:port
				targetNode = DNode{
					Host: parts[0],
					Port: c.networkPort,
				}
			} else {
				panic("Invalid node ID")
			}

			if targetNode.Host == "" {
				log.Error().Str("nodeID", nodeID).Msg("Не удалось определить хост ноды")
				responses <- &GroupVoteResponse{
					Approved: false,
					Message:  fmt.Sprintf("Не удалось определить хост для ноды %s", nodeID),
				}
				return
			}

			// Создаем gRPC клиент для отправки запроса
			client, conn, err := c.createGRPCClient(targetNode)
			if err != nil {
				log.Error().Err(err).Str("node", fmt.Sprintf("%s:%d", targetNode.Host, targetNode.Port)).
					Msg("Ошибка при создании gRPC клиента")
				responses <- &GroupVoteResponse{
					Approved: false,
					Message:  fmt.Sprintf("Ошибка при подключении к ноде: %s", err),
				}
				return
			}
			defer conn.Close()

			// Подготавливаем запрос на голосование
			candidateNodeProto := &pb.DNode{
				Host:            candidateNode.Host,
				Port:            uint32(candidateNode.Port),
				LastAvailableAt: time.Now().Unix(),
			}

			proposingNodeProto := &pb.DNode{
				Host:            c.mainNode.Host,
				Port:            uint32(c.mainNode.Port),
				LastAvailableAt: time.Now().Unix(),
			}

			req := &pb.GroupVoteRequest{
				GroupName:     voteRequest.GroupName,
				GroupId:       voteRequest.GroupID,
				CandidateNode: candidateNodeProto,
				NodesInGroup:  voteRequest.NodesInGroup,
				ProposingNode: proposingNodeProto,
			}

			// Устанавливаем контекст с таймаутом для запроса
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Отправляем запрос на голосование
			resp, err := client.VoteOnGroupProposal(ctx, req)
			if err != nil {
				log.Error().Err(err).Str("node", fmt.Sprintf("%s:%d", targetNode.Host, targetNode.Port)).
					Msg("Ошибка при отправке запроса на голосование")
				responses <- &GroupVoteResponse{
					Approved: false,
					Message:  fmt.Sprintf("Ошибка при отправке запроса на голосование: %s", err),
				}
				return
			}

			// Преобразуем ответ в нужный формат и отправляем в канал
			responses <- &GroupVoteResponse{
				Approved: resp.Approved,
				Message:  resp.Message,
			}
		}(nodeID)
	}

	// Закрываем канал после получения всех ответов
	go func() {
		wg.Wait()
		close(responses)
	}()

	// Подсчитываем голоса
	approved := true
	var errorMsg string

	for resp := range responses {
		if !resp.Approved {
			approved = false
			errorMsg = resp.Message
			break
		}
	}

	if approved {
		// Все ноды одобрили, добавляем кандидата в группу
		err := c.AddNodeToGroup(groupName, candidateNode.SystemID())
		if err != nil {
			return nil, fmt.Errorf("ошибка при добавлении ноды в группу: %s", err)
		}

		return &GroupVoteResponse{
			Approved: true,
			Message:  "",
		}, nil
	}

	return &GroupVoteResponse{
		Approved: false,
		Message:  errorMsg,
	}, nil
}

// HandleVoteRequest обрабатывает входящий запрос на голосование
func (c *ConsensusManager) HandleVoteRequest(request *GroupVoteRequest) (*GroupVoteResponse, error) {
	// Проверяем, существует ли группа
	c.store.mu.RLock()
	group, exists := c.store.groups[request.GroupName]
	c.store.mu.RUnlock()

	if !exists {
		return &GroupVoteResponse{
			Approved: false,
			Message:  fmt.Sprintf("HandleVoteRequest группа не найдена: %s", request.GroupName),
		}, nil
	}

	groupNodes := c.grouping.GetNodesOfGroup(request.GroupName)
	// Проверяем, что мы являемся частью этой группы
	isMember := false
	for _, node := range groupNodes {
		if node.SystemID() == c.nodeID {
			isMember = true
			break
		}
	}

	if !isMember {
		return &GroupVoteResponse{
			Approved: false,
			Message:  "нода не является членом группы",
		}, nil
	}

	groupNodes = c.grouping.GetNodesOfGroup(request.GroupName)
	log.Debug().Msgf("HandleVoteRequest: nodsIds: %d. Max: %d", len(groupNodes), group.MaxSize)

	// Проверяем, что размер группы не превышает максимальный
	if len(groupNodes) >= group.MaxSize {
		log.Debug().Msgf("HandleVoteRequest: dont' approve. Max size reached: %d", group.MaxSize)
		return &GroupVoteResponse{
			Approved: false,
			Message:  fmt.Sprintf("группа уже достигла максимального размера: %d", group.MaxSize),
		}, nil
	}

	// Проверяем, что нода-кандидат не уже в группе
	for _, node := range groupNodes {
		if node.SystemID() == request.CandidateNode.SystemID() {
			return &GroupVoteResponse{
				Approved: false,
				Message:  "нода уже в группе",
			}, nil
		}
	}

	// Голосуем за предложение
	vote := struct {
		GroupName string `json:"group_name"`
		NodeID    string `json:"node_id"`
		Approved  bool   `json:"approved"`
	}{
		GroupName: request.GroupName,
		NodeID:    c.nodeID,
		Approved:  true,
	}

	voteID := fmt.Sprintf("%s-%s", request.GroupName, c.nodeID)
	cmd := ConsensusCommand{
		Op:    "vote",
		Key:   voteID,
		Value: vote,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("ошибка при сериализации голоса: %s", err)
	}

	future := c.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("ошибка при применении голоса: %s", err)
	}

	// Отправляем положительный ответ
	return &GroupVoteResponse{
		Approved: true,
		Message:  "",
	}, nil
}

// GetGroup возвращает информацию о группе по имени
func (c *ConsensusManager) GetGroup(groupName string) (*GroupState, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()

	group, exists := c.store.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("GetGroup группа не найдена: %s", groupName)
	}

	// Создаем копию группы, чтобы избежать гонки данных
	groupCopy := *group
	return &groupCopy, nil
}

// IsLeader проверяет, является ли текущая нода лидером Raft
func (c *ConsensusManager) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

func (c *ConsensusManager) IsClusterFulfilled() bool {
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		log.Error().Err(err).Msg("Не удалось получить конфигурацию Raft")
		return false
	}
	conf := future.Configuration()
	return len(conf.Servers) >= 3
}

// GetLeader возвращает адрес текущего лидера Raft
func (c *ConsensusManager) GetLeader() string {
	return string(c.raft.Leader())
}

// Shutdown останавливает Raft
func (c *ConsensusManager) Shutdown() error {
	future := c.raft.Shutdown()
	return future.Error()
}

// createGRPCClient создает gRPC клиент для взаимодействия с другой нодой
func (c *ConsensusManager) createGRPCClient(node DNode) (pb.DNetworkServiceClient, *grpc.ClientConn, error) {
	// Загружаем клиентский сертификат из директории данных
	certFile := c.certFile
	keyFile := c.keyFile
	caCertPath := c.caCertPath

	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при загрузке клиентского сертификата: %v", err)
	}

	// Загружаем CA сертификат
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при чтении CA сертификата: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, nil, fmt.Errorf("ошибка при добавлении CA сертификата в пул")
	}

	// Настраиваем TLS конфигурацию
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", node.Host, node.Port),
		grpc.WithTransportCredentials(creds),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при подключении к ноде %s:%d: %w", node.Host, node.Port, err)
	}

	client := pb.NewDNetworkServiceClient(conn)
	return client, conn, nil
}
