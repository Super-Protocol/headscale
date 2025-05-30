package dnetwork

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/juanfont/headscale/gen/go/dnetwork/v1"
)

const (
	M_LATENCY_CLASS = "latency_class"
)

// DNetworkServerConfig defines the server configuration
type DNetworkServerConfig struct {
	Port                   uint16        // e.g., 8443
	RaftPort               uint16        // e.g., 8444
	CertFile               string        // server certificate file
	KeyFile                string        // server key file
	CACertPath             string        // CA certificate file (for mutual TLS authentication)
	BootstrapNodes         []DNode       // initial list of headscale nodes (id, host, port)
	PollInterval           time.Duration // interval between node polls
	LatencyMeasureInterval time.Duration // interval between latency measures
	GroupingInterval       time.Duration // interval between latency measures
	AdvertiseHost          string
	GroupingGoals          []GroupConfig
}

// DNetworkServer represents a gRPC-enabled server
type DNetworkServer struct {
	pb.UnimplementedDNetworkServiceServer
	n                   *DNetwork
	gossip              *GossipInteraction
	grouping            *DNetworkGrouping
	config              DNetworkServerConfig
	mainNode            DNode
	groupFormationMutex sync.Mutex
	consensus           *ConsensusManager // Менеджер консенсуса Raft
}

// NewDNetworkServer creates a new instance of DNetworkServer
func NewDNetworkServer(config DNetworkServerConfig) *DNetworkServer {
	n := NewDNetwork()
	mainNode := NewDNode(config.AdvertiseHost, config.Port, config.RaftPort, time.Now())
	n.g.AddNode(*mainNode)
	for _, node := range config.BootstrapNodes {
		n.g.AddNode(node)
	}
	g := NewGossipInteraction(config.AdvertiseHost, config.Port, n)
	grouping := NewDNetworkGrouping(n, *mainNode, config.GroupingGoals)

	log.Info().Msg("Creating new DNetworkServer with push-based gossip mechanism")
	server := &DNetworkServer{
		n:        n,
		gossip:   g,
		config:   config,
		grouping: grouping,
		mainNode: *mainNode,
	}

	dataDir, err := os.MkdirTemp("", "raft-data")
	if err != nil {
		log.Error().Err(err).Msg("Failed to create temporary directory for Raft data")
		panic("Failed to create temporary directory for Raft data")
	}

	consensus, err := NewConsensusManager(*mainNode, grouping, dataDir, config.Port, config.RaftPort, config.CertFile, config.KeyFile, config.CACertPath)
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize Raft consensus manager")
		panic("Failed to initialize Raft consensus manager")
	}

	server.consensus = consensus
	log.Info().Msg("Raft consensus manager initialized successfully")

	log.Info().Msg("Ожидание становления лидером перед созданием групп...")
	if err := server.consensus.WaitForLeadership(30 * time.Second); err != nil { // Таймаут по необходимости
		log.Error().Err(err).Msg("Не удалось стать лидером для создания первоначальных групп")
		panic("Timeout waiting for single raft node leadership")
	} else {
		log.Info().Msg("Узел стал лидером, приступаем к созданию групп.")
	}

	for _, groupConfig := range config.GroupingGoals {
		err := consensus.CreateGroup(groupConfig)
		if err != nil {
			log.Error().Err(err).Str("group", groupConfig.Name).Msg("Failed to create group in consensus")
		} else {
			log.Info().Str("group", groupConfig.Name).Msg("Group created in consensus")
		}
	}

	return server
}

// startGRPCServer initializes and starts a gRPC server with mutual TLS authentication
func (ns *DNetworkServer) startGRPCServer() {
	caCertPool, err := loadCACertPool(ns.config.CACertPath)
	if err != nil {
		log.Fatal().Msgf("Error loading CA certificate: %v", err)
	}

	cert, err := tls.LoadX509KeyPair(ns.config.CertFile, ns.config.KeyFile)
	if err != nil {
		log.Fatal().Msgf("Failed to load server certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	}

	creds := credentials.NewTLS(tlsConfig)
	grpcServer := grpc.NewServer(grpc.Creds(creds))

	// Register the gRPC service implementation
	pb.RegisterDNetworkServiceServer(grpcServer, ns)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", ns.config.Port))
	if err != nil {
		log.Fatal().Msgf("Failed to listen on port %d: %v", ns.config.Port, err)
	}

	log.Printf("Starting gRPC server on port %d", ns.config.Port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal().Msgf("Failed to serve gRPC server: %v", err)
		}
	}()
}

// Start initializes and runs the DNetworkServer with gRPC
func (ns *DNetworkServer) Start() error {
	log.Info().
		Str("advertise_host", ns.config.AdvertiseHost).
		Uint16("port", ns.config.Port).
		Msg("Starting DNetworkServer")

	// Initialize bootstrap nodes
	for _, node := range ns.config.BootstrapNodes {
		ns.n.SetNodeIsAvailable(node.Host, node.Port)
	}

	// Start the gRPC server
	ns.startGRPCServer()

	// Start periodic node polling
	go ns.startSpreading()
	// Start periodic latency measurement
	go ns.startMeasuring()

	go ns.startGrouping()

	// Если это не первичная нода и есть bootstrap nodes, пытаемся присоединиться к кластеру
	if len(ns.config.BootstrapNodes) > 0 {
		go ns.joinConsensusClusterFromBootstrap()
	}

	return nil
}

// VoteOnGroupProposal обрабатывает запрос на голосование по группе
func (ns *DNetworkServer) VoteOnGroupProposal(ctx context.Context, req *pb.GroupVoteRequest) (*pb.GroupVoteResponse, error) {
	if ns.consensus == nil {
		return &pb.GroupVoteResponse{
			Approved: false,
			Message:  "Консенсус не инициализирован",
		}, nil
	}

	candidateNode := DNode{
		Host: req.CandidateNode.Host,
		Port: uint16(req.CandidateNode.Port),
	}

	proposingNode := DNode{
		Host: req.ProposingNode.Host,
		Port: uint16(req.ProposingNode.Port),
	}

	voteRequest := &GroupVoteRequest{
		GroupName:     req.GroupName,
		GroupID:       req.GroupId,
		CandidateNode: candidateNode,
		NodesInGroup:  req.NodesInGroup,
		ProposingNode: proposingNode,
	}

	response, err := ns.consensus.HandleVoteRequest(voteRequest)
	if err != nil {
		return &pb.GroupVoteResponse{
			Approved: false,
			Message:  err.Error(),
		}, nil
	}

	return &pb.GroupVoteResponse{
		Approved: response.Approved,
		Message:  response.Message,
	}, nil
}

// RequestConsensusVote обрабатывает запрос на голосование через консенсус
func (ns *DNetworkServer) RequestConsensusVote(ctx context.Context, req *pb.ConsensusVoteRequest) (*pb.ConsensusVoteResponse, error) {
	if ns.consensus == nil {
		return &pb.ConsensusVoteResponse{
			Accepted: false,
			Message:  "Консенсус не инициализирован",
		}, nil
	}

	vote := struct {
		GroupName string `json:"group_name"`
		NodeID    string `json:"node_id"`
		Approved  bool   `json:"approved"`
	}{
		GroupName: req.GroupName,
		NodeID:    req.NodeId,
		Approved:  req.Approved,
	}

	cmd := ConsensusCommand{
		Op:    "vote",
		Key:   req.VoteId,
		Value: vote,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return &pb.ConsensusVoteResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Ошибка при сериализации голоса: %s", err),
		}, nil
	}

	future := ns.consensus.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.ConsensusVoteResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Ошибка при применении голоса: %s", err),
		}, nil
	}

	return &pb.ConsensusVoteResponse{
		Accepted: true,
		Message:  "",
	}, nil
}

// JoinConsensusCluster обрабатывает запрос на присоединение к Raft кластеру
func (ns *DNetworkServer) JoinConsensusCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	log.Info().Str("node_id", req.NodeId).Str("address", req.Address).Msg("Получен запрос на присоединение к кластеру консенсуса")

	if ns.consensus == nil {
		log.Warn().Str("node_id", req.NodeId).Msg("Отказ в присоединении: консенсус не инициализирован")
		return &pb.JoinClusterResponse{
			Accepted: false,
			Message:  "Консенсус не инициализирован",
		}, nil
	}

	// Проверяем, что мы лидер
	if !ns.consensus.IsLeader() {
		leader := ns.consensus.GetLeader()
		log.Warn().Str("node_id", req.NodeId).Str("leader", leader).Msg("Отказ в присоединении: этот узел не является лидером")
		return &pb.JoinClusterResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Не лидер, текущий лидер: %s", leader),
		}, nil
	}

	// Проверяем, не является ли нода уже частью кластера
	if ns.consensus.IsNodeInCluster(req.NodeId) {
		log.Info().Str("node_id", req.NodeId).Msg("Нода уже является частью кластера")
		return &pb.JoinClusterResponse{
			Accepted: true,
			Message:  "Нода уже является частью кластера",
		}, nil
	}

	// Добавляем ноду в кластер
	log.Info().Str("node_id", req.NodeId).Str("address", req.Address).Msg("Добавление ноды в кластер консенсуса")
	err := ns.consensus.AddNodeToCluster(req.NodeId, req.Address)
	if err != nil {
		log.Error().Err(err).Str("node_id", req.NodeId).Msg("Ошибка при добавлении ноды в кластер")
		return &pb.JoinClusterResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Ошибка при добавлении ноды в кластер: %s", err),
		}, nil
	}

	log.Info().Str("node_id", req.NodeId).Msg("Нода успешно добавлена в кластер консенсуса")
	return &pb.JoinClusterResponse{
		Accepted: true,
		Message:  "",
	}, nil
}

// MeasureLatency implements the gRPC MeasureLatency method.
func (ns *DNetworkServer) MeasureLatency(ctx context.Context, _ *emptypb.Empty) (*pb.MeasureLatencyResponse, error) {
	return &pb.MeasureLatencyResponse{Status: "OK"}, nil
}

// SpreadGossip implements the gRPC SpreadGossip method.
// Converts received proto data into internal structures.
func (ns *DNetworkServer) SpreadGossip(ctx context.Context, req *pb.SpreadGossipRequest) (*pb.SpreadGossipResponse, error) {
	// Convert proto measurements into internal format: map[string]map[string]map[string]Measurement

	internalMeasurements := make(map[string]map[string]map[string]Measurement)
	for key, measurementMap := range req.Measurements {
		internalMeasurements[key] = make(map[string]map[string]Measurement)
		for innerKey, subMap := range measurementMap.Measurements {
			internalMeasurements[key][innerKey] = make(map[string]Measurement)
			for mKey, measurement := range subMap.Measurements {
				internalMeasurements[key][innerKey][mKey] = Measurement{
					Value:              measurement.Value,
					CreationTimeUnix:   measurement.CreationTimeUnix,
					ExpirationTimeUnix: measurement.ExpirationTimeUnix,
				}
			}
		}
	}

	// Convert the array of proto nodes into internal DNode type
	internalNodes := make([]DNode, len(req.Nodes))
	for i, node := range req.Nodes {
		internalNodes[i] = DNode{
			Host:            node.Host,
			Port:            uint16(node.Port),
			LastAvailableAt: time.Unix(node.LastAvailableAt, 0),
		}
	}

	ns.gossip.HandleInfoReceived(internalNodes, internalMeasurements)
	log.Debug().Int("node_count", len(internalNodes)).Msg("Gossip information received and processed via gRPC")
	return &pb.SpreadGossipResponse{Status: "OK"}, nil
}

// ProposeGroupFormation handles incoming group formation proposals
func (ns *DNetworkServer) ProposeGroupFormation(ctx context.Context, req *pb.GroupProposalRequest) (*pb.GroupProposalResponse, error) {
	log.Debug().Msgf("ProposeGroupFormation called")
	proposingNode := DNode{
		Host: req.ProposingNode.Host,
		Port: uint16(req.ProposingNode.Port),
	}
	groupConfig := convertProtoToGroupConfig(req.GroupConfig)

	log.Debug().Msgf("Proposing node: %v. Local node %v", proposingNode, ns.mainNode)

	// Используем Raft консенсус для принятия решения
	voteRequest := &GroupVoteRequest{
		GroupName:     groupConfig.Name,
		CandidateNode: proposingNode,
		NodesInGroup:  req.NodesConnected,
		ProposingNode: proposingNode,
	}

	// Обрабатываем запрос голосования
	response, err := ns.consensus.HandleVoteRequest(voteRequest)
	if err != nil {
		log.Error().Err(err).Msg("Error handling vote request")
		return &pb.GroupProposalResponse{Status: 0}, nil
	}

	if response.Approved {
		log.Info().Msgf("Node %s accepted group proposal for group %s via consensus", proposingNode.SystemID(), groupConfig.Name)
		// Добавляем ноду в локальное представление группы
		//ns.grouping.addNodeToGroup(proposingNode, groupConfig.Name, 1)
		return &pb.GroupProposalResponse{Status: 1}, nil
	}

	log.Info().Msgf("Node %s rejected group proposal for group %s via consensus: %s",
		proposingNode.SystemID(), groupConfig.Name, response.Message)
	return &pb.GroupProposalResponse{Status: 0}, nil
}

// startMeasuring periodically measures latency to all nodes in the network.
func (ns *DNetworkServer) startMeasuring() {
	ticker := time.NewTicker(ns.config.LatencyMeasureInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.measureLatencyToAllNodes()
		}
	}
}

func (ns *DNetworkServer) startGrouping() {
	ticker := time.NewTicker(ns.config.GroupingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.processGroups()
		}
	}
}

func (ns *DNetworkServer) processGroups() {
	log.Debug().Msg("Processing groups")

	if !ns.consensus.IsClusterFulfilled() {
		log.Debug().Msg("Cluster is not fulfilled yet")
		return
	}

	log.Debug().Msg("Lock acquired for group processing")
	for _, groupGoal := range ns.config.GroupingGoals {
		// Проверяем, выполнена ли уже группа
		if ns.grouping.IsGroupFulfilled(groupGoal) {
			log.Debug().Msgf("Group %s is already fulfilled", groupGoal.Name)
			continue
		}

		// Получаем кандидатов для группы
		candidates, err := ns.grouping.GetGroupingCandidates(groupGoal)
		if err != nil {
			log.Error().Err(err).Str("group", groupGoal.Name).Msg("Error retrieving candidates for grouping")
			continue
		}

		if len(candidates) == 0 {
			log.Debug().Str("group", groupGoal.Name).Msg("No suitable candidates for group")
			continue
		}

		log.Debug().Str("group", groupGoal.Name).Int("candidates", len(candidates)).Msg("Found candidates for group")

		// Выбираем кандидата и пытаемся сформировать группу
		selectedCandidate := candidates[rand.Intn(len(candidates))]

		voteResponse, err := ns.consensus.ProposeVote(groupGoal.Name, selectedCandidate)
		if err != nil {
			log.Error().Err(err).Str("group", groupGoal.Name).
				Str("candidate", selectedCandidate.SystemID()).
				Msg("Error proposing vote via consensus")
			continue
		}

		if voteResponse.Approved {
			log.Info().Str("node", selectedCandidate.SystemID()).
				Str("group", groupGoal.Name).
				Msg("Node has joined group via consensus")
			// Добавляем ноду в локальное представление группы
			//ns.grouping.addNodeToGroup(selectedCandidate, groupGoal.Name, 1)
		} else {
			log.Debug().Str("node", selectedCandidate.SystemID()).
				Str("group", groupGoal.Name).
				Str("reason", voteResponse.Message).
				Msg("Vote to add node to group was rejected")
		}
	}
}

// measureLatencyToAllNodes measures latency to all nodes and updates measurements.
func (ns *DNetworkServer) measureLatencyToAllNodes() {
	nodes := ns.n.GetAllNodes()

	localNode := ns.mainNode

	for _, node := range nodes {
		if node == localNode {
			continue
		}
		go func(node DNode) {
			start := time.Now()
			err := ns.pingNode(node)
			latency := time.Since(start)
			if err != nil {
				log.Error().Err(err).Str("node", node.Host).Msg("Failed to measure latency")
				ns.n.SetNodeIsNotAvailable(node.Host, node.Port)
				return
			}
			ns.n.SetNodeIsAvailable(node.Host, node.Port)
			classes := getLatencyClasses(latency)
			shouldUpdate := false
			existingM, exists := ns.n.g.GetMeasurement(localNode, node, M_LATENCY_CLASS)
			if exists {
				// If class changed
				if _, exists := classes[uint32(existingM.Value)]; !exists {
					shouldUpdate = true
				}
			} else {
				shouldUpdate = true
			}
			if shouldUpdate {
				newClass, _ := minClass(classes)
				newMeasurement := Measurement{
					Value:              int64(newClass),
					CreationTimeUnix:   uint64(time.Now().Unix()),
					ExpirationTimeUnix: 0,
				}
				ns.n.g.SetMeasurement(localNode, node, M_LATENCY_CLASS, newMeasurement)
				//log.Debug().Str("node", node.Host).Uint32(M_LATENCY_CLASS, newClass).Msg("Latency measured")
			}
		}(node)
	}
}

// pingNode sends a MeasureLatency request to a node to measure round-trip time.
func (ns *DNetworkServer) pingNode(node DNode) error {
	client, conn, err := ns.createGRPCClient(node)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.MeasureLatency(context.Background(), &emptypb.Empty{})
	return err
}

func (ns *DNetworkServer) proposeGroupFormation(node DNode, g GroupConfig) (*pb.GroupProposalResponse, error) {
	client, conn, err := ns.createGRPCClient(node)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	nodesOfGroup := ns.grouping.GetNodesOfGroup(g.Name)

	nodesConnected := make([]string, len(nodesOfGroup))
	for i, node := range nodesOfGroup {
		nodesConnected[i] = node.SystemID()
	}

	response, err := client.ProposeGroupFormation(context.Background(), &pb.GroupProposalRequest{
		GroupConfig:    convertGroupConfigToProto(g),
		ProposingNode:  convertNodeToProto(ns.mainNode),
		NodesConnected: nodesConnected,
	})
	return response, err
}

// createGRPCClient creates a gRPC client for a given node.
func (ns *DNetworkServer) createGRPCClient(node DNode) (pb.DNetworkServiceClient, *grpc.ClientConn, error) {
	clientCert, err := tls.LoadX509KeyPair(ns.config.CertFile, ns.config.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load client certificate: %v", err)
	}
	caCertPool, err := loadCACertPool(ns.config.CACertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load CA certificate: %v", err)
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}
	creds := credentials.NewTLS(tlsConfig)
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", node.Host, node.Port), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to node %s:%d: %w", node.Host, node.Port, err)
	}
	client := pb.NewDNetworkServiceClient(conn)
	return client, conn, nil
}

// startSpreading periodically fetches gossip candidates and sends local information to them
func (ns *DNetworkServer) startSpreading() {
	ticker := time.NewTicker(ns.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Fetch gossip candidates
			candidates := ns.gossip.GetInteractionCandidates(1)
			if len(candidates) == 0 {
				log.Debug().Msg("No gossip candidates available")
				continue
			}

			// Get local information
			nodes, measurements := ns.gossip.GetInfo()

			// Send gossip data to the first candidate
			candidate := candidates[0]
			err := ns.sendGossipToCandidate(candidate, nodes, measurements)
			if err != nil {
				log.Error().Err(err).Str("candidate", candidate.Host).Msg("Failed to send gossip")
			}
		}
	}
}

// sendGossipToCandidate sends gossip data to a specific candidate
func (ns *DNetworkServer) sendGossipToCandidate(candidate DNode, nodes []DNode, measurements map[string]map[string]map[string]Measurement) error {
	// Настройка mutual TLS для клиента
	clientCert, err := tls.LoadX509KeyPair(ns.config.CertFile, ns.config.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load client certificate: %v", err)
	}
	caCertPool, err := loadCACertPool(ns.config.CACertPath)
	if err != nil {
		return fmt.Errorf("failed to load CA certificate: %v", err)
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, asn1Data := range rawCerts {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return err
				}
				certs[i] = cert
			}
			opts := x509.VerifyOptions{
				Roots:       caCertPool,
				CurrentTime: time.Now(),
			}
			_, err := certs[0].Verify(opts)
			return err
		},
	}
	creds := credentials.NewTLS(tlsConfig)
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", candidate.Host, candidate.Port), grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("failed to connect with mutual TLS to candidate %s:%d: %w", candidate.Host, candidate.Port, err)
	}
	defer conn.Close()

	client := pb.NewDNetworkServiceClient(conn)
	grpcNodes := make([]*pb.DNode, len(nodes))
	for i, node := range nodes {
		grpcNodes[i] = &pb.DNode{
			Host:            node.Host,
			Port:            uint32(node.Port),
			LastAvailableAt: node.LastAvailableAt.Unix(),
		}
	}

	grpcMeasurements := make(map[string]*pb.MeasurementMap)
	for key, measurementMap := range measurements {
		grpcMeasurements[key] = &pb.MeasurementMap{
			Measurements: make(map[string]*pb.MeasurementSubMap),
		}
		for innerKey, subMap := range measurementMap {
			grpcMeasurements[key].Measurements[innerKey] = &pb.MeasurementSubMap{
				Measurements: make(map[string]*pb.Measurement),
			}
			for mKey, measurement := range subMap {
				grpcMeasurements[key].Measurements[innerKey].Measurements[mKey] = &pb.Measurement{
					Value:              measurement.Value,
					CreationTimeUnix:   measurement.CreationTimeUnix,
					ExpirationTimeUnix: measurement.ExpirationTimeUnix,
				}
			}
		}
	}

	req := &pb.SpreadGossipRequest{
		Nodes:        grpcNodes,
		Measurements: grpcMeasurements,
	}

	_, err = client.SpreadGossip(context.Background(), req)
	if err != nil {
		ns.n.SetNodeIsNotAvailable(candidate.Host, candidate.Port)
		return fmt.Errorf("failed to spread gossip to candidate %s:%d: %w", candidate.Host, candidate.Port, err)
	}
	ns.n.SetNodeIsAvailable(candidate.Host, candidate.Port)

	log.Debug().Str("candidate", candidate.Host).Msg("Successfully sent gossip")
	return nil
}

func getLatencyClasses(latency time.Duration) map[uint32]struct{} {
	type latencyClass struct {
		id    uint32
		start time.Duration
		end   time.Duration
	}

	groups := []latencyClass{
		{0, 0 * time.Millisecond, 20 * time.Millisecond},
		{1, 10 * time.Millisecond, 50 * time.Millisecond},
		{2, 30 * time.Millisecond, 100 * time.Millisecond},
		{3, 80 * time.Millisecond, 200 * time.Millisecond},
		{4, 150 * time.Millisecond, 400 * time.Millisecond},
		{5, 300 * time.Millisecond, 1 * time.Second},
		{6, 900 * time.Millisecond, 10 * time.Second},
	}

	result := make(map[uint32]struct{})
	result[7] = struct{}{} // Always add worse class
	for _, g := range groups {
		if latency >= g.start && latency < g.end {
			result[g.id] = struct{}{}
		}
	}

	return result
}

func minClass(m map[uint32]struct{}) (uint32, bool) {
	if len(m) == 0 {
		return 0, false
	}

	keys := make([]uint32, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return slices.Min(keys), true
}

// loadCACertPool loads the CA certificate and returns a certificate pool
func loadCACertPool(caCertPath string) (*x509.CertPool, error) {
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append CA certificate")
	}
	return caCertPool, nil
}

// joinConsensusClusterFromBootstrap пытается присоединиться к существующему кластеру консенсуса
// через bootstrap ноды. Метод перебирает bootstrap ноды и пытается подключиться к первой доступной.
func (ns *DNetworkServer) joinConsensusClusterFromBootstrap() {
	// Ожидаем небольшую задержку перед попыткой присоединения, чтобы gRPC сервер успел запуститься
	time.Sleep(2 * time.Second)

	if ns.consensus.IsLeader() {
		log.Info().Msg("Этот узел уже является лидером кластера, пропускаем присоединение")
		return
	}

	log.Info().Msg("Попытка присоединения к кластеру консенсуса через bootstrap ноды")

	// Идентификатор нашего узла для кластера
	nodeID := ns.mainNode.SystemID()

	// Адрес для Raft
	raftAddr := fmt.Sprintf("%s:%d", ns.config.AdvertiseHost, ns.config.RaftPort)

	// Перебираем все bootstrap ноды, пытаемся присоединиться к первой доступной
	maxRetries := 10
	retryInterval := 5 * time.Second

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			log.Info().Int("retry", retry).Dur("interval", retryInterval).
				Msg("Повторная попытка присоединения к кластеру")
			time.Sleep(retryInterval)
		}

		for _, bootstrapNode := range ns.config.BootstrapNodes {
			log.Info().Str("bootstrap_node", bootstrapNode.Host).
				Uint16("port", bootstrapNode.Port).
				Msg("Попытка присоединения к кластеру через ноду")

			client, conn, err := ns.createGRPCClient(bootstrapNode)
			if err != nil {
				log.Error().Err(err).Str("node", bootstrapNode.Host).
					Msg("Не удалось создать gRPC клиент для bootstrap ноды")
				continue
			}

			// Отправляем запрос на присоединение
			joinReq := &pb.JoinClusterRequest{
				NodeId:  nodeID,
				Address: raftAddr,
			}

			resp, err := client.JoinConsensusCluster(context.Background(), joinReq)
			conn.Close()

			if err != nil {
				log.Error().Err(err).Str("node", bootstrapNode.Host).
					Msg("Ошибка при отправке запроса на присоединение к кластеру")
				continue
			}

			if resp.Accepted {
				log.Info().Str("node", bootstrapNode.Host).
					Msg("Успешно присоединились к кластеру консенсуса")
				return
			} else {
				log.Warn().Str("node", bootstrapNode.Host).
					Str("message", resp.Message).
					Msg("Запрос на присоединение к кластеру отклонен")

				// Если сообщение указывает на другого лидера, пробуем подключиться к нему
				if resp.Message != "" && resp.Message != "Консенсус не инициализирован" {
					if resp.Message[:9] == "Не лидер" {
						// Ищем адрес лидера в сообщении
						log.Info().Str("message", resp.Message).
							Msg("Получена информация о текущем лидере, переключаемся на него")

						// Пробуем найти ноду лидера в нашем списке нод
						leaderHost := resp.Message[len(resp.Message)-15:]
						for _, node := range ns.n.GetAllNodes() {
							if node.SystemID() == leaderHost {
								log.Info().Str("leader", node.Host).
									Uint16("port", node.Port).
									Msg("Пробуем подключиться к лидеру")

								// Создаем gRPC клиент для лидера
								leaderClient, leaderConn, err := ns.createGRPCClient(node)
								if err != nil {
									log.Error().Err(err).Str("node", node.Host).
										Msg("Не удалось создать gRPC клиент для лидера")
									break
								}

								// Отправляем запрос на присоединение
								leaderResp, err := leaderClient.JoinConsensusCluster(context.Background(), joinReq)
								leaderConn.Close()

								if err != nil {
									log.Error().Err(err).Str("node", node.Host).
										Msg("Ошибка при отправке запроса на присоединение к лидеру")
									break
								}

								if leaderResp.Accepted {
									log.Info().Str("node", node.Host).
										Msg("Успешно присоединились к кластеру консенсуса через лидера")
									return
								} else {
									log.Warn().Str("node", node.Host).
										Str("message", leaderResp.Message).
										Msg("Запрос на присоединение к кластеру через лидера отклонен")
								}
								break
							}
						}
					}
				}
			}
		}
	}

	log.Error().Int("max_retries", maxRetries).
		Msg("Исчерпаны все попытки присоединения к кластеру консенсуса")
}

// convertGroupConfigToProto converts a Go GroupConfig struct to a protobuf GroupConfig
func convertGroupConfigToProto(config GroupConfig) *pb.GroupConfig {
	size := &pb.GroupSize{
		Min: int32(config.Size.Min),
		Max: int32(config.Size.Max),
	}

	measurements := make([]*pb.GroupCriteria, len(config.Criteria))
	for i, criteria := range config.Criteria {
		measurements[i] = &pb.GroupCriteria{
			Name:      criteria.Name,
			Condition: criteria.Condition,
		}
		if criteria.Value != nil {
			measurements[i].Value = *criteria.Value
		}
	}

	return &pb.GroupConfig{
		Name:         config.Name,
		Size:         size,
		Measurements: measurements,
	}
}

func convertNodeToProto(node DNode) *pb.DNode {
	return &pb.DNode{
		Host:            node.Host,
		Port:            uint32(node.Port),
		LastAvailableAt: time.Now().Unix(),
	}
}
