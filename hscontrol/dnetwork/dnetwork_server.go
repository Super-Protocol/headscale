package dnetwork

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
	"io/ioutil"
	"math/rand"
	"net"
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
}

// NewDNetworkServer creates a new instance of DNetworkServer
func NewDNetworkServer(config DNetworkServerConfig) *DNetworkServer {
	n := NewDNetwork()
	mainNode := NewDNode(config.AdvertiseHost, config.Port, time.Now())
	n.g.AddNode(*mainNode)
	for _, node := range config.BootstrapNodes {
		n.g.AddNode(node)
	}
	g := NewGossipInteraction(config.AdvertiseHost, config.Port, n)
	grouping := NewDNetworkGrouping(n, *mainNode, config.GroupingGoals)
	log.Info().Msg("Creating new DNetworkServer with push-based gossip mechanism")
	return &DNetworkServer{n: n, gossip: g, config: config, grouping: grouping, mainNode: *mainNode}
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

	return nil
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

	println(fmt.Sprintf("Proposing node: %v. Local node %v", proposingNode, ns.mainNode))

	sem := semaphore.NewWeighted(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sem.Acquire(ctx, 1)
	if err != nil {
		fmt.Println("Can't acquire mutex")
		return &pb.GroupProposalResponse{Status: 0}, nil
	}

	defer sem.Release(1)

	println("Got mutex")

	if ns.grouping.ResolveGroupJoinRequest(proposingNode, groupConfig.Name, 1, req.NodesConnected) {
		log.Info().Msgf("Node %s accepted group proposal for group %s", proposingNode.SystemID(), groupConfig.Name)
		ns.grouping.addNodeToGroup(proposingNode, groupConfig.Name, 1)
		return &pb.GroupProposalResponse{Status: 1}, nil
	}

	log.Info().Msgf("Node %s rejected group proposal for group %s", proposingNode.SystemID(), groupConfig.Name)
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
	println("Processing groups")
	sem := semaphore.NewWeighted(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sem.Acquire(ctx, 1)
	if err != nil {
		return
	}

	defer sem.Release(1)

	println("Got mutex Processing groups")
	for _, groupGoal := range ns.config.GroupingGoals {
		if ns.grouping.IsGroupFulfilled(groupGoal) {
			continue
		}
		candidates, err := ns.grouping.GetGroupingCandidates(groupGoal)
		if err != nil {
			log.Error().Msgf("Error getting Grouping candidates %s", err.Error())
			continue
		}
		log.Debug().Msgf("Grouping candidates of %v for group %s: %v", ns.mainNode, groupGoal.Name, candidates)
		selectedCandidate := candidates[rand.Intn(len(candidates))]
		response, err := ns.proposeGroupFormation(selectedCandidate, groupGoal)
		if err != nil {
			log.Error().Msgf("Error sending grouping request %s", err.Error())
			return
		}
		log.Debug().Msgf("proposeGroupFormation response %v", response)
		if response.Status == 1 {
			ns.grouping.addNodeToGroup(selectedCandidate, groupGoal.Name, 1)
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
