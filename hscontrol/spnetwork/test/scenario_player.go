package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork"
	"github.com/juanfont/headscale/hscontrol/spnetwork/api"
	"log"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/consensus"
	"github.com/juanfont/headscale/hscontrol/spnetwork/grouping"
	"github.com/juanfont/headscale/hscontrol/spnetwork/measurer"
	g "github.com/juanfont/headscale/hscontrol/spnetwork/syncer/gossip"
)

// --- Declaring scenario step types ---

type StepType int

const (
	StepLaunchServer StepType = iota
	StepStopServer
	StepAddBlacklist
	StepRemoveBlacklist
	StepAddGoal
	StepRemoveGoal
)

type ScenarioStep struct {
	At              time.Duration // how long from the start of the test
	Type            StepType
	NodeID          string                        // node identifier
	Bootstrap       []string                      // for StepLaunchServer: list of NodeIDs
	GoalID          string                        // for StepAddGoal, StepRemoveGoal: ID of existing goal or empty string for new one
	MinGroupSize    int                           // for StepAddGoal: minimum group size
	MaxGroupSize    int                           // for StepAddGoal: maximum group size
	InactiveTimeout int64                         // for StepAddGoal: inactivity timeout in seconds
	Criteria        []entities.DimensionCriterion // for StepAddGoal: measurement criteria for grouping
	Message         string                        // optional message to add to the event log
}

// --- Scenario "Player" — encapsulates all creation and startup logic ---

type ScenarioPlayer struct {
	caFile    string
	certDir   string
	basePort  int
	nextPort  int
	showGraph bool
	apiServer *api.Server
	apiPort   int
	// stores the state of running servers and registries by NodeID
	servers    map[string]*spnetwork.SPNetwork
	registries map[string]*common.EntityRegistry
	blacklist  map[string]bool
	eventLog   []string // event log for API transmission
}

func NewScenarioPlayer(basePort int, showGraph bool) *ScenarioPlayer {
	// Create a temporary directory for certificates
	tempDir, err := os.MkdirTemp("", "spnetwork-certs-")
	if err != nil {
		log.Fatalf("Failed to create temporary directory for certificates: %v", err)
	}

	// Generate CA certificate
	caFile, err := GenerateOrLoadCA(tempDir)
	if err != nil {
		log.Fatalf("Failed to generate CA certificate: %v", err)
	}

	log.Printf("Created temporary directory for certificates: %s", tempDir)
	log.Printf("Generated CA certificate: %s", caFile)

	// API server listens on port 8911 by default
	apiPort := 8911

	return &ScenarioPlayer{
		caFile:     caFile,
		certDir:    tempDir,
		basePort:   basePort,
		nextPort:   0,
		showGraph:  showGraph,
		apiPort:    apiPort,
		servers:    make(map[string]*spnetwork.SPNetwork),
		registries: make(map[string]*common.EntityRegistry),
		blacklist:  make(map[string]bool),
		eventLog:   []string{"Scenario initialized"},
	}
}

// Helper constructor for backward compatibility
func NewScenarioPlayerWithoutGraph(basePort int) *ScenarioPlayer {
	return NewScenarioPlayer(basePort, false)
}

// addGoal adds a grouping goal to the registry of the specified node
func (p *ScenarioPlayer) addGoal(nodeID, goalID string, minSize, maxSize int, inactiveTimeout int64, criteria []entities.DimensionCriterion) error {
	reg, ok := p.registries[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	var goal *entities.GroupGoal

	// If goalID is not specified, create a new goal
	if goalID == "" {
		goal = entities.NewGroupGoalWithTimeout(minSize, maxSize, inactiveTimeout)
	} else {
		// If goalID is specified, first check if such a goal exists
		existingGoal, err := reg.GroupGoal.GetEntity(goalID)
		if err == nil {
			// Goal exists, update its parameters
			existingGoal.SetMinGroupSize(minSize)
			existingGoal.SetMaxGroupSize(maxSize)
			existingGoal.SetInactivityTimeout(inactiveTimeout)
			goal = existingGoal
		} else {
			// Goal does not exist, create a new one with the specified ID
			goal = entities.NewGroupGoalWithTimeout(minSize, maxSize, inactiveTimeout)
			goal.ID = goalID
		}
	}

	// Set measurement criteria if specified
	if len(criteria) > 0 {
		goal.SetDimensionCriteria(criteria)
		for _, criterion := range criteria {
			log.Printf("  Added criterion: type=%s, condition=%s, values=%v",
				criterion.Type, criterion.Condition, criterion.Values)
		}
	}

	// Save the goal in the registry
	_, err := reg.GroupGoal.StoreEntity(goal)
	if err != nil {
		return fmt.Errorf("failed to save goal: %v", err)
	}

	log.Printf("Added grouping goal %s for node %s: min=%d, max=%d, timeout=%d, criteria=%d",
		goal.GetID(), nodeID, minSize, maxSize, inactiveTimeout, len(criteria))

	return nil
}

// removeGoal removes a grouping goal from the registry of the specified node
func (p *ScenarioPlayer) removeGoal(nodeID, goalID string) error {
	reg, ok := p.registries[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Get the goal by ID
	goal, err := reg.GroupGoal.GetEntity(goalID)
	if err != nil {
		return fmt.Errorf("goal %s not found: %v", goalID, err)
	}

	// Mark the goal as deleted
	goal.MarkDeleted()

	// Save changes to the registry
	_, err = reg.GroupGoal.StoreEntity(goal)
	if err != nil {
		return fmt.Errorf("failed to delete goal: %v", err)
	}

	log.Printf("Removed grouping goal %s from node %s", goalID, nodeID)
	return nil
}

func (p *ScenarioPlayer) Play(steps []ScenarioStep) {
	sort.Slice(steps, func(i, j int) bool { return steps[i].At < steps[j].At })
	start := time.Now()

	for _, s := range steps {
		if wait := s.At - time.Since(start); wait > 0 {
			time.Sleep(wait)
		}

		// Form the basic step message
		stepMsg := fmt.Sprintf("[%.0fs] >>> %v %s", time.Since(start).Seconds(), s.Type, s.NodeID)
		log.Println(stepMsg)

		// If the step has its own message, add it to the event log
		if s.Message != "" {
			p.addToEventLog(s.Message)
		}

		switch s.Type {
		case StepLaunchServer:
			if err := p.launch(s.NodeID, s.Bootstrap); err != nil {
				errorMsg := fmt.Sprintf("ERROR launch %s: %v", s.NodeID, err)
				log.Println(errorMsg)
			}
		case StepStopServer:
			if srv, ok := p.servers[s.NodeID]; ok {
				srv.Stop()
			}
		case StepAddBlacklist:
			p.blacklist[p.servers[s.NodeID].LocalNode.GetID()] = true
		case StepRemoveBlacklist:
			delete(p.blacklist, p.servers[s.NodeID].LocalNode.GetID())
		case StepAddGoal:
			if err := p.addGoal(s.NodeID, s.GoalID, s.MinGroupSize, s.MaxGroupSize, s.InactiveTimeout, s.Criteria); err != nil {
				errorMsg := fmt.Sprintf("ERROR add goal for %s: %v", s.NodeID, err)
				log.Println(errorMsg)
			}
		case StepRemoveGoal:
			if err := p.removeGoal(s.NodeID, s.GoalID); err != nil {
				errorMsg := fmt.Sprintf("ERROR remove goal %s from %s: %v", s.GoalID, s.NodeID, err)
				log.Println(errorMsg)
			}
		}
	}
}

// addToEventLog adds a message to the event log
func (p *ScenarioPlayer) addToEventLog(message string) {
	timestamp := time.Now().Format("15:04:05.000")
	logEntry := fmt.Sprintf("[%s] %s", timestamp, message)
	p.eventLog = append(p.eventLog, logEntry)

	// If the API server is already running, update the log in it
	if p.apiServer != nil {
		// Since p.eventLog is a reference to an array,
		// the API server will automatically see the updates
	}
}

// Cleanup frees resources used by ScenarioPlayer,
// including deleting the temporary certificate directory
func (p *ScenarioPlayer) Cleanup() {
	// Stop all servers
	for id, srv := range p.servers {
		log.Printf("Stopping server %s", id)
		srv.Stop()
	}

	// Stop the API server if it's running
	if p.apiServer != nil {
		log.Printf("Stopping API server")
		err := p.apiServer.Stop()
		if err != nil {
			log.Printf("ERROR when stopping API server: %v", err)
		}
	}

	// Remove the temporary certificate directory
	if p.certDir != "" {
		log.Printf("Removing temporary certificate directory: %s", p.certDir)
		err := os.RemoveAll(p.certDir)
		if err != nil {
			log.Printf("ERROR when removing temporary directory %s: %v", p.certDir, err)
		}
	}
}

// launchAPIServer launches an API server for network visualization
func (p *ScenarioPlayer) launchAPIServer(registry *common.EntityRegistry) error {
	if !p.showGraph || p.apiServer != nil {
		return nil
	}

	log.Printf("Launching API server on port %d", p.apiPort)

	// Create a new API server and pass it the event log
	p.apiServer = api.NewServer(registry, &p.eventLog)

	// Launch the API server in a separate goroutine
	go func() {
		apiAddr := fmt.Sprintf("127.0.0.1:%d", p.apiPort)
		err := p.apiServer.Start(apiAddr)
		if err != nil {
			log.Printf("Error launching API server: %v", err)
		}
	}()

	// Give the server time to start
	time.Sleep(2 * time.Second)

	// Launch the visualization script
	err := p.launchGraphVisualizer()
	if err != nil {
		log.Printf("Error launching visualizer: %v", err)
	}

	return nil
}

// launchGraphVisualizer launches Python script for graph visualization
func (p *ScenarioPlayer) launchGraphVisualizer() error {
	if !p.showGraph {
		return nil
	}

	log.Printf("Launching graph visualizer")

	// Path to graph.py script (in the same directory as the test file)
	scriptPath, err := filepath.Abs("graph.py")
	if err != nil {
		return fmt.Errorf("failed to get absolute path to graph.py: %v", err)
	}

	// Check if the file exists
	_, err = os.Stat(scriptPath)
	if os.IsNotExist(err) {
		// Try to find in the current test directory
		currentDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory: %v", err)
		}

		scriptPath = filepath.Join(currentDir, "hscontrol", "spnetwork", "test", "graph.py")
		_, err = os.Stat(scriptPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("graph.py file not found: %v", err)
		}
	}

	// Launch Python script
	cmd := exec.Command("python3", scriptPath)

	// Set environment variable for API URL
	cmd.Env = append(os.Environ(), fmt.Sprintf("API_URL=http://127.0.0.1:%d", p.apiPort))

	// Launch the process in background mode
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("error launching graph.py: %v", err)
	}

	log.Printf("Graph visualizer launched (PID: %d)", cmd.Process.Pid)

	return nil
}

// launch creates EntityRegistry, Node, certificates, transport, consensus, measurer, grouping and SPNetwork itself
func (p *ScenarioPlayer) launch(nodeID string, bootstrapIDs []string) error {
	// 1) Registry + bootstrap
	reg := common.NewEntityRegistry(common.NewMemoryEntityRegistry())
	for _, b := range bootstrapIDs {
		_, err := reg.Node.StoreEntity(p.servers[b].LocalNode)
		if err != nil {
			return err
		}
	}

	// 2) Node + ports
	node := entities.NewNode()
	node.SetHost("127.0.0.1")
	gossipPort := p.basePort + p.nextPort
	pingPort := gossipPort + 1000
	p.nextPort++
	node.SetGossipPort(uint16(gossipPort))
	node.SetUdpPingPort(uint16(pingPort))
	_, err := reg.Node.StoreEntity(node)
	if err != nil {
		return err
	}

	// 3) Certificate generation for nodeID (files: certDir/nodeID.crt/key)
	certFile, keyFile, err := p.makeCert(nodeID)
	if err != nil {
		return err
	}

	// 4) Transport, gossip, consensus, measurer, grouping
	tc := g.GrpcTransportConfig{
		ListenHost: "127.0.0.1",
		ListenPort: gossipPort,
		EnableTLS:  true, CertFile: certFile, KeyFile: keyFile, CaFile: p.caFile,
	}
	tr, err := g.NewGrpcTransport(reg, node, tc)
	if err != nil {
		return err
	}
	gossip := g.NewGossip(reg, node.GetID(), tr, 100*time.Millisecond)
	cons, err := consensus.NewDeterministicConsensus(gossip, reg, node, 200*time.Millisecond, 10)
	if err != nil {
		return err
	}
	meas, err := measurer.NewMonkeyMeasurer(reg, node, measurer.MonkeyMeasurerConfig{
		MeasureInterval:            5 * time.Second,
		NewValueProbability:        0.01,
		NodeUnavailableProbability: 0.01,
	}, p.blacklist)
	if err != nil {
		return err
	}
	grp, err := grouping.NewDeterministicGrouping(reg, node, cons, 1*time.Second)
	if err != nil {
		return err
	}

	// 5) New SPNetwork and start
	srv, err := spnetwork.NewSPNetworkManaged(reg, node, gossip, cons, meas, grp)
	if err != nil {
		return err
	}
	if err := srv.Start(); err != nil {
		return err
	}

	p.servers[nodeID] = srv
	p.registries[nodeID] = reg

	// If this is the first server and visualization is enabled, launch the API server
	if len(p.servers) == 1 && p.showGraph {
		if err := p.launchAPIServer(reg); err != nil {
			log.Printf("Error launching API server: %v", err)
		}
	}

	return nil
}

// makeCert — simple generation of a self-signed certificate in certDir/nodeID.{crt, key}
func (p *ScenarioPlayer) makeCert(nodeID string) (certPath, keyPath string, _ error) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	// load CA
	caBytes, _ := os.ReadFile(p.caFile)
	block, _ := pem.Decode(caBytes)
	caCert, _ := x509.ParseCertificate(block.Bytes)
	caKeyPEM, _ := os.ReadFile(filepath.Join(filepath.Dir(p.caFile), "ca.key"))
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	caKey, _ := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)

	certBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return "", "", err
	}
	certPath = filepath.Join(p.certDir, nodeID+".crt")
	keyPath = filepath.Join(p.certDir, nodeID+".key")

	// Writing certificate to file
	certOut, err := os.Create(certPath)
	if err != nil {
		return "", "", err
	}
	defer certOut.Close()
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	if err != nil {
		return "", "", err
	}

	// Writing key to file
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return "", "", err
	}
	defer keyOut.Close()
	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}
