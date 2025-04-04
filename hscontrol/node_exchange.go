package hscontrol

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/mux"

	// Import Headscale packages.
	"github.com/juanfont/headscale/hscontrol/db"
	"github.com/juanfont/headscale/hscontrol/types"
	"gorm.io/gorm"
)

// NodeExchangeConfig holds configuration parameters for the node sync service.
type NodeExchangeConfig struct {
	Port          string        // e.g. ":8443"
	CertFile      string        // server certificate file
	KeyFile       string        // server key file
	CACertPath    string        // CA certificate file (used for mutual TLS)
	RemoteNodes   []string      // initial list of remote headscale node URLs (e.g. "https://node1.example.com:8443")
	PollInterval  time.Duration // interval between polling remote nodes
	AdvertiseHost string
}

type RemoteNodeInfo struct {
	Host        string    `json:"host"`
	Port        string    `json:"port"`
	AvailableAt time.Time `json:"availableAt"`
}

// NodeExchange is responsible for both serving local API endpoints
// and polling remote headscale nodes for tailscale node information.
type NodeExchange struct {
	hsdb          *db.HSDatabase
	config        NodeExchangeConfig
	remoteNodes   map[string]RemoteNodeInfo // current list of remote headscale nodes, keyed by advertiseUrl
	remoteNodesMu sync.Mutex                // protects remoteNodes
	ctx           context.Context
	cancel        context.CancelFunc
	server        *http.Server
}

// NewNodeExchange creates a new NodeExchange instance.
func NewNodeExchange(hsdb *db.HSDatabase, config NodeExchangeConfig) *NodeExchange {
	ctx, cancel := context.WithCancel(context.Background())
	// Make a copy of the initial remote nodes.

	remoteNodes := make(map[string]RemoteNodeInfo)
	for _, urlStr := range config.RemoteNodes {
		u, err := url.Parse(urlStr)
		if err != nil {
			log.Printf("Error parsing remote node URL %s: %v", urlStr, err)
			continue
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			host = u.Host
			port = ""
		}
		remoteNodes[urlStr] = RemoteNodeInfo{
			Host:        host,
			Port:        port,
			AvailableAt: time.Now(),
		}
	}
	return &NodeExchange{
		hsdb:        hsdb,
		config:      config,
		remoteNodes: remoteNodes,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// startServer initializes and starts the HTTPS server (with mutual TLS)
// that serves local API endpoints.
func (ns *NodeExchange) startServer() {
	router := mux.NewRouter()
	// Endpoint to return the local tailscale nodes.
	router.HandleFunc("/api/tailscale/nodes", ns.handleGetTailscaleNodes).Methods("GET")
	// Endpoint to return the list of known headscale nodes.
	router.HandleFunc("/api/headscale/nodes", ns.handleGetHeadscaleNodes).Methods("GET")
	router.HandleFunc("/api/headscale/advertise", ns.advertiseHandler).Methods("POST")

	// Load the CA certificate for client verification.
	caCertPool, err := loadCACertPool(ns.config.CACertPath)
	if err != nil {
		log.Fatal().Msgf("Error loading CA certificate: %v", err)
	}

	// Set up TLS configuration for mutual TLS.
	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caCertPool,
	}
	ns.server = &http.Server{
		Addr:      ns.config.Port,
		Handler:   router,
		TLSConfig: tlsConfig,
	}

	log.Printf("Starting HTTPS server on %s", ns.config.Port)
	// Start the server (this call blocks, so run it in a separate goroutine).
	go func() {
		if err := ns.server.ListenAndServeTLS(ns.config.CertFile, ns.config.KeyFile); err != nil && err != http.ErrServerClosed {
			log.Fatal().Msgf("Server error: %v", err)
		}
	}()
}

// handleGetTailscaleNodes handles GET requests to return the local tailscale nodes.
func (ns *NodeExchange) handleGetTailscaleNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := ns.hsdb.ListNodes()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list nodes: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode nodes: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetHeadscaleNodes returns the list of remote headscale node URLs currently known.
func (ns *NodeExchange) handleGetHeadscaleNodes(w http.ResponseWriter, r *http.Request) {
	ns.remoteNodesMu.Lock()
	defer ns.remoteNodesMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ns.remoteNodes); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode remote nodes: %v", err), http.StatusInternalServerError)
		return
	}
}

func (ns *NodeExchange) advertiseHandler(w http.ResponseWriter, r *http.Request) {
	type AdvertiseRequest struct {
		Host string `json:"ip"`
		Port string `json:"port"`
	}
	var req AdvertiseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if req.Host == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid remote address: %v", err), http.StatusInternalServerError)
			return
		}
		req.Host = host
	}
	advertisedURL := fmt.Sprintf("https://%s:%s", req.Host, req.Port)

	ns.remoteNodesMu.Lock()
	defer ns.remoteNodesMu.Unlock()
	ns.remoteNodes[advertisedURL] = RemoteNodeInfo{
		Host:        req.Host,
		Port:        req.Port,
		AvailableAt: time.Now(),
	}
	w.WriteHeader(http.StatusOK)
}

func (ns *NodeExchange) advertiseToRemote(remoteURL string) error {
	advertiseURL := remoteURL + "/api/headscale/advertise"
	payload := map[string]string{
		"host": ns.config.AdvertiseHost,
		"port": ns.config.Port,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client, err := newMutualTLSClient(ns.config.CACertPath, ns.config.CertFile, ns.config.KeyFile)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", advertiseURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	} else {
		ns.remoteNodesMu.Lock()
		if info, ok := ns.remoteNodes[remoteURL]; ok {
			info.AvailableAt = time.Now()
			ns.remoteNodes[remoteURL] = info
		}
		ns.remoteNodesMu.Unlock()
	}
	return nil
}

// pollRemoteNodes periodically connects to each remote headscale node,
// retrieves the list of tailscale nodes and known headscale nodes,
// and updates the local database and polling list accordingly.
func (ns *NodeExchange) pollRemoteNodes() {
	log.Debug().Msgf("pollRemoteNodes")
	// Create an HTTP client with mutual TLS configured.
	client, err := newMutualTLSClient(ns.config.CACertPath, ns.config.CertFile, ns.config.KeyFile)
	if err != nil {
		log.Fatal().Msgf("Failed to create mutual TLS client: %v", err)
	}

	ticker := time.NewTicker(ns.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ns.ctx.Done():
			log.Printf("Stopping remote node polling")
			return
		case <-ticker.C:
			ns.remoteNodesMu.Lock()
			remoteList := make([]string, 0, len(ns.remoteNodes))
			for url, info := range ns.remoteNodes {
				if time.Since(info.AvailableAt) <= time.Hour {
					remoteList = append(remoteList, url)
				}
			}
			ns.remoteNodesMu.Unlock()

			for _, remoteURL := range remoteList {
				// Poll tailscale nodes from the remote headscale node.
				ns.pollRemoteTailscaleNodes(client, remoteURL)
				// Poll known headscale nodes from the remote headscale node.
				ns.pollRemoteHeadscaleNodes(client, remoteURL)
				if err := ns.advertiseToRemote(remoteURL); err != nil {
					log.Printf("Error advertising to remote node %s: %v", remoteURL, err)
				}
			}
		}
	}
}

// pollRemoteTailscaleNodes retrieves tailscale node information from a remote headscale node
// and adds any new nodes to the local database.
func (ns *NodeExchange) pollRemoteTailscaleNodes(client *http.Client, remoteURL string) {
	log.Debug().Msgf("pollRemoteTailscaleNodes: %s", remoteURL)
	url := remoteURL + "/api/tailscale/nodes"
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Error polling %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	var remoteNodes []types.Node
	if err := json.NewDecoder(resp.Body).Decode(&remoteNodes); err != nil {
		log.Printf("Error decoding response from %s: %v", url, err)
		return
	}

	ns.remoteNodesMu.Lock()
	if info, ok := ns.remoteNodes[remoteURL]; ok {
		info.AvailableAt = time.Now()
		ns.remoteNodes[remoteURL] = info
	}
	ns.remoteNodesMu.Unlock()
	// Iterate through the remote tailscale nodes and add any unknown ones.
	for _, rnode := range remoteNodes {
		// Use Hostname as a unique identifier.
		node, err := ns.hsdb.GetNodeByGlobalId(rnode.GlobalId)
		if err != nil {
			// If not found, add the node.
			log.Debug().Msgf("New tailscale node discovered: %s {%s}", rnode.Hostname, rnode.GlobalId)
			if err := ns.hsdb.Write(func(tx *gorm.DB) error {
				return tx.Create(&rnode).Error
			}); err != nil {
				log.Printf("Error inserting node %s: %v", rnode.Hostname, err)
			}
		} else if node.Revision < rnode.Revision {
			log.Debug().Msgf("Node updated: %s {%s}. Revision: %d", rnode.Hostname, rnode.GlobalId, rnode.Revision)
			if err := ns.hsdb.Write(func(tx *gorm.DB) error {
				updateData := map[string]interface{}{
					"Hostname":   rnode.Hostname,
					"NodeKey":    rnode.NodeKey.String(),
					"MachineKey": rnode.MachineKey.String(),
					"DiscoKey":   rnode.DiscoKey.String(),
					"IPv4":       rnode.IPv4.String(),
					"IPv6":       rnode.IPv6.String(),
					"GivenName":  rnode.GivenName,
					"Revision":   rnode.Revision,
				}
				if endpointsBytes, err := json.Marshal(rnode.Endpoints); err != nil {
					log.Printf("Error marshaling endpoints for node %s: %v", rnode.Hostname, err)
					updateData["Endpoints"] = "[]"
				} else {
					updateData["Endpoints"] = string(endpointsBytes)
				}
				return tx.Set("skip_revision_increment", true).Model(&node).Updates(updateData).Error
			}); err != nil {
				log.Printf("Error updating node %s: %v", rnode.Hostname, err)
			}
		}

	}
}

// pollRemoteHeadscaleNodes retrieves the list of known headscale nodes from a remote headscale node
// and adds any new entries to the local remote node list.
func (ns *NodeExchange) pollRemoteHeadscaleNodes(client *http.Client, remoteURL string) {
	url := remoteURL + "/api/headscale/nodes"
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Error polling %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	var remoteHeadscaleNodes map[string]RemoteNodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&remoteHeadscaleNodes); err != nil {
		log.Printf("Error decoding headscale nodes from %s: %v", url, err)
		return
	}
	ns.remoteNodesMu.Lock()
	defer ns.remoteNodesMu.Unlock()
	for url, info := range remoteHeadscaleNodes {
		if existing, ok := ns.remoteNodes[url]; ok {
			if info.AvailableAt.After(existing.AvailableAt) {
				ns.remoteNodes[url] = info
			}
		} else {
			log.Printf("Discovered new remote headscale node: %s", url)
			ns.remoteNodes[url] = info
		}
	}
}

// contains checks if a slice contains a given string.
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// loadCACertPool loads a CA certificate file into an x509.CertPool.
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

// newMutualTLSClient creates an HTTP client configured for mutual TLS.
func newMutualTLSClient(caCertPath, certFile, keyFile string) (*http.Client, error) {
	// Load client certificate.
	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	// Load CA certificate.
	caCertPool, err := loadCACertPool(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA cert pool: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, nil
}

// Start launches both the HTTPS server and the remote polling goroutine.
func (ns *NodeExchange) Start() {
	ns.startServer()
	go ns.pollRemoteNodes()
}

// Stop cancels the context and gracefully shuts down the server.
func (ns *NodeExchange) Stop() error {
	ns.cancel()
	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ns.server.Shutdown(ctxShutdown)
}
