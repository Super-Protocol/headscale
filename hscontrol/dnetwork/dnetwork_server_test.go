package dnetwork

import (
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"gvisor.dev/gvisor/pkg/rand"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDNetworkServerGossipSync creates several instances of DNetworkServer,
// gives them time to exchange gossip, and then checks that all networks are synchronized.
func TestDNetworkServerGossipSync(t *testing.T) {
	// Generate self-signed certificates for testing (server, key, and CA are the same)
	certFile, keyFile, caFile, err := generateTestCerts()
	if err != nil {
		t.Fatalf("failed to generate test certs: %v", err)
	}
	defer os.Remove(certFile)
	defer os.Remove(keyFile)
	defer os.Remove(caFile)

	const numServers = 15
	servers := make([]*DNetworkServer, 0, numServers)
	// Store address (host and port) for each server
	type Addr struct {
		host string
		port uint16
	}
	addresses := make([]Addr, 0, numServers)

	// Select a free port for each instance
	for i := 0; i < numServers; i++ {
		port, err := getFreePort()
		if err != nil {
			t.Fatalf("failed to get free port: %v", err)
		}
		addresses = append(addresses, Addr{host: "127.0.0.1", port: port})
	}

	var doneChan chan struct{}

	// For each instance, create a list of bootstrap nodes (other servers)
	for i, addr := range addresses {
		bootstrapNodes := make([]DNode, 0, numServers-1)
		if i > 0 {
			// Only include the first server in bootstrapNodes for all servers except the first
			bootstrapNodes = append(bootstrapNodes, DNode{
				Host:            addresses[0].host,
				Port:            addresses[0].port,
				LastAvailableAt: time.Now(),
			})
		} else {
			// The first server has an empty bootstrapNodes list
			bootstrapNodes = []DNode{}
		}

		// Server configuration
		cfg := DNetworkServerConfig{
			Port:                   addr.port,
			CertFile:               certFile,
			KeyFile:                keyFile,
			CACertPath:             caFile,
			BootstrapNodes:         bootstrapNodes,
			PollInterval:           1 * time.Second, // frequent polling for testing
			LatencyMeasureInterval: 100 * time.Millisecond,
			GroupingInterval:       130 * time.Millisecond,
			AdvertiseHost:          addr.host,
			GroupingGoals: []GroupConfig{
				{Name: "kubernetes", Size: GroupSize{
					Min: 2,
					Max: 4,
				}, Criteria: []GroupCriteria{
					{
						Name:      M_LATENCY_CLASS,
						Condition: "min",
					},
				}},
			},
		}

		server := NewDNetworkServer(cfg)
		servers = append(servers, server)

		// Start the server in a goroutine
		go func(s *DNetworkServer) {
			// Ignore the error (could add error logging when starting the server)
			_ = s.Start()
		}(server)
	}

	// If export environment variable is set, enable periodic export of each server's state to a .dot file (2 times per second)
	if exportBasePath, ok := os.LookupEnv("HEADSCALE_TEST_DNETWORK_EXPORT_DOT_PATH"); ok {
		// Ensure base directories exist for each server
		for _, addr := range addresses {
			serverDir := filepath.Join(exportBasePath, fmt.Sprintf("%s:%d", addr.host, addr.port))
			if err := os.MkdirAll(serverDir, 0755); err != nil {
				t.Fatalf("failed to create directory %s: %v", serverDir, err)
			}
		}

		// Slice to keep track of frame count for each server
		counters := make([]int, len(servers))

		exportTicker := time.NewTicker(500 * time.Millisecond)
		defer exportTicker.Stop()
		doneChan = make(chan struct{})

		go func() {
			for {
				select {
				case <-exportTicker.C:
					for i, server := range servers {
						counters[i]++ // increment frame counter
						measToExport := []string{getMeasurementNameForGroup("kubernetes"), M_LATENCY_CLASS}
						for _, name := range measToExport {
							exportFile := filepath.Join(exportBasePath, fmt.Sprintf("%s:%d", addresses[i].host, addresses[i].port), fmt.Sprintf("%s.%d.dot", name, counters[i]))
							server.n.g.ExportToDOT(exportFile, name)
						}
					}
				case <-doneChan:
					return
				}
			}
		}()
	}

	// Give time for gossip exchange between nodes
	const syncDuration = 20 * time.Second
	t.Logf("Waiting for gossip synchronization for %v...", syncDuration)
	time.Sleep(syncDuration)

	if doneChan != nil {
		close(doneChan)
	}

	// Gather the list of nodes from the first (reference) server
	referenceNodes := servers[0].n.g.GetNodes()
	if len(referenceNodes) == 0 {
		t.Fatal("Reference server does not contain any nodes")
	}
	referenceMap := make(map[string]struct{})
	for _, node := range referenceNodes {
		key := fmt.Sprintf("%s:%d", node.Host, node.Port)
		referenceMap[key] = struct{}{}
	}

	// Compare all other networks with the reference one
	for idx, srv := range servers[1:] {
		nodes := srv.n.g.GetNodes()
		t.Logf("Server %d contains %d nodes", idx+1, len(nodes))

		// Check that the number of nodes matches
		assert.Equal(t, len(referenceNodes), len(nodes), "Number of nodes in server %d differs from the reference", idx+1)

		// Check that each node from the reference set exists in this server
		for _, node := range nodes {
			key := fmt.Sprintf("%s:%d", node.Host, node.Port)
			_, exists := referenceMap[key]
			assert.True(t, exists, "Server %d contains node %s, which is missing from the reference set", idx+1, key)
		}
	}
	t.Log("All servers are successfully synchronized")
}

// getFreePort finds a free TCP port and returns it.
func getFreePort() (uint16, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port), nil
}

// generateTestCerts generates a self-signed certificate, private key, and CA certificate, writes them to temporary files, and returns their paths.
func generateTestCerts() (certFile, keyFile, caFile string, err error) {
	// Create RSA private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", "", err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(rand.Int63()),
		Subject: pkix.Name{
			CommonName: "127.0.0.1",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// Self-sign the certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return "", "", "", err
	}

	// Write the certificate to a file
	certOut, err := ioutil.TempFile("", "test_cert_*.pem")
	if err != nil {
		return "", "", "", err
	}
	defer certOut.Close()
	if err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return "", "", "", err
	}

	// Write the private key to a file
	keyOut, err := ioutil.TempFile("", "test_key_*.pem")
	if err != nil {
		return "", "", "", err
	}
	defer keyOut.Close()
	if err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		return "", "", "", err
	}

	// Use the same certificate for the CA
	caOut, err := ioutil.TempFile("", "test_ca_*.pem")
	if err != nil {
		return "", "", "", err
	}
	defer caOut.Close()
	if err = pem.Encode(caOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return "", "", "", err
	}

	return certOut.Name(), keyOut.Name(), caOut.Name(), nil
}
