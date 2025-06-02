package spnetwork

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSPNetworkSynchronization проверяет, что несколько SPNetwork серверов
// корректно синхронизируют информацию о узлах сети между собой
func TestSPNetworkSynchronization(t *testing.T) {
	// Количество серверов для теста (легко меняется)
	numServers := 50

	// Создаем временный каталог для сертификатов
	tempDir, err := os.MkdirTemp("", "spnetwork-test")
	if err != nil {
		t.Fatalf("Не удалось создать временный каталог: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Генерируем тестовые сертификаты
	caFile, certFiles, keyFiles, err := generateTestCertificates(tempDir, numServers)
	if err != nil {
		t.Fatalf("Не удалось сгенерировать тестовые сертификаты: %v", err)
	}

	// Создаем и запускаем серверы SPNetwork
	servers := make([]*SPNetwork, numServers)
	nodes := make([]*entities.Node, numServers)

	// Инициализируем узлы
	basePort := 9000
	for i := 0; i < numServers; i++ {
		node := entities.NewNode(fmt.Sprintf("node-%d", i))
		node.SetHost("127.0.0.1")
		node.SetGossipPort(uint16(basePort + i))
		nodes[i] = node
	}

	// Запускаем первый сервер (bootstrap сервер)
	servers[0], err = NewSPNetwork(nodes[0], nil, PkiConfig{
		CertFile: certFiles[0],
		KeyFile:  keyFiles[0],
		CaFile:   caFile,
	})
	if err != nil {
		t.Fatalf("Не удалось создать bootstrap-сервер: %v", err)
	}
	err = servers[0].Start()
	if err != nil {
		t.Fatalf("Не удалось запустить bootstrap-сервер: %v", err)
	}
	defer servers[0].Stop()

	// Запускаем остальные серверы, использующие первый сервер как bootstrap
	for i := 1; i < numServers; i++ {
		// В качестве bootstrap узла используем первый сервер
		bootstrapNodes := []*entities.Node{nodes[0]}
		servers[i], err = NewSPNetwork(nodes[i], bootstrapNodes, PkiConfig{
			CertFile: certFiles[i],
			KeyFile:  keyFiles[i],
			CaFile:   caFile,
		})
		if err != nil {
			t.Fatalf("Не удалось создать сервер %d: %v", i, err)
		}
		err = servers[i].Start()
		if err != nil {
			t.Fatalf("Не удалось запустить сервер %d: %v", i, err)
		}
		defer servers[i].Stop()
	}

	// Ожидаем 10 секунд для синхронизации
	t.Log("Ожидаем 10 секунд для синхронизации серверов...")
	time.Sleep(10 * time.Second)

	// Проверяем, что все серверы имеют одинаковый список узлов
	for i := 0; i < numServers; i++ {
		for j := i + 1; j < numServers; j++ {
			nodes1, _ := servers[i].nodeRegistry.GetAllEntities()
			nodes2, _ := servers[j].nodeRegistry.GetAllEntities()
			if len(nodes1) != len(nodes2) {
				t.Errorf("Серверы %d и %d имеют разное количество узлов: %d vs %d",
					i, j, len(nodes1), len(nodes2))
			}
		}
	}

	// Проверяем, что каждый сервер знает о всех узлах
	for i := 0; i < numServers; i++ {
		nodes, _ := servers[i].nodeRegistry.GetAllEntities()
		if len(nodes) != numServers {
			t.Errorf("Сервер %d знает только о %d узлах из %d",
				i, len(nodes), numServers)
		}
	}
}

// generateTestCertificates генерирует самоподписанный корневой сертификат и набор
// сертификатов для указанного количества серверов
func generateTestCertificates(dir string, numCerts int) (caFile string, certFiles []string, keyFiles []string, err error) {
	// Генерируем корневой CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", nil, nil, fmt.Errorf("не удалось сгенерировать CA ключ: %w", err)
	}

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "SPNetwork Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return "", nil, nil, fmt.Errorf("не удалось создать CA сертификат: %w", err)
	}

	// Сохраняем CA сертификат
	caFile = filepath.Join(dir, "ca.crt")
	caOut, err := os.Create(caFile)
	if err != nil {
		return "", nil, nil, fmt.Errorf("не удалось создать файл CA: %w", err)
	}
	err = pem.Encode(caOut, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	if err != nil {
		return "", nil, nil, fmt.Errorf("не удалось записать CA сертификат: %w", err)
	}
	caOut.Close()

	certFiles = make([]string, numCerts)
	keyFiles = make([]string, numCerts)

	// Генерируем сертификаты для серверов
	for i := 0; i < numCerts; i++ {
		// Генерируем ключ
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось сгенерировать ключ для сервера %d: %w", i, err)
		}

		// Создаем шаблон сертификата
		template := x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			Subject: pkix.Name{
				CommonName: fmt.Sprintf("SPNetwork Server %d", i),
			},
			DNSNames:    []string{"localhost"},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:   time.Now(),
			NotAfter:    time.Now().Add(24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}

		// Подписываем сертификат с помощью CA
		certBytes, err := x509.CreateCertificate(rand.Reader, &template, &caTemplate, &key.PublicKey, caKey)
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось создать сертификат для сервера %d: %w", i, err)
		}

		// Сохраняем сертификат
		certFiles[i] = filepath.Join(dir, fmt.Sprintf("server-%d.crt", i))
		certOut, err := os.Create(certFiles[i])
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось создать файл сертификата для сервера %d: %w", i, err)
		}
		err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось записать сертификат для сервера %d: %w", i, err)
		}
		certOut.Close()

		// Сохраняем ключ
		keyFiles[i] = filepath.Join(dir, fmt.Sprintf("server-%d.key", i))
		keyOut, err := os.Create(keyFiles[i])
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось создать файл ключа для сервера %d: %w", i, err)
		}
		err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		if err != nil {
			return "", nil, nil, fmt.Errorf("не удалось записать ключ для сервера %d: %w", i, err)
		}
		keyOut.Close()
	}

	return caFile, certFiles, keyFiles, nil
}
