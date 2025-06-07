package spnetwork

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/api"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/juanfont/headscale/hscontrol/spnetwork/consensus"
	"github.com/juanfont/headscale/hscontrol/spnetwork/grouping"
	"github.com/juanfont/headscale/hscontrol/spnetwork/measurer"
	g "github.com/juanfont/headscale/hscontrol/spnetwork/syncer/gossip"
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
	numServers := 7

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
		node := entities.NewNode()
		node.SetHost("127.0.0.1")
		node.SetGossipPort(uint16(basePort + i))
		node.SetUdpPingPort(uint16(basePort + i + 1000))
		nodes[i] = node
	}

	// Сортируем узлы по ID в обратном порядке
	sortedIndices := make([]int, numServers)
	for i := 0; i < numServers; i++ {
		sortedIndices[i] = i
	}

	// Сортировка по ID в обратном порядке
	for i := 0; i < numServers-1; i++ {
		for j := i + 1; j < numServers; j++ {
			if nodes[sortedIndices[i]].ID < nodes[sortedIndices[j]].ID {
				sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
			}
		}
	}

	t.Logf("Порядок запуска серверов (в обратном порядке по ID):")
	for i, idx := range sortedIndices {
		t.Logf("  %d. Сервер #%d с ID: %s", i+1, idx, nodes[idx].ID)
	}

	bootstrapNodes := []*entities.Node{nodes[sortedIndices[0]]}

	var server *api.Server

	// Создаем серверы и запускаем только первую половину сначала
	half := numServers / 2
	if numServers%2 != 0 {
		half++ // Округляем вверх для нечетного числа серверов
	}
	blackList := make(map[string]bool)

	// Подготовка всех серверов
	for x, i := range sortedIndices {
		registry := common.NewEntityRegistry(common.NewMemoryEntityRegistry())
		for _, node := range bootstrapNodes {
			_, err := registry.Node.StoreEntity(node)
			if err != nil {
				return
			}
		}
		_, err := registry.Node.StoreEntity(nodes[i])
		if err != nil {
			return
		}

		if x == 0 {
			consensusGroupGoal := entities.NewGroupGoalWithTimeout(2, 5, 10)
			consensusGroupGoal.AddDimensionCriterion(entities.DimensionCriterion{
				Type:      entities.LatencyClass,
				Condition: entities.ConditionMin,
				Values:    make([]float64, 0),
			})
			_, err := registry.GroupGoal.StoreEntity(consensusGroupGoal)
			if err != nil {
				return
			}
			server = api.NewServer(registry)
			go func() {
				err = server.Start("127.0.0.1:8911")
				if err != nil {
					return
				}
			}()
		}

		localNode := nodes[i]
		pkiConfig := PkiConfig{
			CertFile: certFiles[i],
			KeyFile:  keyFiles[i],
			CaFile:   caFile,
		}
		host, _ := localNode.GetHost()
		port, _ := localNode.GetGossipPort()
		transportConfig := g.GrpcTransportConfig{
			ListenHost: host,
			ListenPort: int(port),
			EnableTLS:  true,
			CertFile:   pkiConfig.CertFile,
			KeyFile:    pkiConfig.KeyFile,
			CaFile:     pkiConfig.CaFile,
		}
		syncerTransport, err := g.NewGrpcTransport(registry, localNode, transportConfig)
		if err != nil {
			return
		}
		s := g.NewGossip(registry, localNode.ID, syncerTransport, time.Duration(100)*time.Millisecond)

		c, err := consensus.NewDeterministicConsensus(s, registry, localNode, time.Duration(200)*time.Millisecond, 10)

		if err != nil {
			return
		}

		//udpPingPort, _ := localNode.GetUdpPingPort()
		//m, err := measurer.NewUDPPingMeasurerWithDefaults(registry, localNode, host, int(udpPingPort))
		m, err := measurer.NewMonkeyMeasurer(registry, localNode, measurer.MonkeyMeasurerConfig{
			MeasureInterval:            time.Duration(5) * time.Second,
			NewValueProbability:        0.01,
			NodeUnavailableProbability: 0.01,
		}, blackList)
		if err != nil {
			return
		}

		g, err := grouping.NewDeterministicGrouping(registry, localNode, c, time.Duration(1)*time.Second)
		if err != nil {
			return
		}

		servers[i], err = NewSPNetworkManaged(registry, nodes[i], s, c, m, g)
		if err != nil {
			t.Fatalf("Не удалось создать сервер %d: %v", i, err)
		}
	}

	// Запускаем первую половину серверов
	t.Log("Запускаем первую половину серверов...")
	for i := 0; i < half; i++ {
		idx := sortedIndices[i]
		t.Logf("Запуск сервера #%d с ID: %s", idx, nodes[idx].ID)
		err = servers[idx].Start()
		if err != nil {
			t.Fatalf("Не удалось запустить сервер %d: %v", idx, err)
		}
		defer servers[idx].Stop()
	}

	// Ожидаем 60 секунд после запуска первой половины
	t.Log("Ожидаем 60 секунд после запуска первой половины серверов...")
	time.Sleep(60 * time.Second)

	// Запускаем вторую половину серверов
	t.Log("Запускаем вторую половину серверов...")
	for i := half; i < numServers; i++ {
		idx := sortedIndices[i]
		t.Logf("Запуск сервера #%d с ID: %s", idx, nodes[idx].ID)
		err = servers[idx].Start()
		if err != nil {
			t.Fatalf("Не удалось запустить сервер %d: %v", idx, err)
		}
		defer servers[idx].Stop()
	}

	// Ожидаем 60 секунд для синхронизации всех серверов
	t.Log("Ожидаем 60 секунд для синхронизации всех серверов...")
	time.Sleep(60 * time.Second)

	t.Log("Добавляем сервера из второй половины в черный список (делаем недоступными)")
	for i := half; i < numServers; i++ {
		idx := sortedIndices[i]
		blackList[nodes[idx].ID] = true
	}

	// Ожидаем 60 секунд для синхронизации всех серверов
	t.Log("Ожидаем 600 секунд для синхронизации всех серверов...")
	time.Sleep(600 * time.Second)

	// Останавливаем сначала Measurer и Grouping для завершения обработки данных
	t.Log("Останавливаем Measurer и Grouping для финализации данных...")
	for i := 0; i < numServers; i++ {
		err := servers[i].Measurer.Stop()
		if err != nil {
			t.Fatalf("Не удалось остановить Measurer на сервере %d: %v", i, err)
		}

		//err = servers[i].Grouping.Stop()
		//if err != nil {
		//	t.Fatalf("Не удалось остановить Grouping на сервере %d: %v", i, err)
		//}

		err = servers[i].Consensus.Stop()
		if err != nil {
			t.Fatalf("Не удалось остановить Consensus на сервере %d: %v", i, err)
		}
	}

	// Ожидаем дополнительные 10 секунд для завершения синхронизации после остановки измерений
	t.Log("Ожидаем еще 10 секунд для финальной синхронизации данных...")
	time.Sleep(10 * time.Second)

	if server != nil {
		err := server.Stop()
		if err != nil {
			return
		}
	}

	// Проверяем, что все серверы имеют одинаковый список сущностей всех типов
	for i := 0; i < numServers; i++ {
		for j := i + 1; j < numServers; j++ {
			// Получаем все сущности из registry для обоих серверов
			entities1 := servers[i].registry.BaseRegistry.GetAllEntities()
			entities2 := servers[j].registry.BaseRegistry.GetAllEntities()

			// Проверяем, что оба сервера имеют одинаковое количество типов сущностей
			if len(entities1) != len(entities2) {
				t.Errorf("Серверы %d и %d имеют разное количество типов сущностей: %d vs %d",
					i, j, len(entities1), len(entities2))
			}

			// Проверяем сущности каждого типа
			for entityType, entitiesOfType1 := range entities1 {
				entitiesOfType2, exists := entities2[entityType]
				if !exists {
					t.Errorf("Сервер %d не содержит сущностей типа %s, которые есть на сервере %d",
						j, entityType, i)
					continue
				}

				// Проверяем количество сущностей каждого типа
				if len(entitiesOfType1) != len(entitiesOfType2) {
					t.Errorf("Серверы %d и %d имеют разное количество сущностей типа %s: %d vs %d",
						i, j, entityType, len(entitiesOfType1), len(entitiesOfType2))
				}

				// Создаем карту для сущностей первого сервера с ключом ID
				entityMap1 := make(map[string]common.Entity)
				for _, entity := range entitiesOfType1 {
					entityMap1[entity.GetID()] = entity
				}

				// Проверяем каждую сущность второго сервера
				for _, entity2 := range entitiesOfType2 {
					entity1, exists := entityMap1[entity2.GetID()]
					if !exists {
						t.Errorf("Сервер %d содержит сущность типа %s с ID %s, отсутствующую на сервере %d",
							j, entityType, entity2.GetID(), i)
						continue
					}

					// Проверяем, что хеши сущностей совпадают
					if bytes.Compare(entity1.GetHash(), entity2.GetHash()) != 0 {
						t.Errorf("Сущность типа %s с ID %s имеет разные хеши на серверах %d и %d",
							entityType, entity2.GetID(), i, j)
					}
				}
			}
		}
	}

	// Проверяем, что каждый сервер знает о всех узлах
	for i := 0; i < numServers; i++ {
		// Получаем узлы через типизированный registry для проверки конкретно узлов
		nodes, _ := servers[i].registry.Node.GetAllEntities()
		if len(nodes) != numServers {
			t.Errorf("Сервер %d знает только о %d узлах из %d",
				i, len(nodes), numServers)
		}
	}

	// Вывод всех сущностей по типам, если тест успешно прошел
	if !t.Failed() {
		t.Log("Тест успешно пройден. Вывод всех сущностей по типам:")

		// Используем первый сервер для вывода информации, так как все серверы синхронизированы
		allEntities := servers[0].registry.BaseRegistry.GetAllEntities()

		// Выводим отдельно список узлов, отсортированных по ID в обратном порядке
		t.Log("Список узлов (отсортирован по ID в обратном порядке):")
		nodes, _ := servers[0].registry.Node.GetAllEntities()
		sortedNodes := make([]*entities.Node, len(nodes))
		for i, node := range nodes {
			sortedNodes[i] = node
		}
		// Сортировка узлов по ID в обратном порядке
		for i := 0; i < len(sortedNodes)-1; i++ {
			for j := i + 1; j < len(sortedNodes); j++ {
				if sortedNodes[i].ID < sortedNodes[j].ID {
					sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
				}
			}
		}
		for i, node := range sortedNodes {
			host, _ := node.GetHost()
			port, _ := node.GetGossipPort()
			t.Logf("  %d. ID: %s, Хост: %s, Порт: %d", i+1, node.ID, host, port)
		}

		for entityType, entities := range allEntities {
			t.Logf("  Тип сущности: %s, Количество: %d", entityType, len(entities))
			for _, entity := range entities {
				t.Logf("    ID: %s", entity.GetID())
			}
		}

		// Отдельно выводим содержимое сущностей Group
		t.Log("Детальная информация о группах:")
		groups, err := servers[0].registry.Group.GetAllEntities()
		if err != nil {
			t.Errorf("Ошибка при получении групп: %v", err)
		} else {
			for _, group := range groups {
				if group.IsDeleted() {
					t.Logf("  Группа [УДАЛЕНА] ID: %s", group.GetID())
					continue
				}

				t.Logf("  Группа ID: %s", group.GetID())
				t.Logf("    Цель: %s", group.GetGoal())
				t.Logf("    Версия: %d", group.GetVersion())
				t.Logf("    Дата создания: %s", time.Unix(group.GetCreationDate(), 0).Format(time.RFC3339))

				participants := group.GetParticipants()
				t.Logf("    Участники (%d):", len(participants))
				for _, participant := range participants {
					joinDate := time.Unix(participant.JoinDateUnix, 0).Format(time.RFC3339)
					t.Logf("      ID: %s, Присоединился: %s", participant.ID, joinDate)
				}
			}
		}

		// Выводим детальную информацию о Vote
		t.Log("Детальная информация о голосах (Vote):")
		votes, err := servers[0].registry.Vote.GetAllEntities()
		if err != nil {
			t.Errorf("Ошибка при получении голосов: %v", err)
		} else {
			for _, vote := range votes {
				if vote.IsDeleted() {
					t.Logf("  Голос [УДАЛЕН] ID: %s", vote.GetID())
					continue
				}

				t.Logf("  Голос ID: %s", vote.GetID())
				t.Logf("    Тип: %s", vote.GetKind())
				t.Logf("    Цель: %s", vote.GetTarget())
				t.Logf("    Значение: %d", vote.GetValue())
				t.Logf("    ID запроса: %s", vote.GetRequestID())
				t.Logf("    Голосующий: %s", vote.GetVoter())
				t.Logf("    Версия: %d", vote.GetVersion())
				t.Logf("    Дата создания: %s", time.Unix(vote.GetDateUnix(), 0).Format(time.RFC3339))
			}
		}

		// Выводим детальную информацию о VoteRequest
		t.Log("Детальная информация о запросах на голосование (VoteRequest):")
		voteRequests, err := servers[0].registry.VoteRequest.GetAllEntities()
		if err != nil {
			t.Errorf("Ошибка при получении запросов на голосование: %v", err)
		} else {
			for _, vr := range voteRequests {
				if vr.IsDeleted() {
					t.Logf("  Запрос на голосование [УДАЛЕН] ID: %s", vr.GetID())
					continue
				}

				t.Logf("  Запрос на голосование ID: %s", vr.GetID())
				t.Logf("    Тип: %s", vr.GetKind())
				t.Logf("    Цель: %s", vr.GetTarget())
				t.Logf("    Таймаут: %d сек", vr.GetTimeoutSecs())
				t.Logf("    Версия: %d", vr.GetVersion())
				t.Logf("    Дата создания: %s", time.Unix(vr.GetDateUnix(), 0).Format(time.RFC3339))
			}
		}

		// Выводим детальную информацию о LeadershipResign
		t.Log("Детальная информация об отказах от лидерства (LeadershipResign):")
		resigns, err := servers[0].registry.LeadershipResign.GetAllEntities()
		if err != nil {
			t.Errorf("Ошибка при получении отказов от лидерства: %v", err)
		} else {
			for _, resign := range resigns {
				if resign.IsDeleted() {
					t.Logf("  Отказ от лидерства [УДАЛЕН] ID: %s", resign.GetID())
					continue
				}

				t.Logf("  Отказ от лидерства ID: %s", resign.GetID())
				t.Logf("    Владелец: %s", resign.GetOwner())
				t.Logf("    ID запроса на голосование: %s", resign.GetVoteRequestID())
				t.Logf("    Версия: %d", resign.GetVersion())
				t.Logf("    Дата создания: %s", time.Unix(resign.GetDateUnix(), 0).Format(time.RFC3339))
			}
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
