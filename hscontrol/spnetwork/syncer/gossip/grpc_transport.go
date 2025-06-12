package gossip

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type GrpcTransportConfig TransportConfig

// SyncStat stores information about results of a single synchronization
type SyncStat struct {
	Timestamp       time.Time // Synchronization time
	RemoteNodeID    string    // Remote node ID
	TotalEntities   int       // Total number of entities
	DiffEntities    int       // Number of different entities
	SyncCoefficient float64   // Synchronization coefficient for this operation
}

type GrpcTransport struct {
	entityRegistry  *common.EntityRegistry
	localNode       *entities.Node
	running         bool
	mu              sync.Mutex
	grpcServer      *grpc.Server
	clients         map[string]*grpc.ClientConn
	clientsMu       sync.RWMutex
	tlsConfig       *tls.Config
	clientTLSConfig *tls.Config
	listenAddr      string
	syncStats       []SyncStat   // История синхронизаций
	syncStatsMu     sync.RWMutex // Мьютекс для доступа к истории синхронизаций
}

// GossipServiceServer represents a gRPC server for message processing
type GossipServiceServer struct {
	pb.UnimplementedGossipServiceServer
	transport *GrpcTransport
}

const (
	// depthBits defines how many bits we take from MD5(ID) for bucket indexing.
	// depthBits = 8 → 256 buckets; depthBits = 10 → 1024 buckets.
	depthBits    = 8
	totalBuckets = 1 << depthBits
)

const minSyncIterations = 100

// getBucketIndexFromID returns the bucket number [0..totalBuckets-1] for a specific ID.
// We take the first depthBits bits from MD5(ID).
func getBucketIndexFromID(id []byte) int {
	sum := md5.Sum(id) // [16]byte
	// depthBits <= 8: take the highest depthBits bits from the first byte
	if depthBits <= 8 {
		// shiftRight = 8 - depthBits, to get only the needed number of bits
		return int(sum[0]) >> (8 - depthBits)
	}
	// If depthBits > 8, we need to take, for example, sum[0] as the highest 8 bits,
	// and add (depthBits-8) bits from sum[1]. But in this example depthBits=8.
	return int(sum[0])
}

// computeBucketHash creates an MD5 hash from an ordered list of (ID||version),
// so that any change in version or content of an entity changes the hash.
// entities are already all inside one bucket.
func computeBucketHash(entities []common.Entity) []byte {
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].GetID() < entities[j].GetID()
	})

	h := md5.New()
	for _, e := range entities {
		id := []byte(e.GetID())
		versionBytes := []byte(fmt.Sprintf("%v", e.GetVersion()))
		h.Write(id)
		h.Write(versionBytes)
	}
	return h.Sum(nil) // 16 bytes MD5
}

func NewGrpcTransport(entityRegistry *common.EntityRegistry, localNode *entities.Node, config GrpcTransportConfig) (*GrpcTransport, error) {
	log.Debug().
		Str("node_id", localNode.GetID()).
		Str("listen_host", config.ListenHost).
		Int("listen_port", config.ListenPort).
		Str("ca_file", config.CaFile).
		Str("cert_file", config.CertFile).
		Str("key_file", config.KeyFile).
		Msg("creating gRPC transport")

	// Loading the root CA certificate
	rootCAs := x509.NewCertPool()
	caCert, err := os.ReadFile(config.CaFile)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", localNode.GetID()).
			Str("ca_file", config.CaFile).
			Msg("failed to read root CA certificate")
		return nil, fmt.Errorf("failed to read root CA certificate: %v", err)
	}

	if !rootCAs.AppendCertsFromPEM(caCert) {
		log.Error().
			Str("node_id", localNode.GetID()).
			Str("ca_file", config.CaFile).
			Msg("failed to add root CA certificate")
		return nil, fmt.Errorf("failed to add root CA certificate")
	}

	// Loading server certificate
	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", localNode.GetID()).
			Str("cert_file", config.CertFile).
			Str("key_file", config.KeyFile).
			Msg("failed to load server certificate")
		return nil, fmt.Errorf("failed to load server certificate: %v", err)
	}

	// Server TLS configuration with mandatory client authentication
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    rootCAs,
		RootCAs:      rootCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	// Client TLS configuration with hostname verification disabled
	clientTLSConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            rootCAs,
		InsecureSkipVerify: true, // Disable common name verification in server certificate
		MinVersion:         tls.VersionTLS12,
	}

	listenAddr := fmt.Sprintf("%s:%d", config.ListenHost, config.ListenPort)
	transport := &GrpcTransport{
		localNode:       localNode,
		running:         false,
		clients:         make(map[string]*grpc.ClientConn),
		tlsConfig:       tlsConfig,
		clientTLSConfig: clientTLSConfig,
		listenAddr:      listenAddr,
		entityRegistry:  entityRegistry,
		syncStats:       make([]SyncStat, 0),
	}

	log.Info().
		Str("node_id", localNode.GetID()).
		Str("listen_addr", listenAddr).
		Msg("gRPC transport created")
	return transport, nil
}

// GetData now compares the pre-calculated client MD5 bucket hashes (BucketHashes)
// and returns only data (full Serialize()) related to those buckets where hashes !=.
func (s *GossipServiceServer) GetData(ctx context.Context, req *pb.GetDataRequest) (*pb.GetDataResponse, error) {

	log.Debug().
		Str("local_node_id", s.transport.localNode.GetID()).
		Str("req_node_id", req.NodeId).
		Msg("Processing GetData request")

	// Получаем все сущности через EntityRegistry
	entitiesByType := make(map[string][]common.Entity)

	// Добавляем ноды
	nodes, err := s.transport.entityRegistry.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting nodes")
	} else {
		nodeEntities := make([]common.Entity, 0, len(nodes))
		for _, node := range nodes {
			nodeEntities = append(nodeEntities, node)
		}
		entitiesByType[common.NodeEntityType] = nodeEntities
	}

	// Добавляем измерения
	measurements, err := s.transport.entityRegistry.Measurement.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting measurements")
	} else {
		measurementEntities := make([]common.Entity, 0, len(measurements))
		for _, measurement := range measurements {
			measurementEntities = append(measurementEntities, measurement)
		}
		entitiesByType[common.MeasurementEntityType] = measurementEntities
	}

	// Добавляем группы
	groups, err := s.transport.entityRegistry.Group.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting groups")
	} else {
		groupEntities := make([]common.Entity, 0, len(groups))
		for _, group := range groups {
			groupEntities = append(groupEntities, group)
		}
		entitiesByType[common.GroupEntityType] = groupEntities
	}

	// Добавляем цели групп
	groupGoals, err := s.transport.entityRegistry.GroupGoal.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting group goals")
	} else {
		groupGoalEntities := make([]common.Entity, 0, len(groupGoals))
		for _, groupGoal := range groupGoals {
			groupGoalEntities = append(groupGoalEntities, groupGoal)
		}
		entitiesByType[common.GroupGoalEntityType] = groupGoalEntities
	}

	// Добавляем голоса
	votes, err := s.transport.entityRegistry.Vote.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting votes")
	} else {
		voteEntities := make([]common.Entity, 0, len(votes))
		for _, vote := range votes {
			voteEntities = append(voteEntities, vote)
		}
		entitiesByType[common.VoteEntityType] = voteEntities
	}

	// Добавляем запросы на голосование
	voteRequests, err := s.transport.entityRegistry.VoteRequest.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting vote requests")
	} else {
		voteRequestEntities := make([]common.Entity, 0, len(voteRequests))
		for _, voteRequest := range voteRequests {
			voteRequestEntities = append(voteRequestEntities, voteRequest)
		}
		entitiesByType[common.VoteRequestEntityType] = voteRequestEntities
	}

	// Добавляем отказы от лидерства
	leadershipResigns, err := s.transport.entityRegistry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("error getting leadership resigns")
	} else {
		leadershipResignEntities := make([]common.Entity, 0, len(leadershipResigns))
		for _, leadershipResign := range leadershipResigns {
			leadershipResignEntities = append(leadershipResignEntities, leadershipResign)
		}
		entitiesByType[common.LeadershipResignEntityType] = leadershipResignEntities
	}

	nodeEntitiesForLog := make([]entities.Node, 0, len(entitiesByType[common.NodeEntityType]))
	for _, entity := range entitiesByType[common.NodeEntityType] {
		if node, ok := entity.(*entities.Node); ok {
			nodeEntitiesForLog = append(nodeEntitiesForLog, *node)
		}
	}

	requestingNode, err := entities.NodeFromProtoBytes(req.NodeData)

	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("can't deserialize node data")
		return nil, err
	} else {
		_, err := s.transport.entityRegistry.Node.StoreEntity(requestingNode)
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", s.transport.localNode.GetID()).
				Msg("can't store requesting node")
			return nil, err
		}
	}

	// 1) Построим у себя "свои" bucket'ы: map[entityType] → map[bucketIdx] → []Entity
	typeBucketMap := make(map[string]map[int][]common.Entity)
	for entityType, entities := range entitiesByType {
		buckets := make(map[int][]common.Entity, totalBuckets)
		for _, entity := range entities {
			id := entity.GetID()
			bucketIdx := getBucketIndexFromID([]byte(id))
			buckets[bucketIdx] = append(buckets[bucketIdx], entity)
		}
		typeBucketMap[entityType] = buckets
	}

	// 2) Пройдём по запросу. Ключ в запросе – "entityType#bucketIdx", значение – клиентский MD5-хэш.
	respData := make(map[string]*pb.BytesArray)

	for compositeKey, clientHash := range req.BucketHashes {
		// compositeKey = "entityType#bucketIdx"
		parts := strings.SplitN(compositeKey, "#", 2)
		if len(parts) != 2 {
			continue
		}
		entityType := parts[0]
		bucketIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		// 3) Найдём в своём typeBucketMap соответствующий slice
		buckets, exists := typeBucketMap[entityType]
		var serverHash []byte
		if !exists {
			// Если у нас вообще нет такого entityType → считаем корзину пустой
			serverHash = md5.New().Sum(nil)
		} else {
			ents := buckets[bucketIdx]
			if len(ents) == 0 {
				serverHash = md5.New().Sum(nil)
			} else {
				serverHash = computeBucketHash(ents)
			}
		}

		// 4) Сравниваем: если !=, то возвращаем все сущности из этого bucket'a
		if !bytes.Equal(clientHash, serverHash) {
			baos := &pb.BytesArray{Items: make([][]byte, 0)}

			if exists {
				for _, entity := range typeBucketMap[entityType][bucketIdx] {
					serialized, err := entity.Serialize()
					if err != nil {
						log.Error().
							Err(err).
							Str("node_id", s.transport.localNode.GetID()).
							Str("req_node_id", req.NodeId).
							Str("entity_type", entityType).
							Int("bucket", bucketIdx).
							Msg("failed to serialize entity")
						continue
					}
					baos.Items = append(baos.Items, serialized)

				}
			}
			// Если exists==false, то ничего не кладём (BytesArray.Items останется пустым).
			respData[compositeKey] = baos
		}
		// Если хэши совпали, пропускаем — клиенту ничего не нужно отдать.
	}

	if len(respData) == 0 {
		log.Debug().
			Str("local_node_id", s.transport.localNode.GetID()).
			Str("req_node_id", req.NodeId).
			Msgf("No data to respond - everything is up to date.")
	} else {
		log.Debug().
			Str("local_node_id", s.transport.localNode.GetID()).
			Str("req_node_id", req.NodeId).
			Msgf("Responding with data: %v", respData)
	}
	return &pb.GetDataResponse{Data: respData}, nil
}

func (t *GrpcTransport) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.running {
		log.Warn().Str("node_id", t.localNode.GetID()).Msg("transport already running")
		return fmt.Errorf("transport already running")
	}

	log.Info().Str("node_id", t.localNode.GetID()).Str("listen_addr", t.listenAddr).Msg("starting gRPC transport")
	creds := credentials.NewTLS(t.tlsConfig)
	t.grpcServer = grpc.NewServer(grpc.Creds(creds))

	server := &GossipServiceServer{transport: t}
	pb.RegisterGossipServiceServer(t.grpcServer, server)

	lis, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		log.Error().Err(err).Str("listen_addr", t.listenAddr).Msg("failed to start listening")
		return fmt.Errorf("failed to start listening on %s: %v", t.listenAddr, err)
	}

	go func() {
		log.Debug().Str("node_id", t.localNode.GetID()).Str("listen_addr", t.listenAddr).Msg("gRPC server started")
		err := t.grpcServer.Serve(lis)
		if err != nil && err != grpc.ErrServerStopped {
			log.Error().Err(err).Str("node_id", t.localNode.GetID()).Msg("gRPC server error")
		}
	}()

	t.running = true
	log.Info().
		Str("node_id", t.localNode.GetID()).
		Str("listen_addr", t.listenAddr).
		Msg("gRPC transport started")
	return nil
}

func (t *GrpcTransport) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running {
		log.Warn().Str("node_id", t.localNode.GetID()).Msg("transport already stopped")
		return fmt.Errorf("transport already stopped")
	}

	log.Info().Str("node_id", t.localNode.GetID()).Msg("stopping gRPC transport")
	if t.grpcServer != nil {
		log.Debug().Str("node_id", t.localNode.GetID()).Msg("stopping gRPC server")
		t.grpcServer.GracefulStop()
	}

	t.clientsMu.Lock()
	log.Debug().
		Str("node_id", t.localNode.GetID()).
		Int("connections_count", len(t.clients)).
		Msg("closing client connections")

	for nodeID, conn := range t.clients {
		if err := conn.Close(); err != nil {
			log.Error().
				Err(err).
				Str("node_id", t.localNode.GetID()).
				Str("target_node", nodeID).
				Msg("error closing connection")
			t.clientsMu.Unlock()
			return err
		}
		log.Debug().
			Str("node_id", t.localNode.GetID()).
			Str("target_node", nodeID).
			Msg("connection closed")
	}
	t.clients = make(map[string]*grpc.ClientConn)
	t.clientsMu.Unlock()

	t.running = false
	log.Info().Str("node_id", t.localNode.GetID()).Msg("gRPC transport stopped")
	return nil
}

// GetSyncCoef returns a synchronization coefficient from 0 to 1, where:
// 0 - the node is completely out of sync with others
// 1 - the node is fully synchronized
// The coefficient is calculated as the average value over the last maxSyncStats synchronizations.
// If the number of synchronizations is less than minSyncStats, 0 is returned.
func (t *GrpcTransport) GetSyncCoef() float32 {
	t.syncStatsMu.RLock()
	defer t.syncStatsMu.RUnlock()

	minSyncStats := minSyncIterations

	// If the number of synchronizations is less than minSyncStats, return 0
	if len(t.syncStats) < minSyncStats {
		log.Debug().
			Str("node_id", t.localNode.GetID()).
			Int("current_syncs", len(t.syncStats)).
			Int("required_syncs", minSyncStats).
			Msg("Not enough synchronization data to calculate coefficient")
		return 0.0
	}

	// Calculate the average value for the last N synchronizations
	startIdx := 0
	if len(t.syncStats) > minSyncStats {
		startIdx = len(t.syncStats) - minSyncStats
	}

	totalCoef := 0.0
	for i := startIdx; i < len(t.syncStats); i++ {
		totalCoef += t.syncStats[i].SyncCoefficient
	}

	avgCoef := float32(totalCoef) / float32(len(t.syncStats)-startIdx)

	log.Debug().
		Str("node_id", t.localNode.GetID()).
		Int("stats_count", len(t.syncStats)-startIdx).
		Float32("sync_coef", avgCoef).
		Msg("Synchronization coefficient calculated")

	return avgCoef
}

// =========== CLIENT PART =========== //

func (t *GrpcTransport) getOrCreateClient(node *entities.Node) (*grpc.ClientConn, error) {
	// Без изменений — копируем ваш код:
	t.clientsMu.RLock()
	client, exists := t.clients[node.ID]
	t.clientsMu.RUnlock()
	if exists {
		log.Debug().
			Str("node_id", t.localNode.GetID()).
			Str("target_node", node.ID).
			Msg("using existing client connection")
		return client, nil
	}

	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	if client, exists = t.clients[node.ID]; exists {
		log.Debug().
			Str("node_id", t.localNode.GetID()).
			Str("target_node", node.ID).
			Msg("using existing client connection (after lock check)")
		return client, nil
	}

	host, ok := node.GetHost()
	if !ok {
		log.Error().
			Str("node_id", node.ID).
			Msg("don't have host property")
	}

	port, ok := node.GetGossipPort()
	if !ok {
		log.Error().
			Str("node_id", node.ID).
			Msg("don't have gossip port property")
	}

	targetAddr := fmt.Sprintf("%s:%d", host, port)
	log.Debug().
		Str("node_id", t.localNode.GetID()).
		Str("target_node", node.ID).
		Str("target_addr", targetAddr).
		Msg("creating new client connection")

	// Используем клиентскую TLS конфигурацию с отключенной проверкой имени хоста
	creds := credentials.NewTLS(t.clientTLSConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		targetAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Str("target_node", node.ID).
			Str("target_addr", targetAddr).
			Msg("error creating client connection")
		return nil, err
	}

	t.clients[node.ID] = conn
	log.Info().
		Str("node_id", t.localNode.GetID()).
		Str("target_node", node.ID).
		Str("target_addr", targetAddr).
		Msg("new client connection established")
	return conn, nil
}

// Sync now builds Merkle-bucket hashes instead of a Bloom filter.
func (t *GrpcTransport) Sync(targetNode *entities.Node) error {
	// Получаем все сущности через EntityRegistry
	entitiesByType := make(map[string][]common.Entity)

	// Добавляем ноды
	nodes, err := t.entityRegistry.Node.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting nodes")
	} else {
		nodeEntities := make([]common.Entity, 0, len(nodes))
		for _, node := range nodes {
			nodeEntities = append(nodeEntities, node)
		}
		entitiesByType[common.NodeEntityType] = nodeEntities
	}

	// Добавляем измерения
	measurements, err := t.entityRegistry.Measurement.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting measurements")
	} else {
		measurementEntities := make([]common.Entity, 0, len(measurements))
		for _, measurement := range measurements {
			measurementEntities = append(measurementEntities, measurement)
		}
		entitiesByType[common.MeasurementEntityType] = measurementEntities
	}

	// Добавляем группы
	groups, err := t.entityRegistry.Group.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting groups")
	} else {
		groupEntities := make([]common.Entity, 0, len(groups))
		for _, group := range groups {
			groupEntities = append(groupEntities, group)
		}
		entitiesByType[common.GroupEntityType] = groupEntities
	}

	// Добавляем цели групп
	groupGoals, err := t.entityRegistry.GroupGoal.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting group goals")
	} else {
		groupGoalEntities := make([]common.Entity, 0, len(groupGoals))
		for _, groupGoal := range groupGoals {
			groupGoalEntities = append(groupGoalEntities, groupGoal)
		}
		entitiesByType[common.GroupGoalEntityType] = groupGoalEntities
	}

	// Добавляем голоса
	votes, err := t.entityRegistry.Vote.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting votes")
	} else {
		voteEntities := make([]common.Entity, 0, len(votes))
		for _, vote := range votes {
			voteEntities = append(voteEntities, vote)
		}
		entitiesByType[common.VoteEntityType] = voteEntities
	}

	// Добавляем запросы на голосование
	voteRequests, err := t.entityRegistry.VoteRequest.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting vote requests")
	} else {
		voteRequestEntities := make([]common.Entity, 0, len(voteRequests))
		for _, voteRequest := range voteRequests {
			voteRequestEntities = append(voteRequestEntities, voteRequest)
		}
		entitiesByType[common.VoteRequestEntityType] = voteRequestEntities
	}

	// Добавляем отказы от лидерства
	leadershipResigns, err := t.entityRegistry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error getting leadership resigns")
	} else {
		leadershipResignEntities := make([]common.Entity, 0, len(leadershipResigns))
		for _, leadershipResign := range leadershipResigns {
			leadershipResignEntities = append(leadershipResignEntities, leadershipResign)
		}
		entitiesByType[common.LeadershipResignEntityType] = leadershipResignEntities
	}

	// 1) Разбиваем по типам → по bucket'ам
	// typeBucketMap: map[entityType] → map[bucketIdx] → []Entity
	typeBucketMap := make(map[string]map[int][]common.Entity)

	for entityType, entities := range entitiesByType {
		buckets := make(map[int][]common.Entity, totalBuckets)
		for _, entity := range entities {
			id := entity.GetID()
			bucketIdx := getBucketIndexFromID([]byte(id))
			buckets[bucketIdx] = append(buckets[bucketIdx], entity)
		}
		typeBucketMap[entityType] = buckets
	}

	// 2) Для каждой пары (entityType, bucketIdx) вычисляем MD5-хэш и
	//    собираем карту requestHashMap: map["entityType#bucketIdx"] → []byte(16)
	requestHashMap := make(map[string][]byte, len(typeBucketMap)*totalBuckets)

	// предвычислим MD5(nil) для пустых корзин
	emptyHash := md5.New().Sum(nil)

	for entityType, buckets := range typeBucketMap {
		for i := 0; i < totalBuckets; i++ {
			ents := buckets[i] // может быть nil
			var bucketHash []byte
			if len(ents) == 0 {
				bucketHash = emptyHash
			} else {
				bucketHash = computeBucketHash(ents)
			}
			compositeKey := entityType + "#" + strconv.Itoa(i)
			requestHashMap[compositeKey] = bucketHash
		}
	}

	// 3) Делаем gRPC вызов, передав в GetDataRequest эти хэши.
	client, err := t.getOrCreateClient(targetNode)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Str("target_node", targetNode.ID).
			Msg("error getting client")
		return fmt.Errorf("error getting client for node %s: %v", targetNode.ID, err)
	}

	msgClient := pb.NewGossipServiceClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	localNodeBytes, err := t.localNode.Serialize()

	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Msg("error serializing local node")
		return fmt.Errorf("error serializing local node: %v", err)
	}

	req := &pb.GetDataRequest{
		NodeId:       t.localNode.GetID(),
		NodeData:     localNodeBytes,
		BucketHashes: requestHashMap,
	}

	resp, err := msgClient.GetData(ctx, req)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", t.localNode.GetID()).
			Str("target_node", targetNode.ID).
			Msg("error sending message")
		return fmt.Errorf("error sending message to node %s: %v", targetNode.ID, err)
	}

	// Счетчики для статистики синхронизации
	totalEntities := 0
	diffEntities := 0

	for _, entities := range entitiesByType {
		totalEntities += len(entities)
	}

	// 4) Process the response: we will get only those "entityType#bucketIdx" where MD5 !=
	for compositeKey, bytesArray := range resp.Data {
		parts := strings.SplitN(compositeKey, "#", 2)
		if len(parts) != 2 {
			continue
		}
		entityType := parts[0]
		// bucketIdx, _ := strconv.Atoi(parts[1]) - we don't need this inside Go anymore for StoreEntity

		for _, entityBytes := range bytesArray.Items {
			// Depending on entityType we deserialize into a specific object.
			switch entityType {
			case common.NodeEntityType:
				node, err := entities.NodeFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing node entity")
					continue
				}
				if saved, err := t.entityRegistry.Node.StoreEntity(node); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", node.GetID()).
						Msg("error storing node entity")
				} else if saved {
					diffEntities++
				}
			case common.MeasurementEntityType:
				measurement, err := entities.MeasurementFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing measurement entity")
					continue
				}
				if saved, err := t.entityRegistry.Measurement.StoreEntity(measurement); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", measurement.GetID()).
						Msg("error storing measurement entity")
				} else if saved {
					diffEntities++
				}
			case common.GroupEntityType:
				group, err := entities.GroupFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing group entity")
					continue
				}

				if saved, err := t.entityRegistry.Group.StoreEntity(group); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", group.GetID()).
						Msg("error storing group entity")
				} else if saved {
					diffEntities++
				}
			case common.GroupGoalEntityType:
				groupGoal, err := entities.GroupGoalFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing group goal entity")
					continue
				}
				if saved, err := t.entityRegistry.GroupGoal.StoreEntity(groupGoal); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", groupGoal.GetID()).
						Msg("error storing group goal entity")
				} else if saved {
					diffEntities++
				}
			case common.VoteEntityType:
				vote, err := entities.VoteFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing vote entity")
					continue
				}
				if saved, err := t.entityRegistry.Vote.StoreEntity(vote); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", vote.GetID()).
						Msg("error storing vote entity")
				} else if saved {
					diffEntities++
				}
			case common.VoteRequestEntityType:
				voteRequest, err := entities.VoteRequestFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing vote request entity")
					continue
				}
				if saved, err := t.entityRegistry.VoteRequest.StoreEntity(voteRequest); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", voteRequest.GetID()).
						Msg("error storing vote request entity")
				} else if saved {
					diffEntities++
				}
			case common.LeadershipResignEntityType:
				leadershipResign, err := entities.LeadershipResignFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing leadership resign entity")
					continue
				}
				if saved, err := t.entityRegistry.LeadershipResign.StoreEntity(leadershipResign); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", leadershipResign.GetID()).
						Msg("error storing leadership resign entity")
				} else if saved {
					diffEntities++
				}
			default:
				log.Debug().
					Str("node_id", t.localNode.GetID()).
					Str("entity_type", entityType).
					Msg("unsupported entity type")
			}
		}
	}

	// Calculate the synchronization coefficient for the current operation
	// 1.0 means complete synchronization (no differences)
	// 0.0 means complete de-synchronization (all entities differ)
	syncCoef := 1.0
	if totalEntities > 0 {
		// If diffEntities is greater than totalEntities, limit the ratio to one
		syncRatio := float64(diffEntities) / float64(totalEntities)
		if syncRatio > 1.0 {
			syncRatio = 1.0
		}
		syncCoef = 1.0 - syncRatio
	}

	// Save synchronization statistics
	syncStat := SyncStat{
		Timestamp:       time.Now(),
		RemoteNodeID:    targetNode.ID,
		TotalEntities:   totalEntities,
		DiffEntities:    diffEntities,
		SyncCoefficient: syncCoef,
	}

	t.syncStatsMu.Lock()
	t.syncStats = append(t.syncStats, syncStat)
	t.syncStatsMu.Unlock()

	log.Debug().
		Str("node_id", t.localNode.GetID()).
		Str("target_node", targetNode.ID).
		Int("total_entities", totalEntities).
		Int("diff_entities", diffEntities).
		Float64("sync_coef", syncCoef).
		Msg("Sync (Merkle) completed successfully")
	return nil
}
