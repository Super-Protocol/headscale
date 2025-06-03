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

type GrpcTransport struct {
	entityRegistry common.EntityRegistry
	localNode      *entities.Node
	running        bool
	mu             sync.Mutex
	grpcServer     *grpc.Server
	clients        map[string]*grpc.ClientConn
	clientsMu      sync.RWMutex
	tlsConfig      *tls.Config
	listenAddr     string
}

// GossipServiceServer represents a gRPC server for message processing
type GossipServiceServer struct {
	pb.UnimplementedGossipServiceServer
	transport *GrpcTransport
}

const (
	// depthBits определяет, сколько бит мы берём из MD5(ID) для индексирования bucket'ов.
	// depthBits = 8 → 256 корзин; depthBits = 10 → 1024 корзин.
	depthBits    = 8
	totalBuckets = 1 << depthBits
)

// getBucketIndexFromID возвращает номер корзины [0..totalBuckets-1] для конкретного ID.
// Мы берём первые depthBits бит из MD5(ID).
func getBucketIndexFromID(id []byte) int {
	sum := md5.Sum(id) // [16]byte
	// depthBits <= 8: возьмём старшие depthBits бит из первого байта
	if depthBits <= 8 {
		// shiftRight = 8 - depthBits, чтобы получить только нужное число бит
		return int(sum[0]) >> (8 - depthBits)
	}
	// Если depthBits > 8, то нужно взять, например, sum[0] как старшие 8 бит,
	// а к ним добавить (depthBits-8) бит из sum[1]. Но в примере depthBits=8.
	return int(sum[0])
}

// computeBucketHash собирает MD5-хэш от упорядоченного списка (ID||version),
// чтобы при любом изменении версии или содержимого у entity хэш менялся.
// entities уже все лежат внутри одной корзины.
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
	return h.Sum(nil) // 16 байт MD5
}

func NewGrpcTransport(entityRegistry common.EntityRegistry, localNode *entities.Node, config GrpcTransportConfig) (*GrpcTransport, error) {
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

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    rootCAs,
		RootCAs:      rootCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	listenAddr := fmt.Sprintf("%s:%d", config.ListenHost, config.ListenPort)
	transport := &GrpcTransport{
		localNode:      localNode,
		running:        false,
		clients:        make(map[string]*grpc.ClientConn),
		tlsConfig:      tlsConfig,
		listenAddr:     listenAddr,
		entityRegistry: entityRegistry,
	}

	log.Info().
		Str("node_id", localNode.GetID()).
		Str("listen_addr", listenAddr).
		Msg("gRPC transport created")
	return transport, nil
}

// GetData теперь сравнивает заранее рассчитанные клиентом MD5-хэши корзин (BucketHashes)
// и отдаёт только данные (full Serialize()), относящиеся к тем корзинам, где хэши !=.
func (s *GossipServiceServer) GetData(ctx context.Context, req *pb.GetDataRequest) (*pb.GetDataResponse, error) {

	log.Debug().
		Str("local_node_id", s.transport.localNode.GetID()).
		Str("req_node_id", req.NodeId).
		Msg("Обработка запроса GetData")

	entitiesByType := s.transport.entityRegistry.GetAllEntities()

	nodeEntities := make([]entities.Node, 0, len(entitiesByType["node"]))
	for _, entity := range entitiesByType["node"] {
		if node, ok := entity.(*entities.Node); ok {
			nodeEntities = append(nodeEntities, *node)
		}
	}

	log.Debug().
		Str("local_node_id", s.transport.localNode.GetID()).
		Str("req_node_id", req.NodeId).
		Msgf("node entities: %v", nodeEntities)

	requestingNode, err := entities.NodeFromProtoBytes(req.NodeData)

	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", s.transport.localNode.GetID()).
			Msg("can't deserialize node data")
		return nil, err
	} else {
		err := s.transport.entityRegistry.StoreEntity(common.NodeEntityType, requestingNode)
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", s.transport.localNode.GetID()).
				Msg("can't store requesting node")
			return nil, err
		}
	}

	// 1) Построим у себя “свои” bucket’ы: map[entityType] → map[bucketIdx] → []Entity
	typeBucketMap := make(map[string]map[int][]common.Entity)
	for entityTypeBytes, entities := range entitiesByType {
		entityType := string(entityTypeBytes)
		buckets := make(map[int][]common.Entity, totalBuckets)
		for _, entity := range entities {
			id := entity.GetID()
			bucketIdx := getBucketIndexFromID([]byte(id))
			buckets[bucketIdx] = append(buckets[bucketIdx], entity)
		}
		typeBucketMap[entityType] = buckets
	}

	// 2) Пройдём по запросу. Ключ в запросе – “entityType#bucketIdx”, значение – клиентский MD5-хэш.
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

		// 4) Сравниваем: если !=, то возвращаем все сущности из этого bucket’a
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

// =========== CLIENT PART =========== //

// Sync теперь строит Merkle-bucket хэши вместо Bloom-фильтра.
func (t *GrpcTransport) Sync(targetNode *entities.Node) error {
	entitiesByType := t.entityRegistry.GetAllEntities()

	// 1) Разбиваем по типам → по bucket’ам
	// typeBucketMap: map[entityType] → map[bucketIdx] → []Entity
	typeBucketMap := make(map[string]map[int][]common.Entity)

	for entityTypeBytes, entities := range entitiesByType {
		entityType := string(entityTypeBytes)
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

	// 4) Обрабатываем ответ: получим только те “entityType#bucketIdx”, где MD5 !=
	for compositeKey, bytesArray := range resp.Data {
		parts := strings.SplitN(compositeKey, "#", 2)
		if len(parts) != 2 {
			continue
		}
		entityType := parts[0]
		// bucketIdx, _ := strconv.Atoi(parts[1]) – этого нам внутри Go уже не нужно для StoreEntity

		for _, entityBytes := range bytesArray.Items {
			// В зависимости от entityType десериализуем в конкретный объект.
			if entityType == common.NodeEntityType {
				node, err := entities.NodeFromProtoBytes(entityBytes)
				if err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Msg("error deserializing node entity")
					continue
				}
				if err := t.entityRegistry.StoreEntity(common.NodeEntityType, node); err != nil {
					log.Error().
						Err(err).
						Str("node_id", t.localNode.GetID()).
						Str("entity_type", entityType).
						Str("entity_id", node.GetID()).
						Msg("error storing node entity")
				}
			} else {
				log.Debug().
					Str("node_id", t.localNode.GetID()).
					Str("entity_type", entityType).
					Msg("unsupported  entity type")
			}
		}
	}

	log.Debug().
		Str("node_id", t.localNode.GetID()).
		Str("target_node", targetNode.ID).
		Msg("Sync (Merkle) completed successfully")
	return nil
}

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

	creds := credentials.NewTLS(t.tlsConfig)
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
