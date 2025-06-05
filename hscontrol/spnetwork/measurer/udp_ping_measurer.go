package measurer

import (
	"encoding/binary"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
	"time"
)

// UDPPingMeasurerConfig содержит настройки для UDPPingMeasurer
type UDPPingMeasurerConfig struct {
	ListenHost      string
	ListenPort      int
	MeasureInterval time.Duration
}

// UDPPingMeasurer реализует Measurer с использованием UDP для ping запросов
type UDPPingMeasurer struct {
	entityRegistry  *common.EntityRegistry
	localNode       *entities.Node
	listenAddr      string
	conn            *net.UDPConn
	running         bool
	mu              sync.Mutex
	measureInterval time.Duration
	stopChan        chan struct{}
	measureRunning  bool
}

// NewUDPPingMeasurer создает новый экземпляр UDPPingMeasurer
func NewUDPPingMeasurer(entityRegistry *common.EntityRegistry, localNode *entities.Node, config UDPPingMeasurerConfig) (*UDPPingMeasurer, error) {
	log.Debug().
		Str("node_id", localNode.GetID()).
		Str("listen_host", config.ListenHost).
		Int("listen_port", config.ListenPort).
		Dur("measure_interval", config.MeasureInterval).
		Msg("creating UDP ping measurer")

	listenAddr := fmt.Sprintf("%s:%d", config.ListenHost, config.ListenPort)

	measurer := &UDPPingMeasurer{
		entityRegistry:  entityRegistry,
		localNode:       localNode,
		listenAddr:      listenAddr,
		measureInterval: config.MeasureInterval,
		stopChan:        make(chan struct{}),
	}

	log.Info().
		Str("node_id", localNode.GetID()).
		Str("listen_addr", listenAddr).
		Msg("UDP ping measurer created")

	return measurer, nil
}

// Start запускает UDP сервер и измерение задержки
func (m *UDPPingMeasurer) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		log.Warn().Str("node_id", m.localNode.GetID()).Msg("measurer already running")
		return fmt.Errorf("measurer already running")
	}

	log.Info().
		Str("node_id", m.localNode.GetID()).
		Str("listen_addr", m.listenAddr).
		Msg("starting UDP ping measurer")

	addr, err := net.ResolveUDPAddr("udp", m.listenAddr)
	if err != nil {
		log.Error().
			Err(err).
			Str("listen_addr", m.listenAddr).
			Msg("failed to resolve UDP address")
		return fmt.Errorf("failed to resolve UDP address %s: %v", m.listenAddr, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Error().
			Err(err).
			Str("listen_addr", m.listenAddr).
			Msg("failed to start UDP listener")
		return fmt.Errorf("failed to start UDP listener on %s: %v", m.listenAddr, err)
	}

	m.conn = conn

	go m.handleIncomingPackets()
	go m.measureLoop()

	m.running = true
	log.Info().
		Str("node_id", m.localNode.GetID()).
		Str("listen_addr", m.listenAddr).
		Msg("UDP ping measurer started")

	return nil
}

// Stop останавливает UDP сервер и измерение задержки
func (m *UDPPingMeasurer) Stop() error {
	m.mu.Lock()

	if !m.running {
		m.mu.Unlock()
		log.Warn().Str("node_id", m.localNode.GetID()).Msg("measurer already stopped")
		return fmt.Errorf("measurer already stopped")
	}

	log.Info().Str("node_id", m.localNode.GetID()).Msg("stopping UDP ping measurer")

	// Сначала отмечаем, что сервис остановлен
	m.running = false

	// Закрываем канал для сигнала горутинам
	close(m.stopChan)

	// Сохраняем ссылку на соединение перед его закрытием
	conn := m.conn

	// Обнуляем соединение под блокировкой
	m.conn = nil

	// Разблокируем мьютекс перед потенциально блокирующей операцией Close()
	m.mu.Unlock()

	// Закрываем соединение после разблокировки мьютекса
	if conn != nil {
		if err := conn.Close(); err != nil {
			log.Error().
				Err(err).
				Str("node_id", m.localNode.GetID()).
				Msg("error closing UDP connection")
			return err
		}
	}

	log.Info().Str("node_id", m.localNode.GetID()).Msg("UDP ping measurer stopped")

	return nil
}

// handleIncomingPackets обрабатывает входящие UDP пакеты
func (m *UDPPingMeasurer) handleIncomingPackets() {
	buffer := make([]byte, 16) // достаточно для int64 timestamp

	for {
		// Проверяем сигнал остановки
		select {
		case <-m.stopChan:
			return // нормальное завершение
		default:
			// продолжаем выполнение
		}

		// Безопасно получаем текущее соединение под блокировкой
		m.mu.Lock()
		conn := m.conn
		m.mu.Unlock()

		// Проверяем, что соединение еще активно
		if conn == nil {
			// Соединение закрыто, выходим из цикла
			return
		}

		// Устанавливаем небольшой таймаут чтения, чтобы периодически проверять сигнал остановки
		err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", m.localNode.GetID()).
				Msg("error setting read deadline")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Это просто таймаут чтения, продолжаем
				continue
			}

			// Проверяем сигнал остановки еще раз
			select {
			case <-m.stopChan:
				return
			default:
				log.Error().
					Err(err).
					Str("node_id", m.localNode.GetID()).
					Msg("error reading from UDP connection")
				continue
			}
		}

		if n < 8 { // минимальный размер сообщения - 8 байт (int64)
			continue
		}

		// Проверяем первый байт для определения типа сообщения
		messageType := buffer[0]

		switch messageType {
		case 0x01: // Ping request
			if n < 9 { // 1 байт тип + 8 байт timestamp
				continue
			}
			timestamp := int64(binary.BigEndian.Uint64(buffer[1:9]))
			m.handlePingRequest(addr, timestamp)

		case 0x02: // Ping response
			if n < 17 { // 1 байт тип + 8 байт req timestamp + 8 байт resp timestamp
				continue
			}
			reqTimestamp := int64(binary.BigEndian.Uint64(buffer[1:9]))
			respTimestamp := int64(binary.BigEndian.Uint64(buffer[9:17]))
			m.handlePingResponse(reqTimestamp, respTimestamp)
		}
	}
}

// handlePingRequest обрабатывает входящий ping запрос
func (m *UDPPingMeasurer) handlePingRequest(addr *net.UDPAddr, requestTimestamp int64) {
	responseBuffer := make([]byte, 17) // 1 byte message type + 8 bytes req timestamp + 8 bytes resp timestamp
	responseBuffer[0] = 0x02           // Ping response

	// Копируем timestamp запроса
	binary.BigEndian.PutUint64(responseBuffer[1:9], uint64(requestTimestamp))

	// Добавляем timestamp ответа
	responseTimestamp := time.Now().UnixNano()
	binary.BigEndian.PutUint64(responseBuffer[9:17], uint64(responseTimestamp))

	// Безопасно получаем текущее соединение под блокировкой
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	// Проверяем, что соединение еще активно
	if conn == nil {
		log.Debug().
			Str("node_id", m.localNode.GetID()).
			Str("remote_addr", addr.String()).
			Msg("connection closed, skipping ping response")
		return
	}

	_, err := conn.WriteToUDP(responseBuffer, addr)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", m.localNode.GetID()).
			Str("remote_addr", addr.String()).
			Msg("error sending ping response")
	}
}

// handlePingResponse обрабатывает ответ на ping
func (m *UDPPingMeasurer) handlePingResponse(requestTimestamp, responseTimestamp int64) {
	receivedAt := time.Now().UnixNano()

	// Вычисляем RTT в миллисекундах
	// RTT = (полученный_время - отправленный_время) / 1_000_000
	rttNanos := receivedAt - requestTimestamp
	rttMs := float64(rttNanos) / 1_000_000.0

	log.Debug().
		Str("node_id", m.localNode.GetID()).
		Float64("rtt_ms", rttMs).
		Msg("received ping response")

	// Поскольку мы не можем определить, от какой ноды пришел ответ,
	// по UDP пакету, мы полагаемся на соответствие между timestamp запроса
	// и текущей ноды, к которой был отправлен запрос.
	//
	// В реальном продакшн-решении можно добавить ID ноды в пакет.
}

// measureLoop периодически измеряет задержку до всех удаленных нод
func (m *UDPPingMeasurer) measureLoop() {
	ticker := time.NewTicker(m.measureInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := m.runMeasureIfNotRunning()
			if err != nil {
				return
			}
		case <-m.stopChan:
			return
		}
	}
}

// runMeasureIfNotRunning запускает измерение, если оно еще не запущено
func (m *UDPPingMeasurer) runMeasureIfNotRunning() error {
	m.mu.Lock()
	if m.measureRunning {
		m.mu.Unlock()
		return nil
	}
	m.measureRunning = true
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			m.measureRunning = false
			m.mu.Unlock()
		}()

		nodes, err := m.entityRegistry.Node.GetAllEntities()
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", m.localNode.GetID()).
				Msg("error getting nodes")
			return
		}

		if len(nodes) == 0 {
			return
		}

		var remoteNodes []*entities.Node
		for _, n := range nodes {
			if n.GetID() != m.localNode.GetID() && !n.IsDeleted() {
				remoteNodes = append(remoteNodes, n)
			}
		}

		if len(remoteNodes) == 0 {
			return
		}

		// Измеряем все ноды, но можно оптимизировать, чтобы измерять только случайные
		for _, targetNode := range remoteNodes {
			_ = m.measureNode(targetNode)
		}
	}()

	return nil
}

// measureNode измеряет задержку до указанной ноды
func (m *UDPPingMeasurer) measureNode(targetNode *entities.Node) error {
	host, ok := targetNode.GetHost()
	if !ok {
		log.Error().
			Str("node_id", targetNode.GetID()).
			Msg("node doesn't have host property")
		return fmt.Errorf("node %s doesn't have host property", targetNode.GetID())
	}

	// Здесь мы используем тот же порт, что и для gossip, если это не указано отдельно
	port, ok := targetNode.GetUdpPingPort()
	if !ok {
		log.Error().
			Str("node_id", targetNode.GetID()).
			Msg("node doesn't have gossip_port property")
		return fmt.Errorf("node %s doesn't have gossip_port property", targetNode.GetID())
	}

	targetAddr := fmt.Sprintf("%s:%d", host, port)

	log.Debug().
		Str("node_id", m.localNode.GetID()).
		Str("target_node", targetNode.GetID()).
		Str("target_addr", targetAddr).
		Msg("sending ping request")

	udpAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", m.localNode.GetID()).
			Str("target_addr", targetAddr).
			Msg("error resolving UDP address")
		return err
	}

	// Создаем ping request
	requestBuffer := make([]byte, 9) // 1 byte message type + 8 bytes timestamp
	requestBuffer[0] = 0x01          // Ping request

	// Генерируем уникальный timestamp для запроса
	requestTimestamp := time.Now().UnixNano()
	binary.BigEndian.PutUint64(requestBuffer[1:9], uint64(requestTimestamp))

	// Проверяем доступность соединения под блокировкой
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	// Проверяем, что соединение еще активно
	if conn == nil {
		log.Debug().
			Str("node_id", m.localNode.GetID()).
			Str("target_node", targetNode.GetID()).
			Msg("connection already closed, skipping measurement")
		return nil
	}

	// Отправляем запрос
	_, err = conn.WriteToUDP(requestBuffer, udpAddr)
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", m.localNode.GetID()).
			Str("target_addr", targetAddr).
			Msg("error sending ping request")
		return err
	}

	// Ждем ответа с таймаутом
	err = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", m.localNode.GetID()).
			Msg("error setting read deadline")
		return err
	}

	responseBuffer := make([]byte, 17)
	var rttMs float64

	// Читаем ответы, пока не найдем соответствующий или не истечет таймаут
	responseReceived := false
	startTime := time.Now()

	for time.Since(startTime) < 2*time.Second {
		// Проверяем наличие сигнала остановки
		select {
		case <-m.stopChan:
			log.Debug().
				Str("node_id", m.localNode.GetID()).
				Str("target_node", targetNode.GetID()).
				Msg("stopping measurement due to stop signal")
			return nil
		default:
			// Продолжаем выполнение
		}

		n, addr, err := conn.ReadFromUDP(responseBuffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break // Таймаут
			}
			log.Error().
				Err(err).
				Str("node_id", m.localNode.GetID()).
				Str("target_addr", targetAddr).
				Msg("error reading ping response")
			break
		}

		if n < 17 || addr.String() != udpAddr.String() || responseBuffer[0] != 0x02 {
			continue // Не ответ на ping или от другого адреса
		}

		respReqTimestamp := int64(binary.BigEndian.Uint64(responseBuffer[1:9]))
		if respReqTimestamp != requestTimestamp {
			continue // Не соответствует нашему запросу
		}

		// Нашли соответствующий ответ
		receivedAt := time.Now().UnixNano()
		rttNanos := receivedAt - requestTimestamp
		rttMs = float64(rttNanos) / 1_000_000.0
		responseReceived = true
		break
	}

	// Сбрасываем таймаут
	err = conn.SetReadDeadline(time.Time{})
	if err != nil {
		log.Error().
			Err(err).
			Str("node_id", m.localNode.GetID()).
			Msg("error resetting read deadline")
	}

	if !responseReceived {
		log.Debug().
			Str("node_id", m.localNode.GetID()).
			Str("target_node", targetNode.GetID()).
			Msg("ping timeout")
		return nil
	}

	log.Debug().
		Str("node_id", m.localNode.GetID()).
		Str("target_node", targetNode.GetID()).
		Float64("rtt_ms", rttMs).
		Msg("measured latency")

	// Сохраняем измерение в реестре
	measurementID := fmt.Sprintf("%s-%s", m.localNode.GetID(), targetNode.GetID())
	measurement, err := m.entityRegistry.Measurement.GetEntity(measurementID)
	found := measurement != nil
	if !found {
		// Создаем новое измерение
		measurement = entities.NewMeasurement(
			m.localNode.GetID(),
			targetNode.GetID(),
			entities.LatencyClass,
			entities.CalculateLatencyClass(rttMs),
			time.Now().Unix(),
		)
		_, err = m.entityRegistry.Measurement.StoreEntity(measurement)
		if err != nil {
			log.Error().
				Err(err).
				Str("node_id", m.localNode.GetID()).
				Str("target_node", targetNode.GetID()).
				Msg("error storing new measurement")
			return err
		}
	} else {
		// Обновляем существующее измерение
		cls := entities.CalculateLatencyClass(rttMs)
		if measurement.Value != cls {
			measurement.UpdateValue(cls, time.Now().Unix())

			_, err = m.entityRegistry.Measurement.StoreEntity(measurement)
			if err != nil {
				log.Error().
					Err(err).
					Str("node_id", m.localNode.GetID()).
					Str("target_node", targetNode.GetID()).
					Msg("error updating measurement")
				return err
			}
		}
	}

	return nil
}
