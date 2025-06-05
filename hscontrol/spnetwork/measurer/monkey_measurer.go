package measurer

import (
	"fmt"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"math/rand"
	"sync"
	"time"
)

// MonkeyMeasurerConfig содержит настройки для MonkeyMeasurer
type MonkeyMeasurerConfig struct {
	MeasureInterval            time.Duration
	NewValueProbability        float64 // Вероятность использования нового случайного значения вместо существующего (0.0-1.0)
	NodeUnavailableProbability float64 // Вероятность того, что нода будет недоступна (0.0-1.0)
}

// MonkeyMeasurer реализует Measurer, который генерирует случайные значения для тестирования
type MonkeyMeasurer struct {
	entityRegistry             *common.EntityRegistry
	localNode                  *entities.Node
	running                    bool
	mu                         sync.Mutex
	measureInterval            time.Duration
	newValueProbability        float64
	nodeUnavailableProbability float64
	stopChan                   chan struct{}
	measureRunning             bool
	rand                       *rand.Rand
}

// NewMonkeyMeasurer создает новый экземпляр MonkeyMeasurer
func NewMonkeyMeasurer(entityRegistry *common.EntityRegistry, localNode *entities.Node, config MonkeyMeasurerConfig) (*MonkeyMeasurer, error) {
	log.Debug().
		Str("node_id", localNode.GetID()).
		Dur("measure_interval", config.MeasureInterval).
		Float64("new_value_probability", config.NewValueProbability).
		Float64("node_unavailable_probability", config.NodeUnavailableProbability).
		Msg("creating monkey measurer")

	if config.NewValueProbability < 0 || config.NewValueProbability > 1 {
		return nil, fmt.Errorf("new value probability must be between 0 and 1")
	}

	if config.NodeUnavailableProbability < 0 || config.NodeUnavailableProbability > 1 {
		return nil, fmt.Errorf("node unavailable probability must be between 0 and 1")
	}

	measurer := &MonkeyMeasurer{
		entityRegistry:             entityRegistry,
		localNode:                  localNode,
		measureInterval:            config.MeasureInterval,
		newValueProbability:        config.NewValueProbability,
		nodeUnavailableProbability: config.NodeUnavailableProbability,
		stopChan:                   make(chan struct{}),
		rand:                       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	log.Info().
		Str("node_id", localNode.GetID()).
		Msg("Monkey measurer created")

	return measurer, nil
}

// Start запускает измерение задержки
func (m *MonkeyMeasurer) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		log.Warn().Str("node_id", m.localNode.GetID()).Msg("measurer already running")
		return fmt.Errorf("measurer already running")
	}

	log.Info().
		Str("node_id", m.localNode.GetID()).
		Msg("starting monkey measurer")

	go m.measureLoop()

	m.running = true
	log.Info().
		Str("node_id", m.localNode.GetID()).
		Msg("Monkey measurer started")

	return nil
}

// Stop останавливает измерение задержки
func (m *MonkeyMeasurer) Stop() error {
	m.mu.Lock()

	if !m.running {
		m.mu.Unlock()
		log.Warn().Str("node_id", m.localNode.GetID()).Msg("measurer already stopped")
		return fmt.Errorf("measurer already stopped")
	}

	log.Info().Str("node_id", m.localNode.GetID()).Msg("stopping monkey measurer")

	// Сначала отмечаем, что сервис остановлен
	m.running = false

	// Закрываем канал для сигнала горутинам
	close(m.stopChan)

	m.mu.Unlock()

	log.Info().Str("node_id", m.localNode.GetID()).Msg("Monkey measurer stopped")

	return nil
}

// measureLoop периодически "измеряет" задержку до всех удаленных нод
func (m *MonkeyMeasurer) measureLoop() {
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
func (m *MonkeyMeasurer) runMeasureIfNotRunning() error {
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

		for _, targetNode := range remoteNodes {
			_ = m.measureNode(targetNode)
		}
	}()

	return nil
}

// measureNode генерирует случайное измерение задержки до указанной ноды
func (m *MonkeyMeasurer) measureNode(targetNode *entities.Node) error {
	// Проверяем, доступна ли нода (с заданной вероятностью)
	if m.rand.Float64() < m.nodeUnavailableProbability {
		log.Debug().
			Str("node_id", m.localNode.GetID()).
			Str("target_node", targetNode.GetID()).
			Msg("simulating unavailable node")
		return nil
	}

	// Проверяем существующее измерение
	measurementID := fmt.Sprintf("%s-%s", m.localNode.GetID(), targetNode.GetID())
	measurement, err := m.entityRegistry.Measurement.GetEntity(measurementID)
	found := measurement != nil

	// Если измерение существует и мы решаем использовать старое значение
	if found && m.rand.Float64() > m.newValueProbability {
		log.Debug().
			Str("node_id", m.localNode.GetID()).
			Str("target_node", targetNode.GetID()).
			Float64("latency_class", measurement.Value).
			Msg("keeping existing latency class")
		return nil
	}

	// Генерируем случайный класс задержки (0-4)
	randomLatencyClass := float64(m.rand.Intn(5))

	log.Debug().
		Str("node_id", m.localNode.GetID()).
		Str("target_node", targetNode.GetID()).
		Float64("latency_class", randomLatencyClass).
		Msg("generated random latency class")

	if !found {
		// Создаем новое измерение
		measurement = entities.NewMeasurement(
			m.localNode.GetID(),
			targetNode.GetID(),
			entities.LatencyClass,
			randomLatencyClass,
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
		if measurement.Value != randomLatencyClass {
			measurement.UpdateValue(randomLatencyClass, time.Now().Unix())

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
