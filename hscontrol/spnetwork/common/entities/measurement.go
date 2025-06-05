package entities

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"sync"
)

type MeasurementType string

const (
	LatencyClass   MeasurementType = "latency_class"
	BandwidthClass                 = "bandwidth_class"
)

// Константы для классов задержки
const (
	LatencyClass0 float64 = 0 // 0-10 мс
	LatencyClass1 float64 = 1 // 8-50 мс
	LatencyClass2 float64 = 2 // 40-100 мс
	LatencyClass3 float64 = 3 // 80-200 мс
	LatencyClass4 float64 = 4 // 150+ мс
)

type Measurement struct {
	Owner    string
	Target   string
	Type     MeasurementType
	Value    float64
	DateUnix int64
	Version  uint64
	Deleted  bool
	mu       sync.RWMutex
}

func NewMeasurement(owner, target string, t MeasurementType, value float64, dateUnix int64) *Measurement {
	return &Measurement{
		Owner:    owner,
		Target:   target,
		Type:     t,
		Value:    value,
		DateUnix: dateUnix,
		Version:  0,
		Deleted:  false,
	}
}

// GetID возвращает уникальный идентификатор измерения в формате "owner-target"
func (m *Measurement) GetID() string {
	return fmt.Sprintf("%s-%s", m.Owner, m.Target)
}

// GetVersion возвращает текущую версию измерения
func (m *Measurement) GetVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Version
}

// IsDeleted возвращает статус удаления
func (m *Measurement) IsDeleted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Deleted
}

// GetHash вычисляет хеш для измерения, используя только ID и Version
func (m *Measurement) GetHash() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h := md5.New()
	h.Write([]byte(m.GetID()))

	err := binary.Write(h, binary.LittleEndian, m.Version)
	if err != nil {
		return nil
	}

	return h.Sum(nil)
}

// ToProto конвертирует Measurement в protobuf сообщение
func (m *Measurement) ToProto() *p.Measurement {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &p.Measurement{
		Owner:    m.Owner,
		Target:   m.Target,
		Type:     string(m.Type),
		Value:    m.Value,
		DateUnix: m.DateUnix,
		Version:  m.Version,
		Deleted:  m.Deleted,
	}
}

// Serialize сериализует Measurement в байты
func (m *Measurement) Serialize() ([]byte, error) {
	return proto.Marshal(m.ToProto())
}

// UpdateValue обновляет значение измерения и увеличивает версию
func (m *Measurement) UpdateValue(value float64, dateUnix int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Value = value
	m.DateUnix = dateUnix
	m.Version++
}

// MarkDeleted помечает измерение как удаленное и увеличивает версию
func (m *Measurement) MarkDeleted() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Deleted = true
	m.Version++
}

// MeasurementFromProto создает Measurement из protobuf сообщения
func MeasurementFromProto(p *p.Measurement) *Measurement {
	return &Measurement{
		Owner:    p.Owner,
		Target:   p.Target,
		Type:     MeasurementType(p.Type),
		Value:    p.Value,
		DateUnix: p.DateUnix,
		Version:  p.Version,
		Deleted:  p.Deleted,
	}
}

// MeasurementFromProtoBytes десериализует Measurement из байтов
func MeasurementFromProtoBytes(data []byte) (*Measurement, error) {
	protoMeasurement := &p.Measurement{}
	err := proto.Unmarshal(data, protoMeasurement)
	if err != nil {
		return nil, err
	}
	return MeasurementFromProto(protoMeasurement), nil
}

// CalculateLatencyClass определяет класс задержки на основе измеренного значения
func CalculateLatencyClass(latencyMs float64) float64 {
	switch {
	case latencyMs <= 10:
		return LatencyClass0
	case latencyMs >= 8 && latencyMs <= 50:
		return LatencyClass1
	case latencyMs >= 40 && latencyMs <= 100:
		return LatencyClass2
	case latencyMs >= 80 && latencyMs <= 200:
		return LatencyClass3
	default:
		return LatencyClass4 // 150+ мс
	}
}
