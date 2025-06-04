package entities

import (
	"crypto/md5"
	"encoding/binary"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sync"
)

// DimensionConditionType определяет тип условия для измерения
type DimensionConditionType string

const (
	// ConditionLessThan - значение должно быть меньше указанного
	ConditionLessThan DimensionConditionType = "lt"
	// ConditionGreaterThan - значение должно быть больше указанного
	ConditionGreaterThan DimensionConditionType = "gt"
	// ConditionBetween - значение должно быть в указанном диапазоне
	ConditionBetween DimensionConditionType = "between"
	// ConditionMin - минимальное значение в группе
	ConditionMin DimensionConditionType = "min"
	// ConditionMax - максимальное значение в группе
	ConditionMax DimensionConditionType = "max"
)

// DimensionCriterion описывает критерий для измерения
type DimensionCriterion struct {
	// Type - тип измерения
	Type MeasurementType `json:"type"`
	// Condition - тип условия для включения в группу
	Condition DimensionConditionType `json:"condition"`
	// Values - значения для условия (одно или два в зависимости от типа условия)
	Values []float64 `json:"values"`
}

// GroupGoal определяет цель формирования групп нод
type GroupGoal struct {
	// ID уникальный идентификатор цели
	ID string
	// MinGroupSize минимальное количество нод в группе
	MinGroupSize int
	// MaxGroupSize максимальное количество нод в группе
	MaxGroupSize int
	// MaxGroups максимальное количество групп (опционально)
	MaxGroups *int
	// InactivityTimeout время в секундах, после которого неактивный участник исключается из группы
	// Если 0, то исключение по таймауту не производится
	InactivityTimeout int64
	// DimensionCriteria критерии включения нод в группу
	DimensionCriteria []DimensionCriterion
	// Version - версия сущности, увеличивается при изменениях
	Version uint64
	// Deleted - признак удаления
	Deleted bool
	// мьютекс для безопасного доступа к полям
	mu sync.RWMutex
}

// GetMinGroupSize возвращает минимальный размер группы
func (g *GroupGoal) GetMinGroupSize() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.MinGroupSize
}

// SetMinGroupSize устанавливает минимальный размер группы
func (g *GroupGoal) SetMinGroupSize(size int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.MinGroupSize = size
}

// GetMaxGroupSize возвращает максимальный размер группы
func (g *GroupGoal) GetMaxGroupSize() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.MaxGroupSize
}

// SetMaxGroupSize устанавливает максимальный размер группы
func (g *GroupGoal) SetMaxGroupSize(size int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.MaxGroupSize = size
}

// GetMaxGroups возвращает максимальное количество групп или nil, если не ограничено
func (g *GroupGoal) GetMaxGroups() *int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.MaxGroups == nil {
		return nil
	}
	result := *g.MaxGroups
	return &result
}

// SetMaxGroups устанавливает максимальное количество групп
func (g *GroupGoal) SetMaxGroups(maxGroups *int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if maxGroups == nil {
		g.MaxGroups = nil
		return
	}
	value := *maxGroups
	g.MaxGroups = &value
}

// GetDimensionCriteria возвращает копию критериев измерений
func (g *GroupGoal) GetDimensionCriteria() []DimensionCriterion {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]DimensionCriterion, len(g.DimensionCriteria))
	copy(result, g.DimensionCriteria)
	return result
}

// SetDimensionCriteria устанавливает критерии измерений
func (g *GroupGoal) SetDimensionCriteria(criteria []DimensionCriterion) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.DimensionCriteria = make([]DimensionCriterion, len(criteria))
	copy(g.DimensionCriteria, criteria)
}

// AddDimensionCriterion добавляет критерий измерения
func (g *GroupGoal) AddDimensionCriterion(criterion DimensionCriterion) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.DimensionCriteria = append(g.DimensionCriteria, criterion)
}

// GetInactivityTimeout возвращает время в секундах, после которого неактивный участник исключается из группы
func (g *GroupGoal) GetInactivityTimeout() int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.InactivityTimeout
}

// SetInactivityTimeout устанавливает время в секундах, после которого неактивный участник исключается из группы
func (g *GroupGoal) SetInactivityTimeout(timeout int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.InactivityTimeout = timeout
	g.Version++
}

// GetID возвращает идентификатор цели группирования
func (g *GroupGoal) GetID() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ID
}

// GetVersion возвращает текущую версию
func (g *GroupGoal) GetVersion() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Version
}

// IsDeleted возвращает признак удаления
func (g *GroupGoal) IsDeleted() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Deleted
}

// MarkDeleted помечает цель как удаленную и увеличивает версию
func (g *GroupGoal) MarkDeleted() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Deleted = true
	g.Version++
}

// GetHash вычисляет хеш для группировочной цели
func (g *GroupGoal) GetHash() []byte {
	g.mu.RLock()
	defer g.mu.RUnlock()

	h := md5.New()
	h.Write([]byte(g.ID))

	// Хешируем MinGroupSize
	binary.Write(h, binary.LittleEndian, int32(g.MinGroupSize))

	// Хешируем MaxGroupSize
	binary.Write(h, binary.LittleEndian, int32(g.MaxGroupSize))

	// Хешируем MaxGroups, если указан
	if g.MaxGroups != nil {
		h.Write([]byte{1})
		binary.Write(h, binary.LittleEndian, int32(*g.MaxGroups))
	} else {
		h.Write([]byte{0})
	}

	// Хешируем InactivityTimeout
	binary.Write(h, binary.LittleEndian, g.InactivityTimeout)

	// Хешируем критерии измерений
	for _, criterion := range g.DimensionCriteria {
		h.Write([]byte(criterion.Type))
		h.Write([]byte(criterion.Condition))
		for _, value := range criterion.Values {
			binary.Write(h, binary.LittleEndian, value)
		}
	}

	// Хешируем версию
	binary.Write(h, binary.LittleEndian, g.Version)

	// Хешируем признак удаления
	if g.Deleted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return h.Sum(nil)
}

// NewGroupGoal создает новую цель группирования с указанными параметрами
func NewGroupGoal(minSize, maxSize int) *GroupGoal {
	return &GroupGoal{
		ID:                uuid.New().String(),
		MinGroupSize:      minSize,
		MaxGroupSize:      maxSize,
		InactivityTimeout: 0, // По умолчанию отключено
		DimensionCriteria: make([]DimensionCriterion, 0),
		Version:           0,
		Deleted:           false,
	}
}

// NewGroupGoalWithTimeout создает новую цель группирования с указанными параметрами и таймаутом неактивности
func NewGroupGoalWithTimeout(minSize, maxSize int, inactivityTimeout int64) *GroupGoal {
	return &GroupGoal{
		ID:                uuid.New().String(),
		MinGroupSize:      minSize,
		MaxGroupSize:      maxSize,
		InactivityTimeout: inactivityTimeout,
		DimensionCriteria: make([]DimensionCriterion, 0),
		Version:           0,
		Deleted:           false,
	}
}

// ToProto конвертирует GroupGoal в protobuf сообщение
func (g *GroupGoal) ToProto() *p.GroupGoal {
	g.mu.RLock()
	defer g.mu.RUnlock()

	protoGoal := &p.GroupGoal{
		Id:                g.ID,
		MinGroupSize:      int32(g.MinGroupSize),
		MaxGroupSize:      int32(g.MaxGroupSize),
		InactivityTimeout: g.InactivityTimeout,
		Version:           g.Version,
		Deleted:           g.Deleted,
	}

	if g.MaxGroups != nil {
		maxGroups := int32(*g.MaxGroups)
		protoGoal.MaxGroups = &maxGroups
	}

	protoCriteria := make([]*p.DimensionCriterion, len(g.DimensionCriteria))
	for i, criterion := range g.DimensionCriteria {
		protoCriterion := &p.DimensionCriterion{
			Type:      criterion.Type,
			Condition: string(criterion.Condition),
			Values:    criterion.Values,
		}
		protoCriteria[i] = protoCriterion
	}
	protoGoal.DimensionCriteria = protoCriteria

	return protoGoal
}

// Serialize сериализует GroupGoal в байты
func (g *GroupGoal) Serialize() ([]byte, error) {
	return proto.Marshal(g.ToProto())
}

// GroupGoalFromProto создает GroupGoal из protobuf сообщения
func GroupGoalFromProto(p *p.GroupGoal) *GroupGoal {
	goal := &GroupGoal{
		ID:                p.Id,
		MinGroupSize:      int(p.MinGroupSize),
		MaxGroupSize:      int(p.MaxGroupSize),
		InactivityTimeout: p.InactivityTimeout,
		DimensionCriteria: make([]DimensionCriterion, len(p.DimensionCriteria)),
		Version:           p.Version,
		Deleted:           p.Deleted,
	}

	if p.MaxGroups != nil {
		maxGroups := int(*p.MaxGroups)
		goal.MaxGroups = &maxGroups
	}

	for i, protoCriterion := range p.DimensionCriteria {
		criterion := DimensionCriterion{
			Type:      protoCriterion.Type,
			Condition: DimensionConditionType(protoCriterion.Condition),
			Values:    protoCriterion.Values,
		}
		goal.DimensionCriteria[i] = criterion
	}

	return goal
}

// GroupGoalFromProtoBytes десериализует GroupGoal из байтов
func GroupGoalFromProtoBytes(data []byte) (*GroupGoal, error) {
	protoGoal := &p.GroupGoal{}
	err := proto.Unmarshal(data, protoGoal)
	if err != nil {
		return nil, err
	}
	return GroupGoalFromProto(protoGoal), nil
}
