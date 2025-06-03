package entities

import "sync"

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
	Type string `json:"type"`
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
	// DimensionCriteria критерии включения нод в группу
	DimensionCriteria []DimensionCriterion
	// мьютекс для безопасного доступа к полям
	mu sync.RWMutex
}

// GetID возвращает идентификатор цели
func (g *GroupGoal) GetID() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ID
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

// NewGroupGoal создает новую цель группирования с указанными параметрами
func NewGroupGoal(id string, minSize, maxSize int) *GroupGoal {
	return &GroupGoal{
		ID:                id,
		MinGroupSize:      minSize,
		MaxGroupSize:      maxSize,
		DimensionCriteria: make([]DimensionCriterion, 0),
	}
}
