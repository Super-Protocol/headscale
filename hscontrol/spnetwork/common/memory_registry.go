package common

import (
	"fmt"
	"sync"
)

type MemoryEntityRegistry struct {
	entities map[string]map[string]Entity // map[entityType]map[entityID]Entity
	mu       sync.RWMutex
}

func NewMemoryEntityRegistry() *MemoryEntityRegistry {
	return &MemoryEntityRegistry{
		entities: make(map[string]map[string]Entity),
	}
}

func (r *MemoryEntityRegistry) GetAllEntities() map[string][]Entity {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string][]Entity)

	for entityType, typeStore := range r.entities {
		entities := make([]Entity, 0, len(typeStore))
		for _, entity := range typeStore {
			entities = append(entities, entity)
		}
		result[entityType] = entities
	}

	return result
}

func (r *MemoryEntityRegistry) GetEntity(entityType string, id string) (Entity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if typeMap, exists := r.entities[entityType]; exists {
		entity, found := typeMap[id]
		return entity, found
	}

	return nil, false
}

func (r *MemoryEntityRegistry) StoreEntity(entityType string, entity Entity) (bool, error) {
	if entity == nil {
		return false, fmt.Errorf("cannot save nil entity")
	}

	entityID := entity.GetID()

	r.mu.Lock()
	defer r.mu.Unlock()

	if typeMap, exists := r.entities[entityType]; exists {
		if existingEntity, found := typeMap[entityID]; found {
			if entity.GetVersion() <= existingEntity.GetVersion() {
				return false, nil
			}
		}
	} else {
		r.entities[entityType] = make(map[string]Entity)
	}

	r.entities[entityType][entityID] = entity
	return true, nil
}

func (r *MemoryEntityRegistry) DeleteEntity(entityType string, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if typeMap, exists := r.entities[entityType]; exists {
		if entity, found := typeMap[id]; found {
			// Сначала помечаем сущность как удаленную
			entity.SetDeleted(true)

			// Затем удаляем из мапы
			// Эта сущность останется доступной при вызове GetAllEntitiesByType
			// только если кто-то сохранит ссылку на неё
			delete(typeMap, id)
			return nil
		}
	}

	return fmt.Errorf("entity of type %s with ID %s not found", entityType, id)
}

func (r *MemoryEntityRegistry) GetAllEntitiesByType(entityType string) []Entity {
	r.mu.RLock()
	defer r.mu.RUnlock()

	typeMap, exists := r.entities[entityType]
	if !exists {
		return []Entity{}
	}

	entities := make([]Entity, 0, len(typeMap))

	for _, entity := range typeMap {
		entities = append(entities, entity)
	}

	return entities
}
