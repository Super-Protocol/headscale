package common

import (
	"fmt"
	"sync"
)

type MemoryEntityRegistry struct {
	entities        map[string]map[string]Entity // map[entityType]map[entityID]Entity
	deletedEntities map[string]map[string]bool
	mu              sync.RWMutex
}

func NewMemoryEntityRegistry() *MemoryEntityRegistry {
	return &MemoryEntityRegistry{
		entities:        make(map[string]map[string]Entity),
		deletedEntities: make(map[string]map[string]bool),
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

func (r *MemoryEntityRegistry) StoreEntity(entityType string, entity Entity) error {
	if entity == nil {
		return fmt.Errorf("cannot save nil entity")
	}

	entityID := entity.GetID()

	r.mu.Lock()
	defer r.mu.Unlock()

	if typeMap, exists := r.entities[entityType]; exists {
		if existingEntity, found := typeMap[entityID]; found {

			// If deleted do nothing
			if deletedMap, exists := r.deletedEntities[entityType]; exists {
				if _, exists := deletedMap[entityID]; exists {
					return nil
				}
			}

			if entity.GetVersion() <= existingEntity.GetVersion() {
				return nil
			}
		}
	} else {
		r.entities[entityType] = make(map[string]Entity)
	}

	r.entities[entityType][entityID] = entity
	return nil
}

func (r *MemoryEntityRegistry) DeleteEntity(entityType string, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if typeMap, exists := r.entities[entityType]; exists {
		if _, found := typeMap[id]; found {

			if _, exists := r.deletedEntities[entityType]; !exists {
				r.deletedEntities[entityType] = make(map[string]bool)
			}

			r.deletedEntities[entityType][id] = true

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
