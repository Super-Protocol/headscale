package common

import (
	"fmt"
)

type TypedRegistry[T Entity] struct {
	registry   Registry
	entityType string
}

func NewTypedRegistry[T Entity](registry Registry, entityType string) *TypedRegistry[T] {
	return &TypedRegistry[T]{
		registry:   registry,
		entityType: entityType,
	}
}

func (tr *TypedRegistry[T]) GetEntity(entityID string) (T, error) {
	var zeroValue T

	entity, ok := tr.registry.GetEntity(tr.entityType, entityID)
	if !ok {
		return zeroValue, fmt.Errorf("entity with id %s is not found", entityID)
	}

	typedEntity, ok := entity.(T)
	if !ok {
		return zeroValue, fmt.Errorf("entity with id %s is not of expected type", entityID)
	}

	return typedEntity, nil
}

func (tr *TypedRegistry[T]) GetAllEntities() ([]T, error) {
	entities := tr.registry.GetAllEntitiesByType(tr.entityType)

	typedEntities := make([]T, 0, len(entities))

	for _, entity := range entities {
		typedEntity, ok := entity.(T)
		if !ok {
			continue
		}

		typedEntities = append(typedEntities, typedEntity)
	}

	return typedEntities, nil
}

func (tr *TypedRegistry[T]) StoreEntity(entity T) (bool, error) {
	return tr.registry.StoreEntity(tr.entityType, entity)
}

func (tr *TypedRegistry[T]) DeleteEntity(entityID string) error {
	return tr.registry.DeleteEntity(tr.entityType, entityID)
}
