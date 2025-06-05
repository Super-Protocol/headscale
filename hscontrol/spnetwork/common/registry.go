package common

type Registry interface {
	StoreEntity(entityType string, entity Entity) (bool, error)
	GetEntity(entityType string, entityID string) (Entity, bool)
	GetAllEntitiesByType(entityType string) []Entity
	GetAllEntities() map[string][]Entity
	DeleteEntity(entityType string, id string) error
}
