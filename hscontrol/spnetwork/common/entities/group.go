package entities

import (
	"github.com/google/uuid"
)

type Group struct {
	ID           string
	Name         string
	Owner        string
	MinSize      uint32
	MaxSize      uint32
	Participants []string
	Ready        bool
	Version      uint64
	Deleted      bool
}

func (n Group) NewGroup(name string) *Group {
	return &Group{
		ID:   uuid.New().String(),
		Name: name,
	}
}

func (n Group) NewGroupWithId(id string, name string) *Group {
	return &Group{
		ID:   id,
		Name: name,
	}
}
