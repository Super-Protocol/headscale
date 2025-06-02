package common

type Entity interface {
	GetID() string
	GetVersion() uint64
	IsDeleted() bool
	GetHash() []byte
	Serialize() ([]byte, error)
}
