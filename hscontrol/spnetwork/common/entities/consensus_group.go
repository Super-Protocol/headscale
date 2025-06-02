package entities

type ConsensusGroup struct {
	ID           string
	Name         string
	Leader       string
	MinSize      uint32
	MaxSize      uint32
	Participants []string
	Ready        bool
	Version      uint64
	Deleted      bool
}
