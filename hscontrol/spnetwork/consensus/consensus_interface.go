package consensus

type Consensus interface {
	Start() error
	Stop() error
	IsLeader() bool
	GetLeaderID() (string, bool)
}
