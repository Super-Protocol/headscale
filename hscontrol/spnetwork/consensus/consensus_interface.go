package consensus

type Consensus interface {
	Start() error
	Stop() error
}

type LeaderSource interface {
	IsLeader() bool
	GetLeaderID() (string, bool)
}
