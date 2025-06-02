package consensus

type Consensus interface {
	Join() error
	Leave() error
}
