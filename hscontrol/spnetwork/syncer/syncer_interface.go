package syncer

type Syncer interface {
	Start() error
	Stop() error
}
