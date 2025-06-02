package grouping

type Grouping interface {
	Start() error
	Stop() error
}
