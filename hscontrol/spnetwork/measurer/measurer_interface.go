package measurer

type Measurer interface {
	Start() error
	Stop() error
}
