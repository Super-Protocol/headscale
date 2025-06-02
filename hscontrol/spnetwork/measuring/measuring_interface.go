package measuring

type Measuring interface {
	Start() error
	Stop() error
}
