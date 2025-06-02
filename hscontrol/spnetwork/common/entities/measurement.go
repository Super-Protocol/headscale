package entities

type MeasurementType int

const (
	LatencyClass   MeasurementType = 0
	BandwidthClass                 = 1
)

type Measurement struct {
	Owner    string
	Target   string
	Type     MeasurementType
	Value    float64
	DateUnix int64
	Version  uint64
	Deleted  bool
}

func (m Measurement) NewMeasurement(t MeasurementType, value float64, dateUnix int64) *Measurement {
	return &Measurement{
		Type:     t,
		Value:    value,
		DateUnix: dateUnix,
	}
}
