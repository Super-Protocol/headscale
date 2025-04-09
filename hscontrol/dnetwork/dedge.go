package dnetwork

import (
	"gonum.org/v1/gonum/graph"
	"sync"
)

type Measurement struct {
	Value              int64  `json:"value"`
	CreationTimeUnix   uint64 `json:"creation_time_unix"`
	ExpirationTimeUnix uint64 `json:"expiration_time_unix"`
}

type DEdge struct {
	from           DNode
	to             DNode
	measurements   map[string]Measurement
	measurementsMu sync.RWMutex
}

func (e *DEdge) ReversedEdge() graph.Edge {

	return &DEdge{
		from:         e.to,
		to:           e.from,
		measurements: e.measurements,
	}
}

func NewDEdge(from, to DNode) *DEdge {
	return &DEdge{
		from:         from,
		to:           to,
		measurements: make(map[string]Measurement),
	}
}

func (e *DEdge) GetMeasurement(name string) (Measurement, bool) {
	e.measurementsMu.RLock()
	defer e.measurementsMu.RUnlock()
	val, ok := e.measurements[name]
	return val, ok
}

func (e *DEdge) SetMeasurement(name string, m Measurement) {
	e.measurementsMu.Lock()
	defer e.measurementsMu.Unlock()
	e.measurements[name] = m
}

func (e *DEdge) DeleteMeasurement(name string) {
	e.measurementsMu.Lock()
	defer e.measurementsMu.Unlock()
	delete(e.measurements, name)
}

func (e *DEdge) From() graph.Node { return e.from }
func (e *DEdge) To() graph.Node   { return e.to }
