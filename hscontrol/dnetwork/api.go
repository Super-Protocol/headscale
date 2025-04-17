package dnetwork

// This file contains API types and documentation for the DNetwork server
// with OpenAPI/Swagger annotations

// @title DNetwork Gossip API
// @version 1.0
// @description API for headscale node gossip and network topology information exchange
// @BasePath /api

// NodeResponse represents a node in the distributed network
// swagger:model NodeResponse
type NodeResponse struct {
	// The host address of the node
	// example: 192.168.1.100
	Host string `json:"host"`

	// The port number the node is listening on
	// example: 8443
	// minimum: 1
	// maximum: 65535
	Port uint16 `json:"port"`

	// When the node was last seen available
	// example: 2023-06-15T14:22:33Z
	// format: date-time
	LastAvailableAt string `json:"last_available_at"`
}

// MeasurementResponse represents a measurement between two nodes
// swagger:model MeasurementResponse
type MeasurementResponse struct {
	// The measurement value
	// example: 42
	Value int64 `json:"value"`

	// Unix timestamp when the measurement was created
	// example: 1686837753
	CreationTimeUnix uint64 `json:"creation_time_unix"`

	// Unix timestamp when the measurement expires
	// example: 1686924153
	ExpirationTimeUnix uint64 `json:"expiration_time_unix"`
}

// GossipResponse is the response for the get_gossip API endpoint
// swagger:response GossipResponse
type GossipResponseDoc struct {
	// in: body
	Body struct {
		// List of nodes in the network
		// required: true
		Nodes []NodeResponse `json:"nodes"`

		// Criteria between nodes, indexed by from_node_id -> to_node_id -> measurement_name
		// required: true
		Measurements map[string]map[string]map[string]MeasurementResponse `json:"measurements"`
	}
}

// swagger:route POST /api/gossip/spread gossip spreadGossip
// Receives and processes gossip information from other nodes
// responses:
//   200: SpreadGossipResponse
//   500: ErrorResponse

// ErrorResponse is the error response for API endpoints
// swagger:response ErrorResponse
type ErrorResponse struct {
	// in: body
	Body struct {
		// Error message
		// example: Internal server error
		Error string `json:"error"`
	}
}

// SpreadGossipResponse is the response for the gossip/spread API endpoint
// swagger:response SpreadGossipResponse
type SpreadGossipResponseDoc struct {
	// in: body
	Body struct {
		// Status message
		// example: success
		Status string `json:"status"`
	}
}
