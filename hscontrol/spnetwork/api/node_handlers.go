package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
	"strconv"
)

// registerNodeRoutes регистрирует маршруты для сущности Node
func (s *Server) registerNodeRoutes(r *mux.Router) {
	r.HandleFunc("/api/nodes", s.getAllNodes).Methods("GET")
	r.HandleFunc("/api/nodes/{id}", s.getNode).Methods("GET")
	r.HandleFunc("/api/nodes", s.createNode).Methods("POST")
	r.HandleFunc("/api/nodes/{id}", s.updateNode).Methods("PUT")
	r.HandleFunc("/api/nodes/{id}", s.deleteNode).Methods("DELETE")
}

// NodeResponse представляет ответ с данными узла
type NodeResponse struct {
	ID         string            `json:"id"`
	Properties map[string]string `json:"properties"`
	Version    uint64            `json:"version"`
	Deleted    bool              `json:"deleted"`
}

// NodeRequest представляет запрос на создание/обновление узла
type NodeRequest struct {
	Properties map[string]string `json:"properties"`
}

// getAllNodes возвращает все узлы из реестра
func (s *Server) getAllNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.EntityRegistry.Node.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Error getting all nodes")
		sendError(w, http.StatusInternalServerError, "Error getting nodes")
		return
	}

	var response []NodeResponse
	for _, node := range nodes {
		if !node.IsDeleted() {
			protoNode := node.ToProto()
			response = append(response, NodeResponse{
				ID:         node.GetID(),
				Properties: protoNode.Properties,
				Version:    node.GetVersion(),
				Deleted:    node.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getNode возвращает узел по его ID
func (s *Server) getNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	node, err := s.EntityRegistry.Node.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting node")
		sendError(w, http.StatusNotFound, "Node not found")
		return
	}

	if node.IsDeleted() {
		sendError(w, http.StatusNotFound, "Node is deleted")
		return
	}

	protoNode := node.ToProto()
	response := NodeResponse{
		ID:         node.GetID(),
		Properties: protoNode.Properties,
		Version:    node.GetVersion(),
		Deleted:    node.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// createNode создает новый узел
func (s *Server) createNode(w http.ResponseWriter, r *http.Request) {
	var request NodeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	node := entities.NewNode()

	// Устанавливаем свойства из запроса
	for key, value := range request.Properties {
		switch key {
		case "host":
			node.SetHost(value)
		case "gossip_port":
			port, err := strconv.ParseUint(value, 10, 16)
			if err == nil {
				node.SetGossipPort(uint16(port))
			}
		case "udp_ping_port":
			port, err := strconv.ParseUint(value, 10, 16)
			if err == nil {
				node.SetUdpPingPort(uint16(port))
			}
		}
	}

	// Сохраняем узел в реестре
	_, err := s.EntityRegistry.Node.StoreEntity(node)
	if err != nil {
		log.Error().Err(err).Msg("Error storing node")
		sendError(w, http.StatusInternalServerError, "Error creating node")
		return
	}

	protoNode := node.ToProto()
	response := NodeResponse{
		ID:         node.GetID(),
		Properties: protoNode.Properties,
		Version:    node.GetVersion(),
		Deleted:    node.IsDeleted(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateNode обновляет узел по его ID
func (s *Server) updateNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request NodeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	node, err := s.EntityRegistry.Node.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting node")
		sendError(w, http.StatusNotFound, "Node not found")
		return
	}

	if node.IsDeleted() {
		sendError(w, http.StatusNotFound, "Node is deleted")
		return
	}

	// Обновляем свойства из запроса
	for key, value := range request.Properties {
		switch key {
		case "host":
			node.SetHost(value)
		case "gossip_port":
			port, err := strconv.ParseUint(value, 10, 16)
			if err == nil {
				node.SetGossipPort(uint16(port))
			}
		case "udp_ping_port":
			port, err := strconv.ParseUint(value, 10, 16)
			if err == nil {
				node.SetUdpPingPort(uint16(port))
			}
		}
	}

	// Сохраняем обновленный узел в реестре
	_, err = s.EntityRegistry.Node.StoreEntity(node)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated node")
		sendError(w, http.StatusInternalServerError, "Error updating node")
		return
	}

	protoNode := node.ToProto()
	response := NodeResponse{
		ID:         node.GetID(),
		Properties: protoNode.Properties,
		Version:    node.GetVersion(),
		Deleted:    node.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteNode помечает узел как удаленный
func (s *Server) deleteNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	err := s.EntityRegistry.Node.DeleteEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error deleting node")
		sendError(w, http.StatusInternalServerError, "Error deleting node")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
