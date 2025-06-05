package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
)

// registerGroupRoutes регистрирует маршруты для сущности Group
func (s *Server) registerGroupRoutes(r *mux.Router) {
	r.HandleFunc("/api/groups", s.getAllGroups).Methods("GET")
	r.HandleFunc("/api/groups/{id}", s.getGroup).Methods("GET")
	r.HandleFunc("/api/groups", s.createGroup).Methods("POST")
	r.HandleFunc("/api/groups/{id}", s.updateGroup).Methods("PUT")
	r.HandleFunc("/api/groups/{id}", s.deleteGroup).Methods("DELETE")
	r.HandleFunc("/api/groups/{id}/participants", s.addParticipant).Methods("POST")
	r.HandleFunc("/api/groups/{id}/participants/{participantId}", s.removeParticipant).Methods("DELETE")
}

// ParticipantResponse представляет участника группы в ответе
type ParticipantResponse struct {
	ID           string `json:"id"`
	JoinDateUnix int64  `json:"joinDateUnix"`
}

// GroupResponse представляет ответ с данными группы
type GroupResponse struct {
	ID               string                `json:"id"`
	Goal             string                `json:"goal"`
	Participants     []ParticipantResponse `json:"participants"`
	Version          uint64                `json:"version"`
	Deleted          bool                  `json:"deleted"`
	CreationDateUnix int64                 `json:"creationDateUnix"`
}

// GroupRequest представляет запрос на создание/обновление группы
type GroupRequest struct {
	Goal string `json:"goal"`
}

// ParticipantRequest представляет запрос на добавление участника
type ParticipantRequest struct {
	ParticipantID string `json:"participantId"`
}

// getAllGroups возвращает все группы из реестра
func (s *Server) getAllGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.EntityRegistry.Group.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Error getting all groups")
		sendError(w, http.StatusInternalServerError, "Error getting groups")
		return
	}

	var response []GroupResponse
	for _, group := range groups {
		if !group.IsDeleted() {
			participants := make([]ParticipantResponse, 0)
			for _, p := range group.GetParticipants() {
				participants = append(participants, ParticipantResponse{
					ID:           p.ID,
					JoinDateUnix: p.JoinDateUnix,
				})
			}

			response = append(response, GroupResponse{
				ID:               group.GetID(),
				Goal:             group.GetGoal(),
				Participants:     participants,
				Version:          group.GetVersion(),
				Deleted:          group.IsDeleted(),
				CreationDateUnix: group.GetCreationDate(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getGroup возвращает группу по её ID
func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	group, err := s.EntityRegistry.Group.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group")
		sendError(w, http.StatusNotFound, "Group not found")
		return
	}

	if group.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group is deleted")
		return
	}

	participants := make([]ParticipantResponse, 0)
	for _, p := range group.GetParticipants() {
		participants = append(participants, ParticipantResponse{
			ID:           p.ID,
			JoinDateUnix: p.JoinDateUnix,
		})
	}

	response := GroupResponse{
		ID:               group.GetID(),
		Goal:             group.GetGoal(),
		Participants:     participants,
		Version:          group.GetVersion(),
		Deleted:          group.IsDeleted(),
		CreationDateUnix: group.GetCreationDate(),
	}

	sendJSON(w, http.StatusOK, response)
}

// createGroup создает новую группу
func (s *Server) createGroup(w http.ResponseWriter, r *http.Request) {
	var request GroupRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	group := entities.NewGroup()
	group.SetGoal(request.Goal)

	// Сохраняем группу в реестре
	_, err := s.EntityRegistry.Group.StoreEntity(group)
	if err != nil {
		log.Error().Err(err).Msg("Error storing group")
		sendError(w, http.StatusInternalServerError, "Error creating group")
		return
	}

	response := GroupResponse{
		ID:               group.GetID(),
		Goal:             group.GetGoal(),
		Participants:     []ParticipantResponse{},
		Version:          group.GetVersion(),
		Deleted:          group.IsDeleted(),
		CreationDateUnix: group.GetCreationDate(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateGroup обновляет группу по её ID
func (s *Server) updateGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request GroupRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	group, err := s.EntityRegistry.Group.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group")
		sendError(w, http.StatusNotFound, "Group not found")
		return
	}

	if group.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group is deleted")
		return
	}

	// Обновляем цель группы
	group.SetGoal(request.Goal)

	// Сохраняем обновленную группу в реестре
	_, err = s.EntityRegistry.Group.StoreEntity(group)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated group")
		sendError(w, http.StatusInternalServerError, "Error updating group")
		return
	}

	participants := make([]ParticipantResponse, 0)
	for _, p := range group.GetParticipants() {
		participants = append(participants, ParticipantResponse{
			ID:           p.ID,
			JoinDateUnix: p.JoinDateUnix,
		})
	}

	response := GroupResponse{
		ID:               group.GetID(),
		Goal:             group.GetGoal(),
		Participants:     participants,
		Version:          group.GetVersion(),
		Deleted:          group.IsDeleted(),
		CreationDateUnix: group.GetCreationDate(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteGroup помечает группу как удаленную
func (s *Server) deleteGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	group, err := s.EntityRegistry.Group.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group")
		sendError(w, http.StatusNotFound, "Group not found")
		return
	}

	// Помечаем группу как удаленную
	group.MarkDeleted()

	// Сохраняем обновленную группу в реестре
	_, err = s.EntityRegistry.Group.StoreEntity(group)
	if err != nil {
		log.Error().Err(err).Msg("Error storing deleted group")
		sendError(w, http.StatusInternalServerError, "Error deleting group")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// addParticipant добавляет участника в группу
func (s *Server) addParticipant(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request ParticipantRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	group, err := s.EntityRegistry.Group.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group")
		sendError(w, http.StatusNotFound, "Group not found")
		return
	}

	if group.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group is deleted")
		return
	}

	// Проверяем, существует ли нода с указанным ID
	_, err = s.EntityRegistry.Node.GetEntity(request.ParticipantID)
	if err != nil {
		log.Error().Err(err).Str("node_id", request.ParticipantID).Msg("Node not found")
		sendError(w, http.StatusBadRequest, "Node not found")
		return
	}

	// Добавляем участника в группу
	group.AddParticipant(request.ParticipantID)

	// Сохраняем обновленную группу в реестре
	_, err = s.EntityRegistry.Group.StoreEntity(group)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated group")
		sendError(w, http.StatusInternalServerError, "Error adding participant")
		return
	}

	participants := make([]ParticipantResponse, 0)
	for _, p := range group.GetParticipants() {
		participants = append(participants, ParticipantResponse{
			ID:           p.ID,
			JoinDateUnix: p.JoinDateUnix,
		})
	}

	response := GroupResponse{
		ID:               group.GetID(),
		Goal:             group.GetGoal(),
		Participants:     participants,
		Version:          group.GetVersion(),
		Deleted:          group.IsDeleted(),
		CreationDateUnix: group.GetCreationDate(),
	}

	sendJSON(w, http.StatusOK, response)
}

// removeParticipant удаляет участника из группы
func (s *Server) removeParticipant(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	participantId := vars["participantId"]

	group, err := s.EntityRegistry.Group.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group")
		sendError(w, http.StatusNotFound, "Group not found")
		return
	}

	if group.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group is deleted")
		return
	}

	// Удаляем участника из группы
	group.RemoveParticipant(participantId)

	// Сохраняем обновленную группу в реестре
	_, err = s.EntityRegistry.Group.StoreEntity(group)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated group")
		sendError(w, http.StatusInternalServerError, "Error removing participant")
		return
	}

	participants := make([]ParticipantResponse, 0)
	for _, p := range group.GetParticipants() {
		participants = append(participants, ParticipantResponse{
			ID:           p.ID,
			JoinDateUnix: p.JoinDateUnix,
		})
	}

	response := GroupResponse{
		ID:               group.GetID(),
		Goal:             group.GetGoal(),
		Participants:     participants,
		Version:          group.GetVersion(),
		Deleted:          group.IsDeleted(),
		CreationDateUnix: group.GetCreationDate(),
	}

	sendJSON(w, http.StatusOK, response)
}
