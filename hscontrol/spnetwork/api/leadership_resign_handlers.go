package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
)

// registerLeadershipResignRoutes регистрирует маршруты для сущности LeadershipResign
func (s *Server) registerLeadershipResignRoutes(r *mux.Router) {
	r.HandleFunc("/api/leadershipresigns", s.getAllLeadershipResigns).Methods("GET")
	r.HandleFunc("/api/leadershipresigns/{id}", s.getLeadershipResign).Methods("GET")
	r.HandleFunc("/api/leadershipresigns/owner/{owner}", s.getLeadershipResignsByOwner).Methods("GET")
	r.HandleFunc("/api/leadershipresigns/voterequest/{voteRequestId}", s.getLeadershipResignsByVoteRequest).Methods("GET")
	r.HandleFunc("/api/leadershipresigns", s.createLeadershipResign).Methods("POST")
	r.HandleFunc("/api/leadershipresigns/{id}", s.deleteLeadershipResign).Methods("DELETE")
}

// LeadershipResignResponse представляет ответ с данными отказа от лидерства
type LeadershipResignResponse struct {
	ID            string `json:"id"`
	Owner         string `json:"owner"`
	VoteRequestID string `json:"voteRequestId"`
	DateUnix      int64  `json:"dateUnix"`
	Version       uint64 `json:"version"`
	Deleted       bool   `json:"deleted"`
}

// LeadershipResignRequest представляет запрос на создание отказа от лидерства
type LeadershipResignRequest struct {
	Owner         string `json:"owner"`
	VoteRequestID string `json:"voteRequestId"`
}

// getAllLeadershipResigns возвращает все отказы от лидерства из реестра
func (s *Server) getAllLeadershipResigns(w http.ResponseWriter, r *http.Request) {
	resigns, err := s.EntityRegistry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех отказов от лидерства")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении отказов от лидерства")
		return
	}

	var response []LeadershipResignResponse
	for _, resign := range resigns {
		if !resign.IsDeleted() {
			response = append(response, LeadershipResignResponse{
				ID:            resign.GetID(),
				Owner:         resign.GetOwner(),
				VoteRequestID: resign.GetVoteRequestID(),
				DateUnix:      resign.GetDateUnix(),
				Version:       resign.GetVersion(),
				Deleted:       resign.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getLeadershipResign возвращает отказ от лидерства по его ID
func (s *Server) getLeadershipResign(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	resign, err := s.EntityRegistry.LeadershipResign.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при получении отказа от лидерства")
		sendError(w, http.StatusNotFound, "Отказ от лидерства не найден")
		return
	}

	if resign.IsDeleted() {
		sendError(w, http.StatusNotFound, "Отказ от лидерства удален")
		return
	}

	response := LeadershipResignResponse{
		ID:            resign.GetID(),
		Owner:         resign.GetOwner(),
		VoteRequestID: resign.GetVoteRequestID(),
		DateUnix:      resign.GetDateUnix(),
		Version:       resign.GetVersion(),
		Deleted:       resign.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// getLeadershipResignsByOwner возвращает отказы от лидерства по владельцу
func (s *Server) getLeadershipResignsByOwner(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	owner := vars["owner"]

	resigns, err := s.EntityRegistry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех отказов от лидерства")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении отказов от лидерства")
		return
	}

	var response []LeadershipResignResponse
	for _, resign := range resigns {
		if !resign.IsDeleted() && resign.GetOwner() == owner {
			response = append(response, LeadershipResignResponse{
				ID:            resign.GetID(),
				Owner:         resign.GetOwner(),
				VoteRequestID: resign.GetVoteRequestID(),
				DateUnix:      resign.GetDateUnix(),
				Version:       resign.GetVersion(),
				Deleted:       resign.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getLeadershipResignsByVoteRequest возвращает отказы от лидерства по ID запроса на голосование
func (s *Server) getLeadershipResignsByVoteRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	voteRequestId := vars["voteRequestId"]

	resigns, err := s.EntityRegistry.LeadershipResign.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех отказов от лидерства")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении отказов от лидерства")
		return
	}

	var response []LeadershipResignResponse
	for _, resign := range resigns {
		if !resign.IsDeleted() && resign.GetVoteRequestID() == voteRequestId {
			response = append(response, LeadershipResignResponse{
				ID:            resign.GetID(),
				Owner:         resign.GetOwner(),
				VoteRequestID: resign.GetVoteRequestID(),
				DateUnix:      resign.GetDateUnix(),
				Version:       resign.GetVersion(),
				Deleted:       resign.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// createLeadershipResign создает новый отказ от лидерства
func (s *Server) createLeadershipResign(w http.ResponseWriter, r *http.Request) {
	var request LeadershipResignRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	// Проверяем, что owner и voteRequestID указаны
	if request.Owner == "" || request.VoteRequestID == "" {
		sendError(w, http.StatusBadRequest, "Необходимо указать владельца и ID запроса на голосование")
		return
	}

	// Проверяем существование ноды-владельца
	_, err := s.EntityRegistry.Node.GetEntity(request.Owner)
	if err != nil {
		log.Error().Err(err).Str("owner_id", request.Owner).Msg("Нода-владелец не найдена")
		sendError(w, http.StatusBadRequest, "Нода-владелец не найдена")
		return
	}

	// Проверяем существование запроса на голосование
	voteRequest, err := s.EntityRegistry.VoteRequest.GetEntity(request.VoteRequestID)
	if err != nil {
		log.Error().Err(err).Str("vote_request_id", request.VoteRequestID).Msg("Запрос на голосование не найден")
		sendError(w, http.StatusBadRequest, "Запрос на голосование не найден")
		return
	}

	if voteRequest.IsDeleted() {
		sendError(w, http.StatusBadRequest, "Запрос на голосование удален")
		return
	}

	// Создаем новый отказ от лидерства
	resign := entities.NewLeadershipResign(request.Owner, request.VoteRequestID)

	// Сохраняем отказ от лидерства в реестре
	_, err = s.EntityRegistry.LeadershipResign.StoreEntity(resign)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при сохранении отказа от лидерства")
		sendError(w, http.StatusInternalServerError, "Ошибка при создании отказа от лидерства")
		return
	}

	response := LeadershipResignResponse{
		ID:            resign.GetID(),
		Owner:         resign.GetOwner(),
		VoteRequestID: resign.GetVoteRequestID(),
		DateUnix:      resign.GetDateUnix(),
		Version:       resign.GetVersion(),
		Deleted:       resign.IsDeleted(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// deleteLeadershipResign помечает отказ от лидерства как удаленный
func (s *Server) deleteLeadershipResign(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	err := s.EntityRegistry.LeadershipResign.DeleteEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при удалении отказа от лидерства")
		sendError(w, http.StatusInternalServerError, "Ошибка при удалении отказа от лидерства")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "удален"})
}
