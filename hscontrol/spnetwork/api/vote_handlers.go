package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

// registerVoteRoutes регистрирует маршруты для сущности Vote
func (s *Server) registerVoteRoutes(r *mux.Router) {
	r.HandleFunc("/api/votes", s.getAllVotes).Methods("GET")
	r.HandleFunc("/api/votes/{id}", s.getVote).Methods("GET")
	r.HandleFunc("/api/votes/request/{requestId}", s.getVotesForRequest).Methods("GET")
	r.HandleFunc("/api/votes", s.createVote).Methods("POST")
	r.HandleFunc("/api/votes/{id}", s.updateVote).Methods("PUT")
	r.HandleFunc("/api/votes/{id}", s.deleteVote).Methods("DELETE")
}

// VoteResponse представляет ответ с данными голоса
type VoteResponse struct {
	ID        string `json:"id"`
	RequestID string `json:"requestId"`
	Kind      string `json:"kind"`
	Target    string `json:"target"`
	Value     int    `json:"value"`
	DateUnix  int64  `json:"dateUnix"`
	Version   uint64 `json:"version"`
	Deleted   bool   `json:"deleted"`
	Voter     string `json:"voter"`
}

// VoteRequest представляет запрос на создание/обновление голоса
type VoteRequest struct {
	RequestID string `json:"requestId"`
	Kind      string `json:"kind,omitempty"`
	Target    string `json:"target,omitempty"`
	Value     int    `json:"value"`
	Voter     string `json:"voter"`
}

// getAllVotes возвращает все голоса из реестра
func (s *Server) getAllVotes(w http.ResponseWriter, r *http.Request) {
	votes, err := s.EntityRegistry.Vote.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех голосов")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении голосов")
		return
	}

	var response []VoteResponse
	for _, vote := range votes {
		if !vote.IsDeleted() {
			response = append(response, VoteResponse{
				ID:        vote.GetID(),
				RequestID: vote.GetRequestID(),
				Kind:      vote.GetKind(),
				Target:    vote.GetTarget(),
				Value:     vote.GetValue(),
				DateUnix:  vote.GetDateUnix(),
				Version:   vote.GetVersion(),
				Deleted:   vote.IsDeleted(),
				Voter:     vote.GetVoter(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getVote возвращает голос по его ID
func (s *Server) getVote(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	vote, err := s.EntityRegistry.Vote.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при получении голоса")
		sendError(w, http.StatusNotFound, "Голос не найден")
		return
	}

	if vote.IsDeleted() {
		sendError(w, http.StatusNotFound, "Голос удален")
		return
	}

	response := VoteResponse{
		ID:        vote.GetID(),
		RequestID: vote.GetRequestID(),
		Kind:      vote.GetKind(),
		Target:    vote.GetTarget(),
		Value:     vote.GetValue(),
		DateUnix:  vote.GetDateUnix(),
		Version:   vote.GetVersion(),
		Deleted:   vote.IsDeleted(),
		Voter:     vote.GetVoter(),
	}

	sendJSON(w, http.StatusOK, response)
}

// getVotesForRequest возвращает все голоса для указанного запроса
func (s *Server) getVotesForRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	requestId := vars["requestId"]

	votes, err := s.EntityRegistry.Vote.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех голосов")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении голосов")
		return
	}

	var response []VoteResponse
	for _, vote := range votes {
		if !vote.IsDeleted() && vote.GetRequestID() == requestId {
			response = append(response, VoteResponse{
				ID:        vote.GetID(),
				RequestID: vote.GetRequestID(),
				Kind:      vote.GetKind(),
				Target:    vote.GetTarget(),
				Value:     vote.GetValue(),
				DateUnix:  vote.GetDateUnix(),
				Version:   vote.GetVersion(),
				Deleted:   vote.IsDeleted(),
				Voter:     vote.GetVoter(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// createVote создает новый голос
func (s *Server) createVote(w http.ResponseWriter, r *http.Request) {
	var request VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	// Проверяем наличие requestId
	if request.RequestID == "" {
		sendError(w, http.StatusBadRequest, "Не указан ID запроса на голосование")
		return
	}

	// Получаем связанный запрос на голосование
	voteRequest, err := s.EntityRegistry.VoteRequest.GetEntity(request.RequestID)
	if err != nil {
		log.Error().Err(err).Str("request_id", request.RequestID).Msg("Запрос на голосование не найден")
		sendError(w, http.StatusBadRequest, "Запрос на голосование не найден")
		return
	}

	// Проверяем, что запрос на голосование не удален
	if voteRequest.IsDeleted() {
		sendError(w, http.StatusBadRequest, "Запрос на голосование удален")
		return
	}

	// Проверяем, что запрос на голосование не истек
	if !voteRequest.IsActive(time.Now().Unix()) {
		sendError(w, http.StatusBadRequest, "Запрос на голосование истек")
		return
	}

	// Проверяем, что указана голосующая нода
	if request.Voter == "" {
		sendError(w, http.StatusBadRequest, "Не указана голосующая нода")
		return
	}

	// Проверяем существование голосующей ноды
	_, err = s.EntityRegistry.Node.GetEntity(request.Voter)
	if err != nil {
		log.Error().Err(err).Str("voter_id", request.Voter).Msg("Голосующая нода не найдена")
		sendError(w, http.StatusBadRequest, "Голосующая нода не найдена")
		return
	}

	// Проверяем, не голосовала ли уже эта нода
	if s.EntityRegistry.HasVoteFromNodeForRequest(request.RequestID, request.Voter) {
		sendError(w, http.StatusBadRequest, "Эта нода уже голосовала за данный запрос")
		return
	}

	// Создаем новый голос
	vote := entities.NewVoteForRequest(voteRequest, request.Value, request.Voter)

	// Сохраняем голос в реестре
	_, err = s.EntityRegistry.Vote.StoreEntity(vote)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при сохранении голоса")
		sendError(w, http.StatusInternalServerError, "Ошибка при создании голоса")
		return
	}

	response := VoteResponse{
		ID:        vote.GetID(),
		RequestID: vote.GetRequestID(),
		Kind:      vote.GetKind(),
		Target:    vote.GetTarget(),
		Value:     vote.GetValue(),
		DateUnix:  vote.GetDateUnix(),
		Version:   vote.GetVersion(),
		Deleted:   vote.IsDeleted(),
		Voter:     vote.GetVoter(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateVote обновляет голос по его ID
func (s *Server) updateVote(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	vote, err := s.EntityRegistry.Vote.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при получении голоса")
		sendError(w, http.StatusNotFound, "Голос не найден")
		return
	}

	if vote.IsDeleted() {
		sendError(w, http.StatusNotFound, "Голос удален")
		return
	}

	// Обновляем только значение голоса
	// Создаем новый голос с теми же параметрами, но новым значением
	newVote := entities.NewVote(entities.VoteKind(vote.GetKind()), vote.GetTarget(), request.Value, vote.GetVoter())
	newVote.SetRequestID(vote.GetRequestID())

	// Сохраняем обновленный голос в реестре
	_, err = s.EntityRegistry.Vote.StoreEntity(newVote)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при сохранении обновленного голоса")
		sendError(w, http.StatusInternalServerError, "Ошибка при обновлении голоса")
		return
	}

	response := VoteResponse{
		ID:        newVote.GetID(),
		RequestID: newVote.GetRequestID(),
		Kind:      newVote.GetKind(),
		Target:    newVote.GetTarget(),
		Value:     newVote.GetValue(),
		DateUnix:  newVote.GetDateUnix(),
		Version:   newVote.GetVersion(),
		Deleted:   newVote.IsDeleted(),
		Voter:     newVote.GetVoter(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteVote помечает голос как удаленный
func (s *Server) deleteVote(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	err := s.EntityRegistry.Vote.DeleteEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при удалении голоса")
		sendError(w, http.StatusInternalServerError, "Ошибка при удалении голоса")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "удален"})
}
