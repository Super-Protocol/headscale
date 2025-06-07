package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

// registerVoteRequestRoutes регистрирует маршруты для сущности VoteRequest
func (s *Server) registerVoteRequestRoutes(r *mux.Router) {
	r.HandleFunc("/api/voterequests", s.getAllVoteRequests).Methods("GET")
	r.HandleFunc("/api/voterequests/{id}", s.getVoteRequest).Methods("GET")
	r.HandleFunc("/api/voterequests/kind/{kind}", s.getVoteRequestsByKind).Methods("GET")
	r.HandleFunc("/api/voterequests/target/{target}", s.getVoteRequestsByTarget).Methods("GET")
	r.HandleFunc("/api/voterequests", s.createVoteRequest).Methods("POST")
	r.HandleFunc("/api/voterequests/{id}", s.updateVoteRequest).Methods("PUT")
	r.HandleFunc("/api/voterequests/{id}", s.deleteVoteRequest).Methods("DELETE")
}

// VoteRequestResponse представляет ответ с данными запроса на голосование
type VoteRequestResponse struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Target      string `json:"target"`
	DateUnix    int64  `json:"dateUnix"`
	TimeoutSecs int64  `json:"timeoutSecs"`
	Version     uint64 `json:"version"`
	Deleted     bool   `json:"deleted"`
}

// VoteRequestCreateRequest представляет запрос на создание запроса на голосование
type VoteRequestCreateRequest struct {
	Kind        string `json:"kind"`
	Target      string `json:"target"`
	TimeoutSecs int64  `json:"timeoutSecs,omitempty"`
}

// VoteRequestUpdateRequest представляет запрос на обновление запроса на голосование
type VoteRequestUpdateRequest struct {
	TimeoutSecs int64 `json:"timeoutSecs"`
}

// getAllVoteRequests возвращает все запросы на голосование из реестра
func (s *Server) getAllVoteRequests(w http.ResponseWriter, r *http.Request) {
	voteRequests, err := s.EntityRegistry.VoteRequest.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех запросов на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении запросов на голосование")
		return
	}

	var response []VoteRequestResponse
	for _, voteRequest := range voteRequests {
		if !voteRequest.IsDeleted() {
			response = append(response, VoteRequestResponse{
				ID:          voteRequest.GetID(),
				Kind:        string(voteRequest.GetKind()),
				Target:      voteRequest.GetTarget(),
				DateUnix:    voteRequest.GetDateUnix(),
				TimeoutSecs: voteRequest.GetTimeoutSecs(),
				Version:     voteRequest.GetVersion(),
				Deleted:     voteRequest.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getVoteRequest возвращает запрос на голосование по его ID
func (s *Server) getVoteRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	voteRequest, err := s.EntityRegistry.VoteRequest.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при получении запроса на голосование")
		sendError(w, http.StatusNotFound, "Запрос на голосование не найден")
		return
	}

	if voteRequest.IsDeleted() {
		sendError(w, http.StatusNotFound, "Запрос на голосование удален")
		return
	}

	response := VoteRequestResponse{
		ID:          voteRequest.GetID(),
		Kind:        string(voteRequest.GetKind()),
		Target:      voteRequest.GetTarget(),
		DateUnix:    voteRequest.GetDateUnix(),
		TimeoutSecs: voteRequest.GetTimeoutSecs(),
		Version:     voteRequest.GetVersion(),
		Deleted:     voteRequest.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// getVoteRequestsByKind возвращает запросы на голосование по типу
func (s *Server) getVoteRequestsByKind(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	kind := vars["kind"]

	voteRequests, err := s.EntityRegistry.VoteRequest.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех запросов на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении запросов на голосование")
		return
	}

	var response []VoteRequestResponse
	for _, voteRequest := range voteRequests {
		if !voteRequest.IsDeleted() && string(voteRequest.GetKind()) == kind {
			response = append(response, VoteRequestResponse{
				ID:          voteRequest.GetID(),
				Kind:        string(voteRequest.GetKind()),
				Target:      voteRequest.GetTarget(),
				DateUnix:    voteRequest.GetDateUnix(),
				TimeoutSecs: voteRequest.GetTimeoutSecs(),
				Version:     voteRequest.GetVersion(),
				Deleted:     voteRequest.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getVoteRequestsByTarget возвращает запросы на голосование по цели
func (s *Server) getVoteRequestsByTarget(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	target := vars["target"]

	voteRequests, err := s.EntityRegistry.VoteRequest.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при получении всех запросов на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при получении запросов на голосование")
		return
	}

	var response []VoteRequestResponse
	for _, voteRequest := range voteRequests {
		if !voteRequest.IsDeleted() && voteRequest.GetTarget() == target {
			response = append(response, VoteRequestResponse{
				ID:          voteRequest.GetID(),
				Kind:        string(voteRequest.GetKind()),
				Target:      voteRequest.GetTarget(),
				DateUnix:    voteRequest.GetDateUnix(),
				TimeoutSecs: voteRequest.GetTimeoutSecs(),
				Version:     voteRequest.GetVersion(),
				Deleted:     voteRequest.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// createVoteRequest создает новый запрос на голосование
func (s *Server) createVoteRequest(w http.ResponseWriter, r *http.Request) {
	var request VoteRequestCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	// Проверяем тип запроса на голосование
	if request.Kind != string(entities.VoteKindNetworkLeadership) {
		sendError(w, http.StatusBadRequest, "Неверный тип запроса на голосование")
		return
	}

	// Проверяем, существует ли нода с указанным ID
	_, err := s.EntityRegistry.Node.GetEntity(request.Target)
	if err != nil {
		log.Error().Err(err).Str("target_id", request.Target).Msg("Целевая нода не найдена")
		sendError(w, http.StatusBadRequest, "Целевая нода не найдена")
		return
	}

	var voteRequest *entities.VoteRequest
	now := time.Now().Unix()

	// Создаем новый запрос на голосование
	if request.TimeoutSecs > 0 {
		voteRequest = entities.NewVoteRequestWithTimeout(entities.VoteKind(request.Kind), request.Target, now, request.TimeoutSecs)
	} else {
		voteRequest = entities.NewVoteRequest(entities.VoteKind(request.Kind), request.Target, now)
	}

	// Сохраняем запрос на голосование в реестре
	_, err = s.EntityRegistry.VoteRequest.StoreEntity(voteRequest)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при сохранении запроса на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при создании запроса на голосование")
		return
	}

	response := VoteRequestResponse{
		ID:          voteRequest.GetID(),
		Kind:        string(voteRequest.GetKind()),
		Target:      voteRequest.GetTarget(),
		DateUnix:    voteRequest.GetDateUnix(),
		TimeoutSecs: voteRequest.GetTimeoutSecs(),
		Version:     voteRequest.GetVersion(),
		Deleted:     voteRequest.IsDeleted(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateVoteRequest обновляет запрос на голосование по его ID
func (s *Server) updateVoteRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request VoteRequestUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	voteRequest, err := s.EntityRegistry.VoteRequest.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при получении запроса на голосование")
		sendError(w, http.StatusNotFound, "Запрос на голосование не найден")
		return
	}

	if voteRequest.IsDeleted() {
		sendError(w, http.StatusNotFound, "Запрос на голосование удален")
		return
	}

	// Создаем новый запрос на голосование с обновленным таймаутом
	newVoteRequest := entities.NewVoteRequestWithTimeout(
		voteRequest.GetKind(),
		voteRequest.GetTarget(),
		voteRequest.GetDateUnix(),
		request.TimeoutSecs,
	)

	// Сохраняем обновленный запрос на голосование в реестре
	_, err = s.EntityRegistry.VoteRequest.StoreEntity(newVoteRequest)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка при сохранении обновленного запроса на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при обновлении запроса на голосование")
		return
	}

	response := VoteRequestResponse{
		ID:          newVoteRequest.GetID(),
		Kind:        string(newVoteRequest.GetKind()),
		Target:      newVoteRequest.GetTarget(),
		DateUnix:    newVoteRequest.GetDateUnix(),
		TimeoutSecs: newVoteRequest.GetTimeoutSecs(),
		Version:     newVoteRequest.GetVersion(),
		Deleted:     newVoteRequest.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteVoteRequest помечает запрос на голосование как удаленный
func (s *Server) deleteVoteRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	err := s.EntityRegistry.VoteRequest.DeleteEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Ошибка при удалении запроса на голосование")
		sendError(w, http.StatusInternalServerError, "Ошибка при удалении запроса на голосование")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "удален"})
}
