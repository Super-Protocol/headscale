package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
)

// registerGroupGoalRoutes регистрирует маршруты для сущности GroupGoal
func (s *Server) registerGroupGoalRoutes(r *mux.Router) {
	r.HandleFunc("/api/groupgoals", s.getAllGroupGoals).Methods("GET")
	r.HandleFunc("/api/groupgoals/{id}", s.getGroupGoal).Methods("GET")
	r.HandleFunc("/api/groupgoals", s.createGroupGoal).Methods("POST")
	r.HandleFunc("/api/groupgoals/{id}", s.updateGroupGoal).Methods("PUT")
	r.HandleFunc("/api/groupgoals/{id}", s.deleteGroupGoal).Methods("DELETE")
	r.HandleFunc("/api/groupgoals/{id}/criteria", s.addCriterion).Methods("POST")
}

// DimensionCriterionResponse представляет критерий для измерения в ответе
type DimensionCriterionResponse struct {
	Type      string    `json:"type"`
	Condition string    `json:"condition"`
	Values    []float64 `json:"values"`
}

// GroupGoalResponse представляет ответ с данными цели группирования
type GroupGoalResponse struct {
	ID                string                       `json:"id"`
	MinGroupSize      int                          `json:"minGroupSize"`
	MaxGroupSize      int                          `json:"maxGroupSize"`
	MaxGroups         *int                         `json:"maxGroups,omitempty"`
	InactivityTimeout int64                        `json:"inactivityTimeout"`
	DimensionCriteria []DimensionCriterionResponse `json:"dimensionCriteria"`
	Version           uint64                       `json:"version"`
	Deleted           bool                         `json:"deleted"`
}

// GroupGoalRequest представляет запрос на создание/обновление цели группирования
type GroupGoalRequest struct {
	MinGroupSize      int                           `json:"minGroupSize"`
	MaxGroupSize      int                           `json:"maxGroupSize"`
	MaxGroups         *int                          `json:"maxGroups,omitempty"`
	InactivityTimeout int64                         `json:"inactivityTimeout"`
	DimensionCriteria []entities.DimensionCriterion `json:"dimensionCriteria,omitempty"`
}

// CriterionRequest представляет запрос на добавление критерия
type CriterionRequest struct {
	Type      string    `json:"type"`
	Condition string    `json:"condition"`
	Values    []float64 `json:"values"`
}

// getAllGroupGoals возвращает все цели группирования из реестра
func (s *Server) getAllGroupGoals(w http.ResponseWriter, r *http.Request) {
	goals, err := s.EntityRegistry.GroupGoal.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Error getting all group goals")
		sendError(w, http.StatusInternalServerError, "Error getting group goals")
		return
	}

	var response []GroupGoalResponse
	for _, goal := range goals {
		if !goal.IsDeleted() {
			criteria := make([]DimensionCriterionResponse, 0)
			for _, c := range goal.GetDimensionCriteria() {
				criteria = append(criteria, DimensionCriterionResponse{
					Type:      string(c.Type),
					Condition: string(c.Condition),
					Values:    c.Values,
				})
			}

			response = append(response, GroupGoalResponse{
				ID:                goal.GetID(),
				MinGroupSize:      goal.GetMinGroupSize(),
				MaxGroupSize:      goal.GetMaxGroupSize(),
				MaxGroups:         goal.GetMaxGroups(),
				InactivityTimeout: goal.GetInactivityTimeout(),
				DimensionCriteria: criteria,
				Version:           goal.GetVersion(),
				Deleted:           goal.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getGroupGoal возвращает цель группирования по её ID
func (s *Server) getGroupGoal(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	goal, err := s.EntityRegistry.GroupGoal.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group goal")
		sendError(w, http.StatusNotFound, "Group goal not found")
		return
	}

	if goal.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group goal is deleted")
		return
	}

	criteria := make([]DimensionCriterionResponse, 0)
	for _, c := range goal.GetDimensionCriteria() {
		criteria = append(criteria, DimensionCriterionResponse{
			Type:      string(c.Type),
			Condition: string(c.Condition),
			Values:    c.Values,
		})
	}

	response := GroupGoalResponse{
		ID:                goal.GetID(),
		MinGroupSize:      goal.GetMinGroupSize(),
		MaxGroupSize:      goal.GetMaxGroupSize(),
		MaxGroups:         goal.GetMaxGroups(),
		InactivityTimeout: goal.GetInactivityTimeout(),
		DimensionCriteria: criteria,
		Version:           goal.GetVersion(),
		Deleted:           goal.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// createGroupGoal создает новую цель группирования
func (s *Server) createGroupGoal(w http.ResponseWriter, r *http.Request) {
	var request GroupGoalRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Проверяем корректность параметров
	if request.MinGroupSize <= 0 || request.MaxGroupSize <= 0 || request.MinGroupSize > request.MaxGroupSize {
		sendError(w, http.StatusBadRequest, "Invalid group size parameters")
		return
	}

	// Создаем новую цель группирования
	var goal *entities.GroupGoal
	if request.InactivityTimeout > 0 {
		goal = entities.NewGroupGoalWithTimeout(request.MinGroupSize, request.MaxGroupSize, request.InactivityTimeout)
	} else {
		goal = entities.NewGroupGoal(request.MinGroupSize, request.MaxGroupSize)
	}

	// Устанавливаем максимальное количество групп, если указано
	goal.SetMaxGroups(request.MaxGroups)

	// Добавляем критерии, если они есть
	if len(request.DimensionCriteria) > 0 {
		goal.SetDimensionCriteria(request.DimensionCriteria)
	}

	// Сохраняем цель в реестре
	_, err := s.EntityRegistry.GroupGoal.StoreEntity(goal)
	if err != nil {
		log.Error().Err(err).Msg("Error storing group goal")
		sendError(w, http.StatusInternalServerError, "Error creating group goal")
		return
	}

	criteria := make([]DimensionCriterionResponse, 0)
	for _, c := range goal.GetDimensionCriteria() {
		criteria = append(criteria, DimensionCriterionResponse{
			Type:      string(c.Type),
			Condition: string(c.Condition),
			Values:    c.Values,
		})
	}

	response := GroupGoalResponse{
		ID:                goal.GetID(),
		MinGroupSize:      goal.GetMinGroupSize(),
		MaxGroupSize:      goal.GetMaxGroupSize(),
		MaxGroups:         goal.GetMaxGroups(),
		InactivityTimeout: goal.GetInactivityTimeout(),
		DimensionCriteria: criteria,
		Version:           goal.GetVersion(),
		Deleted:           goal.IsDeleted(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateGroupGoal обновляет цель группирования по её ID
func (s *Server) updateGroupGoal(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request GroupGoalRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	goal, err := s.EntityRegistry.GroupGoal.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group goal")
		sendError(w, http.StatusNotFound, "Group goal not found")
		return
	}

	if goal.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group goal is deleted")
		return
	}

	// Проверяем корректность параметров
	if request.MinGroupSize <= 0 || request.MaxGroupSize <= 0 || request.MinGroupSize > request.MaxGroupSize {
		sendError(w, http.StatusBadRequest, "Invalid group size parameters")
		return
	}

	// Обновляем параметры цели
	goal.SetMinGroupSize(request.MinGroupSize)
	goal.SetMaxGroupSize(request.MaxGroupSize)
	goal.SetMaxGroups(request.MaxGroups)
	goal.SetInactivityTimeout(request.InactivityTimeout)

	// Обновляем критерии, если они есть
	if len(request.DimensionCriteria) > 0 {
		goal.SetDimensionCriteria(request.DimensionCriteria)
	}

	// Сохраняем обновленную цель в реестре
	_, err = s.EntityRegistry.GroupGoal.StoreEntity(goal)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated group goal")
		sendError(w, http.StatusInternalServerError, "Error updating group goal")
		return
	}

	criteria := make([]DimensionCriterionResponse, 0)
	for _, c := range goal.GetDimensionCriteria() {
		criteria = append(criteria, DimensionCriterionResponse{
			Type:      string(c.Type),
			Condition: string(c.Condition),
			Values:    c.Values,
		})
	}

	response := GroupGoalResponse{
		ID:                goal.GetID(),
		MinGroupSize:      goal.GetMinGroupSize(),
		MaxGroupSize:      goal.GetMaxGroupSize(),
		MaxGroups:         goal.GetMaxGroups(),
		InactivityTimeout: goal.GetInactivityTimeout(),
		DimensionCriteria: criteria,
		Version:           goal.GetVersion(),
		Deleted:           goal.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteGroupGoal помечает цель группирования как удаленную
func (s *Server) deleteGroupGoal(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	goal, err := s.EntityRegistry.GroupGoal.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group goal")
		sendError(w, http.StatusNotFound, "Group goal not found")
		return
	}

	// Помечаем цель как удаленную
	goal.MarkDeleted()

	// Сохраняем обновленную цель в реестре
	_, err = s.EntityRegistry.GroupGoal.StoreEntity(goal)
	if err != nil {
		log.Error().Err(err).Msg("Error storing deleted group goal")
		sendError(w, http.StatusInternalServerError, "Error deleting group goal")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// addCriterion добавляет критерий к цели группирования
func (s *Server) addCriterion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var request CriterionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	goal, err := s.EntityRegistry.GroupGoal.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting group goal")
		sendError(w, http.StatusNotFound, "Group goal not found")
		return
	}

	if goal.IsDeleted() {
		sendError(w, http.StatusNotFound, "Group goal is deleted")
		return
	}

	// Проверяем корректность параметров критерия
	if len(request.Type) == 0 || len(request.Condition) == 0 {
		sendError(w, http.StatusBadRequest, "Invalid criterion parameters")
		return
	}

	// Проверяем условие
	condition := entities.DimensionConditionType(request.Condition)
	switch condition {
	case entities.ConditionBetween:
		if len(request.Values) != 2 {
			sendError(w, http.StatusBadRequest, "Between condition requires exactly 2 values")
			return
		}
	case entities.ConditionLessThan, entities.ConditionGreaterThan:
		if len(request.Values) != 1 {
			sendError(w, http.StatusBadRequest, "LessThan/GreaterThan condition requires exactly 1 value")
			return
		}
	case entities.ConditionMin, entities.ConditionMax:
		// Эти условия не требуют значений
		if len(request.Values) > 0 {
			request.Values = nil
		}
	default:
		sendError(w, http.StatusBadRequest, "Invalid condition type")
		return
	}

	// Создаем новый критерий
	criterion := entities.DimensionCriterion{
		Type:      entities.MeasurementType(request.Type),
		Condition: condition,
		Values:    request.Values,
	}

	// Добавляем критерий к цели
	goal.AddDimensionCriterion(criterion)

	// Сохраняем обновленную цель в реестре
	_, err = s.EntityRegistry.GroupGoal.StoreEntity(goal)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated group goal")
		sendError(w, http.StatusInternalServerError, "Error adding criterion")
		return
	}

	criteria := make([]DimensionCriterionResponse, 0)
	for _, c := range goal.GetDimensionCriteria() {
		criteria = append(criteria, DimensionCriterionResponse{
			Type:      string(c.Type),
			Condition: string(c.Condition),
			Values:    c.Values,
		})
	}

	response := GroupGoalResponse{
		ID:                goal.GetID(),
		MinGroupSize:      goal.GetMinGroupSize(),
		MaxGroupSize:      goal.GetMaxGroupSize(),
		MaxGroups:         goal.GetMaxGroups(),
		InactivityTimeout: goal.GetInactivityTimeout(),
		DimensionCriteria: criteria,
		Version:           goal.GetVersion(),
		Deleted:           goal.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}
