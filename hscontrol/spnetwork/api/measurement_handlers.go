package api

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

// registerMeasurementRoutes регистрирует маршруты для сущности Measurement
func (s *Server) registerMeasurementRoutes(r *mux.Router) {
	r.HandleFunc("/api/measurements", s.getAllMeasurements).Methods("GET")
	r.HandleFunc("/api/measurements/node/{nodeId}", s.getMeasurementsForNode).Methods("GET")
	r.HandleFunc("/api/measurements/{owner}/{target}", s.getMeasurement).Methods("GET")
	r.HandleFunc("/api/measurements", s.createMeasurement).Methods("POST")
	r.HandleFunc("/api/measurements/{owner}/{target}", s.updateMeasurement).Methods("PUT")
	r.HandleFunc("/api/measurements/{owner}/{target}", s.deleteMeasurement).Methods("DELETE")
}

// MeasurementResponse представляет ответ с данными измерения
type MeasurementResponse struct {
	Owner    string  `json:"owner"`
	Target   string  `json:"target"`
	Type     string  `json:"type"`
	Value    float64 `json:"value"`
	DateUnix int64   `json:"dateUnix"`
	Version  uint64  `json:"version"`
	Deleted  bool    `json:"deleted"`
}

// MeasurementRequest представляет запрос на создание/обновление измерения
type MeasurementRequest struct {
	Owner    string  `json:"owner"`
	Target   string  `json:"target"`
	Type     string  `json:"type"`
	Value    float64 `json:"value"`
	DateUnix int64   `json:"dateUnix,omitempty"` // Опционально, по умолчанию текущее время
}

// getAllMeasurements возвращает все измерения из реестра
func (s *Server) getAllMeasurements(w http.ResponseWriter, r *http.Request) {
	measurements, err := s.EntityRegistry.Measurement.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Error getting all measurements")
		sendError(w, http.StatusInternalServerError, "Error getting measurements")
		return
	}

	var response []MeasurementResponse
	for _, measurement := range measurements {
		if !measurement.IsDeleted() {
			response = append(response, MeasurementResponse{
				Owner:    measurement.Owner,
				Target:   measurement.Target,
				Type:     string(measurement.Type),
				Value:    measurement.Value,
				DateUnix: measurement.DateUnix,
				Version:  measurement.GetVersion(),
				Deleted:  measurement.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getMeasurementsForNode возвращает все измерения для указанного узла (как owner или target)
func (s *Server) getMeasurementsForNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeId := vars["nodeId"]

	// Получаем все измерения
	measurements, err := s.EntityRegistry.Measurement.GetAllEntities()
	if err != nil {
		log.Error().Err(err).Msg("Error getting all measurements")
		sendError(w, http.StatusInternalServerError, "Error getting measurements")
		return
	}

	var response []MeasurementResponse
	for _, measurement := range measurements {
		if (measurement.Owner == nodeId || measurement.Target == nodeId) && !measurement.IsDeleted() {
			response = append(response, MeasurementResponse{
				Owner:    measurement.Owner,
				Target:   measurement.Target,
				Type:     string(measurement.Type),
				Value:    measurement.Value,
				DateUnix: measurement.DateUnix,
				Version:  measurement.GetVersion(),
				Deleted:  measurement.IsDeleted(),
			})
		}
	}

	sendJSON(w, http.StatusOK, response)
}

// getMeasurement возвращает измерение по owner и target
func (s *Server) getMeasurement(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	owner := vars["owner"]
	target := vars["target"]

	// Формируем ID измерения (owner-target)
	id := fmt.Sprintf("%s-%s", owner, target)

	measurement, err := s.EntityRegistry.Measurement.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting measurement")
		sendError(w, http.StatusNotFound, "Measurement not found")
		return
	}

	if measurement.IsDeleted() {
		sendError(w, http.StatusNotFound, "Measurement is deleted")
		return
	}

	response := MeasurementResponse{
		Owner:    measurement.Owner,
		Target:   measurement.Target,
		Type:     string(measurement.Type),
		Value:    measurement.Value,
		DateUnix: measurement.DateUnix,
		Version:  measurement.GetVersion(),
		Deleted:  measurement.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// createMeasurement создает новое измерение
func (s *Server) createMeasurement(w http.ResponseWriter, r *http.Request) {
	var request MeasurementRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Проверяем, что owner и target указаны
	if request.Owner == "" || request.Target == "" {
		sendError(w, http.StatusBadRequest, "Owner and Target are required")
		return
	}

	// Проверяем, что owner и target существуют в реестре
	_, err := s.EntityRegistry.Node.GetEntity(request.Owner)
	if err != nil {
		log.Error().Err(err).Str("node_id", request.Owner).Msg("Owner node not found")
		sendError(w, http.StatusBadRequest, "Owner node not found")
		return
	}

	_, err = s.EntityRegistry.Node.GetEntity(request.Target)
	if err != nil {
		log.Error().Err(err).Str("node_id", request.Target).Msg("Target node not found")
		sendError(w, http.StatusBadRequest, "Target node not found")
		return
	}

	// Проверяем тип измерения
	measurementType := entities.MeasurementType(request.Type)
	if measurementType != entities.LatencyClass && measurementType != entities.BandwidthClass {
		sendError(w, http.StatusBadRequest, "Invalid measurement type")
		return
	}

	// Если дата не указана, используем текущее время
	dateUnix := request.DateUnix
	if dateUnix == 0 {
		dateUnix = time.Now().Unix()
	}

	// Создаем новое измерение
	measurement := entities.NewMeasurement(
		request.Owner,
		request.Target,
		measurementType,
		request.Value,
		dateUnix,
	)

	// Сохраняем измерение в реестре
	_, err = s.EntityRegistry.Measurement.StoreEntity(measurement)
	if err != nil {
		log.Error().Err(err).Msg("Error storing measurement")
		sendError(w, http.StatusInternalServerError, "Error creating measurement")
		return
	}

	response := MeasurementResponse{
		Owner:    measurement.Owner,
		Target:   measurement.Target,
		Type:     string(measurement.Type),
		Value:    measurement.Value,
		DateUnix: measurement.DateUnix,
		Version:  measurement.GetVersion(),
		Deleted:  measurement.IsDeleted(),
	}

	sendJSON(w, http.StatusCreated, response)
}

// updateMeasurement обновляет измерение по owner и target
func (s *Server) updateMeasurement(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	owner := vars["owner"]
	target := vars["target"]

	var request MeasurementRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Формируем ID измерения (owner-target)
	id := fmt.Sprintf("%s-%s", owner, target)

	measurement, err := s.EntityRegistry.Measurement.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting measurement")
		sendError(w, http.StatusNotFound, "Measurement not found")
		return
	}

	if measurement.IsDeleted() {
		sendError(w, http.StatusNotFound, "Measurement is deleted")
		return
	}

	// Если дата не указана, используем текущее время
	dateUnix := request.DateUnix
	if dateUnix == 0 {
		dateUnix = time.Now().Unix()
	}

	// Обновляем значение измерения
	measurement.UpdateValue(request.Value, dateUnix)

	// Сохраняем обновленное измерение в реестре
	_, err = s.EntityRegistry.Measurement.StoreEntity(measurement)
	if err != nil {
		log.Error().Err(err).Msg("Error storing updated measurement")
		sendError(w, http.StatusInternalServerError, "Error updating measurement")
		return
	}

	response := MeasurementResponse{
		Owner:    measurement.Owner,
		Target:   measurement.Target,
		Type:     string(measurement.Type),
		Value:    measurement.Value,
		DateUnix: measurement.DateUnix,
		Version:  measurement.GetVersion(),
		Deleted:  measurement.IsDeleted(),
	}

	sendJSON(w, http.StatusOK, response)
}

// deleteMeasurement помечает измерение как удаленное
func (s *Server) deleteMeasurement(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	owner := vars["owner"]
	target := vars["target"]

	// Формируем ID измерения (owner-target)
	id := fmt.Sprintf("%s-%s", owner, target)

	measurement, err := s.EntityRegistry.Measurement.GetEntity(id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Error getting measurement")
		sendError(w, http.StatusNotFound, "Measurement not found")
		return
	}

	// Помечаем измерение как удаленное
	measurement.MarkDeleted()

	// Сохраняем обновленное измерение в реестре
	_, err = s.EntityRegistry.Measurement.StoreEntity(measurement)
	if err != nil {
		log.Error().Err(err).Msg("Error storing deleted measurement")
		sendError(w, http.StatusInternalServerError, "Error deleting measurement")
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
