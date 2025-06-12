package api

import (
	"github.com/gorilla/mux"
	"net/http"
)

// registerEventLogRoutes регистрирует маршруты для работы с журналом событий
func (s *Server) registerEventLogRoutes(router *mux.Router) {
	router.HandleFunc("/eventlog", s.getEventLog).Methods("GET")
}

// getEventLog возвращает журнал событий
func (s *Server) getEventLog(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, http.StatusOK, s.eventLog)
}
