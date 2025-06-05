package api

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

// Server представляет HTTP-сервер для EntityRegistry
type Server struct {
	EntityRegistry *common.EntityRegistry
	router         *mux.Router
	httpServer     *http.Server
}

// NewServer создает новый экземпляр сервера для EntityRegistry
func NewServer(registry *common.EntityRegistry) *Server {
	s := &Server{
		EntityRegistry: registry,
	}
	s.setupRouter()
	return s
}

// setupRouter настраивает маршрутизатор для обработки HTTP-запросов
func (s *Server) setupRouter() {
	r := mux.NewRouter()

	// Регистрация маршрутов для всех типов сущностей
	s.registerNodeRoutes(r)
	s.registerGroupRoutes(r)
	s.registerGroupGoalRoutes(r)
	s.registerMeasurementRoutes(r)

	s.router = r
}

// Start запускает HTTP-сервер на указанном адресе
func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Handler:      s.router,
		Addr:         addr,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Info().Str("addr", addr).Msg("Starting API server")
	return s.httpServer.ListenAndServe()
}

// Stop останавливает HTTP-сервер
func (s *Server) Stop() error {
	log.Info().Msg("Stopping API server")
	return s.httpServer.Close()
}

// sendJSON отправляет ответ в формате JSON
func sendJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Error marshaling JSON: %v", err)))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

// sendError отправляет сообщение об ошибке в формате JSON
func sendError(w http.ResponseWriter, code int, message string) {
	sendJSON(w, code, map[string]string{"error": message})
}
