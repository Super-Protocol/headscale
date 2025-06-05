package api

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common"
	"github.com/rs/zerolog/log"
)

// StartAPIServer создает и запускает API сервер для EntityRegistry
func StartAPIServer(registry *common.EntityRegistry, addr string) (*Server, error) {
	server := NewServer(registry)
	
	// Запускаем сервер в отдельной горутине
	go func() {
		err := server.Start(addr)
		if err != nil {
			log.Error().Err(err).Msg("API server error")
		}
	}()
	
	log.Info().
		Str("addr", addr).
		Msg("API server started")
	
	return server, nil
}
