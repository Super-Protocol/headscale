package measurer

// TransportConfig содержит настройки для транспорта
type TransportConfig struct {
	ListenHost string
	ListenPort int
	CaFile     string
	CertFile   string
	KeyFile    string
}
