package db

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juanfont/headscale/hscontrol/types"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"tailscale.com/types/key"
	"time"
)

const (
	base32Pad = false
)

// NodeJSONSync — «класс», который управляет экспортом/импортом Node в JSON.
type NodeJSONSync struct {
	hsdb            *HSDatabase     // Ссылка на базу
	dirPath         string          // Путь к директории, где храним JSON
	interval        time.Duration   // Интервал проверки
	ctx             context.Context // Контекст для фоновой горутины
	cancel          context.CancelFunc
	onNodesImported func()
}

// NewNodeJSONSync — «конструктор».
// Создаёт объект NodeJSONSync, который запускает фоновую горутину для импорта JSON.
func NewNodeJSONSync(
	hsdb *HSDatabase,
	dirPath string,
	interval time.Duration,
	onNodesImported func(),
) (*NodeJSONSync, error) {
	// Можем сразу проверить, существует ли директория
	if dirPath == "" {
		return nil, fmt.Errorf("node JSON sync directory is not specified")
	}

	// Создадим контекст, который сможем отменить при Stop()
	ctx, cancel := context.WithCancel(context.Background())

	njs := &NodeJSONSync{
		hsdb:            hsdb,
		dirPath:         dirPath,
		interval:        interval,
		ctx:             ctx,
		cancel:          cancel,
		onNodesImported: onNodesImported,
	}

	// Запускаем фоновую задачу
	go njs.start()

	log.Debug().
		Str("dir", dirPath).
		Dur("interval", interval).
		Msg("NodeJSONSync started")

	return njs, nil
}

// start — внутренняя функция, которую мы вызываем из конструктора.
func (njs *NodeJSONSync) start() {
	ticker := time.NewTicker(njs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-njs.ctx.Done():
			log.Info().
				Str("dir", njs.dirPath).
				Msg("NodeJSONSync stopped")
			return

		case <-ticker.C:
			// Периодически вызываем importNodesFromDir
			err := njs.importNodesFromDir(njs.ctx, njs.dirPath)
			if err != nil {
				log.Error().Err(err).
					Str("dir", njs.dirPath).
					Msg("Failed to import nodes from JSON directory")
			}
		}
	}
}

// Stop останавливает фоновую горутину NodeJSONSync.
func (njs *NodeJSONSync) Stop() {
	njs.cancel()
}

// ExportNodeToJSON — публичный метод, чтобы удобно вызвать экспорт ноды в JSON.
// Можно вызывать после RegisterNode или изнутри RegisterNode напрямую.
func (njs *NodeJSONSync) ExportNodeToJSON(node *types.Node) error {
	encodedKey := encodeMachineKey(node.MachineKey)
	fileName := filepath.Join(njs.dirPath, encodedKey+".json")

	data, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal node to JSON: %w", err)
	}

	if err := os.WriteFile(fileName, data, 0o600); err != nil {
		return fmt.Errorf("failed to write node JSON file '%s': %w", fileName, err)
	}

	log.Debug().
		Str("file", fileName).
		Str("machine_key", node.MachineKey.ShortString()).
		Msg("Exported node to JSON")

	return nil
}

// importNodesFromDir — сканирует dirPath, ищет *.json,
// проверяет, есть ли такой MachineKey в БД, если нет — импортирует.
func (njs *NodeJSONSync) importNodesFromDir(ctx context.Context, dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	nodesUpdated := false
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Папки ещё нет — не страшно, просто выдадим предупреждение
			log.Warn().
				Str("dir", dirPath).
				Msg("JSON import directory does not exist")
			return nil
		}
		return fmt.Errorf("cannot read directory %s: %w", dirPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if !strings.HasSuffix(fileName, ".json") {
			continue
		}

		encodedKey := strings.TrimSuffix(fileName, ".json")
		machineKey, err := decodeMachineKey(encodedKey)
		if err != nil {
			log.Error().Err(err).
				Str("filename", fileName).
				Msg("Failed to decode machineKey from filename")
			continue
		}

		// Проверяем, нет ли уже этой ноды
		_, err = njs.hsdb.GetNodeByMachineKey(machineKey)
		if err == nil {
			// Нода уже есть
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			// Другая ошибка
			log.Error().Err(err).
				Str("filename", fileName).
				Msg("Failed to check node in DB")
			continue
		}

		// Если ноды нет — пробуем импортировать
		fullPath := filepath.Join(dirPath, fileName)
		if err := njs.importNodeJSON(ctx, fullPath); err != nil {
			log.Error().Err(err).
				Str("filename", fileName).
				Msg("Failed to import node from JSON")
		} else {
			nodesUpdated = true
		}
	}
	if nodesUpdated && njs.onNodesImported != nil {
		njs.onNodesImported()
	}
	return nil
}

// importNodeJSON — читает файл, парсит Node и добавляет в БД (RegisterNode).
func (njs *NodeJSONSync) importNodeJSON(ctx context.Context, fullPath string) error {
	raw, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", fullPath, err)
	}

	var node types.Node
	if err := json.Unmarshal(raw, &node); err != nil {
		return fmt.Errorf("failed to unmarshal node from JSON: %w", err)
	}
	node.ID = 0
	_, regErr := njs.hsdb.RegisterNode(node, node.IPv4, node.IPv6)
	if regErr != nil {
		return fmt.Errorf("failed to register node: %w", regErr)
	}

	log.Info().
		Str("hostname", node.Hostname).
		Str("machine_key", node.MachineKey.ShortString()).
		Msg("Node imported from JSON into DB")

	return nil
}

// encodeMachineKey — кодируем MachineKey в base32, чтобы не было проблем с именами файлов.
func encodeMachineKey(mkey key.MachinePublic) string {
	// MachinePublic.String() даёт что-то вроде "mkey:abcdef0123..."
	raw := mkey.String()
	raw = strings.TrimPrefix(raw, "mkey:") // уберём префикс

	enc := base32.StdEncoding
	if !base32Pad {
		enc = enc.WithPadding(base32.NoPadding)
	}
	return enc.EncodeToString([]byte(raw))
}

// decodeMachineKey — обратная операция: декодируем base32 и делаем UnmarshalText("mkey:...").
func decodeMachineKey(encoded string) (key.MachinePublic, error) {
	dec := base32.StdEncoding
	if !base32Pad {
		dec = dec.WithPadding(base32.NoPadding)
	}

	rawBytes, err := dec.DecodeString(encoded)
	if err != nil {
		return key.MachinePublic{}, fmt.Errorf("base32 decode error: %w", err)
	}
	raw := string(rawBytes)

	// Восстановим формат "mkey:..."
	full := "mkey:" + raw

	var mp key.MachinePublic
	if err := mp.UnmarshalText([]byte(full)); err != nil {
		return key.MachinePublic{}, fmt.Errorf("failed to unmarshal machine key: %w", err)
	}
	return mp, nil
}
