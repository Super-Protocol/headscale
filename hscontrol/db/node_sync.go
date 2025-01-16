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
			err := njs.SyncNodes()
			if err != nil {
				log.Error().Err(err).
					Str("dir", njs.dirPath).
					Msg("Failed to sync nodes with JSON directory")
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

	// Шаг 1: Проверяем, есть ли файл на диске
	if _, err := os.Stat(fileName); err == nil {
		// Файл существует, проверяем `UpdatedAt`
		raw, err := ioutil.ReadFile(fileName)
		if err != nil {
			return fmt.Errorf("failed to read existing JSON file '%s': %w", fileName, err)
		}

		var existingNode types.Node
		if err := json.Unmarshal(raw, &existingNode); err != nil {
			return fmt.Errorf("failed to unmarshal existing JSON file '%s': %w", fileName, err)
		}

		// Сравниваем `UpdatedAt`
		if !node.UpdatedAt.After(existingNode.UpdatedAt) {
			log.Trace().
				Str("file", fileName).
				Str("machine_key", node.MachineKey.ShortString()).
				Msg("Node on disk is newer or equal, skipping export")
			return nil
		}
	}

	// Шаг 2: Выполняем экспорт, если файл либо отсутствует, либо его версия старее
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

// SyncNodes выполняет полную синхронизацию между БД и файлами JSON.
func (njs *NodeJSONSync) SyncNodes() error {
	// Шаг 1: Обновляем JSON-файлы из БД
	nodes, err := njs.hsdb.ListNodes()
	if err != nil {
		return fmt.Errorf("failed to list nodes from database: %w", err)
	}

	for _, node := range nodes {
		err := njs.ExportNodeToJSON(node)
		if err != nil {
			log.Error().Err(err).
				Str("node", node.Hostname).
				Msg("Failed to export node to JSON")
		}
	}

	// Шаг 2: Импортируем ноды из файлов JSON
	err = njs.importNodesFromDir(njs.ctx, njs.dirPath)
	if err != nil {
		return fmt.Errorf("failed to import nodes from JSON directory: %w", err)
	}

	return nil
}

// importNodesFromDir импортирует ноды из файлов JSON, обновляя или добавляя их в БД.
func (njs *NodeJSONSync) importNodesFromDir(ctx context.Context, dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Warn().
				Str("dir", dirPath).
				Msg("JSON import directory does not exist")
			return nil
		}
		return fmt.Errorf("cannot read directory %s: %w", dirPath, err)
	}

	nodesUpdated := false

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		fullPath := filepath.Join(dirPath, entry.Name())

		var node types.Node
		raw, err := ioutil.ReadFile(fullPath)
		if err != nil {
			log.Error().Err(err).
				Str("file", entry.Name()).
				Msg("Failed to read JSON file")
			continue
		}

		err = json.Unmarshal(raw, &node)
		if err != nil {
			log.Error().Err(err).
				Str("file", entry.Name()).
				Msg("Failed to unmarshal JSON file")
			continue
		}

		existingNode, err := njs.hsdb.GetNodeByMachineKey(node.MachineKey)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Error().Err(err).
				Str("machine_key", node.MachineKey.ShortString()).
				Msg("Failed to fetch node from database")
			continue
		}

		if existingNode != nil {
			// Обновляем ноду только если UpdatedAt в JSON более новый
			if node.UpdatedAt.After(existingNode.UpdatedAt) {
				node.ID = existingNode.ID // сохраняем ID из БД
				_, err := njs.hsdb.RegisterNode(node, node.IPv4, node.IPv6)
				if err != nil {
					log.Error().Err(err).
						Str("machine_key", node.MachineKey.ShortString()).
						Msg("Failed to update node in database")
				} else {
					nodesUpdated = true
				}
			}
		} else {
			// Добавляем новую ноду
			node.ID = 0
			_, err := njs.hsdb.RegisterNode(node, node.IPv4, node.IPv6)
			if err != nil {
				log.Error().Err(err).
					Str("machine_key", node.MachineKey.ShortString()).
					Msg("Failed to register new node in database")
			} else {
				nodesUpdated = true
			}
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
