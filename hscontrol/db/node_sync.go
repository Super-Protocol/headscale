package db

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	//"github.com/google/uuid"
	"github.com/juanfont/headscale/hscontrol/notifier"
	"github.com/juanfont/headscale/hscontrol/types"
	"github.com/juanfont/headscale/hscontrol/util"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NodeJSONSync — «класс», который управляет экспортом/импортом Node в JSON.
type NodeJSONSync struct {
	hsdb         *HSDatabase
	nodeNotifier *notifier.Notifier
	dirPath      string          // Путь к директории, где храним JSON
	interval     time.Duration   // Интервал проверки
	ctx          context.Context // Контекст для фоновой горутины
	cancel       context.CancelFunc
}

// NewNodeJSONSync — «конструктор».
// Создаёт объект NodeJSONSync, который запускает фоновую горутину для импорта JSON.
func NewNodeJSONSync(
	hsdb *HSDatabase,
	nodeNotifier *notifier.Notifier,
	dirPath string,
	interval time.Duration,
) (*NodeJSONSync, error) {
	// Можем сразу проверить, существует ли директория
	if dirPath == "" {
		return nil, fmt.Errorf("node JSON sync directory is not specified")
	}

	// Создадим контекст, который сможем отменить при Stop()
	ctx, cancel := context.WithCancel(context.Background())

	njs := &NodeJSONSync{
		hsdb:         hsdb,
		nodeNotifier: nodeNotifier,
		dirPath:      dirPath,
		interval:     interval,
		ctx:          ctx,
		cancel:       cancel,
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
	encodedKey := getNodeUniqueId(node)
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

		online := njs.nodeNotifier.IsLikelyConnected(node.ID)
		node.IsOnline = &online

		if !node.DiffersFrom(&existingNode) {
			log.Trace().
				Str("file", fileName).
				Str("machine_key", node.MachineKey.ShortString()).
				Msg("Node on disk is newer or equal, skipping export")
			return nil
		}

		/*newFileName := filepath.Join(njs.dirPath, fmt.Sprintf("%s_%s.json.bak", encodedKey, uuid.New().String()))

		if err := os.Rename(fileName, newFileName); err != nil {
			log.Error().
				Err(err).
				Str("old_file", fileName).
				Str("new_file", newFileName).
				Msg("Failed to rename existing file")
			return fmt.Errorf("failed to rename existing JSON file '%s' to '%s': %w", fileName, newFileName, err)
		}

		log.Info().
			Str("old_file", fileName).
			Str("new_file", newFileName).
			Msg("Renamed existing file with new UUID-based name")*/

	}

	node.RegisterMethod = util.RegisterMethodImport
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
	nodes, err := njs.hsdb.ListNodes()
	if err != nil {
		return fmt.Errorf("failed to list nodes from database: %w", err)
	}

	for _, node := range nodes {
		if node.RegisterMethod == util.RegisterMethodImport || len(node.Endpoints) == 0 || node.IPv4 == nil || node.IPv6 == nil ||
			time.Since(node.CreatedAt) < 10*time.Second {
			continue
		}
		err := njs.ExportNodeToJSON(node)
		if err != nil {
			log.Error().Err(err).
				Str("node", node.Hostname).
				Msg("Failed to export node to JSON")
		}
	}

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

	var updatedNodes []types.NodeID

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

		log.Debug().
			Str("hostname", node.Hostname).
			Msg("Try to find node by hostname")

		existingNode, err := njs.hsdb.GetNodeByHostName(node.Hostname)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Error().Err(err).
				Str("hostname", node.Hostname).
				Msg("Failed to fetch node from database")
			continue
		}

		if existingNode != nil {
			log.Debug().
				Str("hostname", node.Hostname).
				Str("RegisterMethod", node.RegisterMethod).
				Msg("Found node by hostname")
		}

		if existingNode != nil {
			if existingNode.RegisterMethod == util.RegisterMethodImport && node.DiffersFrom(existingNode) {
				node.ID = existingNode.ID
				node.RegisterMethod = util.RegisterMethodImport
				err := njs.hsdb.Write(func(tx *gorm.DB) error {
					return tx.Save(&node).Error
				})
				if err != nil {
					log.Error().Err(err).
						Str("hostname", node.Hostname).
						Msg("Failed to update node in database")
				} else {
					updatedNodes = append(updatedNodes, node.ID)
				}
			}
		} else {
			node.ID = 0
			node.RegisterMethod = util.RegisterMethodImport
			_, err := njs.hsdb.RegisterNode(node, node.IPv4, node.IPv6)
			if err != nil {
				log.Error().Err(err).
					Str("hostname", node.Hostname).
					Msg("Failed to register new node in database")
			} else {
				updatedNodes = append(updatedNodes, node.ID)
			}
		}
	}

	if len(updatedNodes) > 0 && njs.nodeNotifier != nil {
		ctx := types.NotifyCtx(context.Background(), "acl-nodes-change", "all")
		njs.nodeNotifier.NotifyAll(ctx, types.StateUpdate{
			Type: types.StateFullUpdate,
		})
	}

	return nil
}

func getNodeUniqueId(node *types.Node) string {
	encoder := base32.StdEncoding.WithPadding(base32.NoPadding)
	base32Name := encoder.EncodeToString([]byte(node.Hostname))
	return strings.ToLower(base32Name)
}
