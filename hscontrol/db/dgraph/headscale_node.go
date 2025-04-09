package dgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"google.golang.org/grpc"
	"log"
	"time"
)

type HeadscaleNode struct {
	UID             string    `json:"uid,omitempty"`
	NodeID          string    `json:"node_id,omitempty"`
	Host            string    `json:"host,omitempty"`
	Port            string    `json:"port,omitempty"`
	LastAvailableAt time.Time `json:"last_available_at,omitempty"`
}

// Characteristic представляет собой пару тип-значение для характеристики соединения.
type Characteristic struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func SaveNode(ctx context.Context, dg *dgo.Dgraph, node *HeadscaleNode) error {
	txn := dg.NewTxn()
	defer txn.Discard(ctx)

	// Мутируем ноду, используя маршалинг структуры в JSON.
	pb, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга ноды: %w", err)
	}

	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   pb,
	}
	resp, err := txn.Mutate(ctx, mu)
	if err != nil {
		return fmt.Errorf("ошибка мутации ноды: %w", err)
	}

	// Если NodeID задан, запишем полученный UID.
	if node.NodeID != "" {
		if uid, ok := resp.Uids[node.NodeID]; ok {
			node.UID = uid
		}
	}
	return nil
}

func UpdateLastAvailableAt(ctx context.Context, dg *dgo.Dgraph, node *HeadscaleNode, newTime time.Time) error {
	// Подготавливаем данные для обновления — здесь мы обновляем только поле last_available_at
	updateData := map[string]interface{}{
		"uid":               node.UID,
		"last_available_at": newTime.Format(time.RFC3339),
	}
	pb, err := json.Marshal(updateData)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга обновления: %w", err)
	}

	txn := dg.NewTxn()
	defer txn.Discard(ctx)
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   pb,
	}
	if _, err := txn.Mutate(ctx, mu); err != nil {
		return fmt.Errorf("ошибка обновления поля last_available_at: %w", err)
	}
	return nil
}

// AddConnection добавляет связь (ребро) между двумя нодами с характеристиками,
// каждая характеристика представлена парой тип-значение.
func AddConnection(ctx context.Context, dg *dgo.Dgraph, from, to *HeadscaleNode, characteristics []Characteristic) error {
	txn := dg.NewTxn()
	defer txn.Discard(ctx)

	facets := ""
	for i, c := range characteristics {
		if i > 0 {
			facets += ", "
		}
		facets += fmt.Sprintf("%s=%d", c.Type, c.Value)
	}

	nquad := fmt.Sprintf("<%s> <connected_to> <%s> (%s) .", from.UID, to.UID, facets)
	mu := &api.Mutation{
		CommitNow: true,
		SetNquads: []byte(nquad),
	}
	_, err := txn.Mutate(ctx, mu)
	if err != nil {
		return fmt.Errorf("ошибка добавления связи: %w", err)
	}
	return nil
}

// UpdateConnection обновляет характеристики связи (ребра) между двумя нодами.
// Для этого сначала удаляется существующая связь, а затем добавляется новая с заданными характеристиками.
func UpdateConnection(ctx context.Context, dg *dgo.Dgraph, from, to *HeadscaleNode, characteristics []Characteristic) error {
	txn := dg.NewTxn()
	defer txn.Discard(ctx)

	// Удаляем существующую связь между нодами.
	delNQuad := fmt.Sprintf("<%s> <connected_to> <%s> .", from.UID, to.UID)
	delMu := &api.Mutation{
		CommitNow: true,
		DelNquads: []byte(delNQuad),
	}
	if _, err := txn.Mutate(ctx, delMu); err != nil {
		return fmt.Errorf("ошибка удаления существующей связи: %w", err)
	}

	// Формируем facets для новой связи.
	facets := ""
	for i, c := range characteristics {
		if i > 0 {
			facets += ", "
		}
		facets += fmt.Sprintf("%s=%d", c.Type, c.Value)
	}

	// Добавляем новую связь с обновлёнными характеристиками.
	addNQuad := fmt.Sprintf("<%s> <connected_to> <%s> (%s) .", from.UID, to.UID, facets)
	addMu := &api.Mutation{
		CommitNow: true,
		SetNquads: []byte(addNQuad),
	}
	if _, err := txn.Mutate(ctx, addMu); err != nil {
		return fmt.Errorf("ошибка добавления обновленной связи: %w", err)
	}
	return nil
}

func main() {
	// Подключаемся к Dgraph (предполагается, что сервер запущен на localhost:9080).
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Не удалось подключиться к Dgraph: %v", err)
	}
	defer conn.Close()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	ctx := context.Background()

	// Создаем две ноды, используя структуры.
	node1 := &HeadscaleNode{
		NodeID: "node1",
		Host:   "192.168.1.1",
		Port:   "8080",
	}
	node2 := &HeadscaleNode{
		NodeID: "node2",
		Host:   "192.168.1.2",
		Port:   "8081",
	}

	// Сохраняем ноды.
	if err := SaveNode(ctx, dg, node1); err != nil {
		log.Fatalf("Ошибка сохранения node1: %v", err)
	}
	if err := SaveNode(ctx, dg, node2); err != nil {
		log.Fatalf("Ошибка сохранения node2: %v", err)
	}
	fmt.Printf("Сохранены ноды: node1 UID=%s, node2 UID=%s\n", node1.UID, node2.UID)

	// Добавляем связь между node1 и node2 с характеристиками latency и bandwidth.
	characteristics := []Characteristic{
		{Type: "latency", Value: 50},
		{Type: "bandwidth", Value: 1000},
	}
	if err := AddConnection(ctx, dg, node1, node2, characteristics); err != nil {
		log.Fatalf("Ошибка добавления связи: %v", err)
	}
	fmt.Println("Связь добавлена с характеристиками (latency и bandwidth)")

	// Обновляем характеристики связи между node1 и node2.
	newCharacteristics := []Characteristic{
		{Type: "latency", Value: 30},
		{Type: "bandwidth", Value: 2000},
	}
	if err := UpdateConnection(ctx, dg, node1, node2, newCharacteristics); err != nil {
		log.Fatalf("Ошибка обновления связи: %v", err)
	}
	fmt.Println("Связь обновлена с новыми характеристиками (latency и bandwidth)")
}
