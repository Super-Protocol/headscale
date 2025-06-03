package entities

import (
	"crypto/md5"
	"encoding/binary"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"sort"
	"sync"
)

type Group struct {
	ID           string
	Goal         string
	MinSize      uint32
	MaxSize      uint32
	Participants []string
	Ready        bool
	Version      uint64
	Deleted      bool
	mu           sync.RWMutex
}

func NewGroup(id string) *Group {
	return &Group{
		ID:           id,
		Participants: make([]string, 0),
		Version:      0,
		Deleted:      false,
	}
}

// GetID возвращает идентификатор группы
func (g *Group) GetID() string {
	return g.ID
}

// GetVersion возвращает текущую версию группы
func (g *Group) GetVersion() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Version
}

// IsDeleted возвращает статус удаления
func (g *Group) IsDeleted() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Deleted
}

// GetHash вычисляет хеш для группы
func (g *Group) GetHash() []byte {
	g.mu.RLock()
	defer g.mu.RUnlock()

	h := md5.New()
	h.Write([]byte(g.ID))
	h.Write([]byte(g.Goal))

	err := binary.Write(h, binary.LittleEndian, g.MinSize)
	if err != nil {
		return nil
	}

	err = binary.Write(h, binary.LittleEndian, g.MaxSize)
	if err != nil {
		return nil
	}

	// Сортируем участников для стабильного хеша
	participants := make([]string, len(g.Participants))
	copy(participants, g.Participants)
	sort.Strings(participants)

	for _, participant := range participants {
		h.Write([]byte(participant))
	}

	if g.Ready {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	err = binary.Write(h, binary.LittleEndian, g.Version)
	if err != nil {
		return nil
	}

	if g.Deleted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return h.Sum(nil)
}

// ToProto конвертирует Group в protobuf сообщение
func (g *Group) ToProto() *p.Group {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return &p.Group{
		Id:           g.ID,
		Goal:         g.Goal,
		MinSize:      g.MinSize,
		MaxSize:      g.MaxSize,
		Participants: g.Participants,
		Ready:        g.Ready,
		Version:      g.Version,
		Deleted:      g.Deleted,
	}
}

// Serialize сериализует Group в байты
func (g *Group) Serialize() ([]byte, error) {
	return proto.Marshal(g.ToProto())
}

// GetGoal возвращает цель группы
func (g *Group) GetGoal() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Goal
}

// SetGoal устанавливает цель группы и увеличивает версию
func (g *Group) SetGoal(goal string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Goal = goal
	g.Version++
}

// GetMinSize возвращает минимальный размер группы
func (g *Group) GetMinSize() uint32 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.MinSize
}

// SetMinSize устанавливает минимальный размер группы и увеличивает версию
func (g *Group) SetMinSize(size uint32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.MinSize = size
	g.Version++
}

// GetMaxSize возвращает максимальный размер группы
func (g *Group) GetMaxSize() uint32 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.MaxSize
}

// SetMaxSize устанавливает максимальный размер группы и увеличивает версию
func (g *Group) SetMaxSize(size uint32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.MaxSize = size
	g.Version++
}

// GetParticipants возвращает список участников группы
func (g *Group) GetParticipants() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]string, len(g.Participants))
	copy(result, g.Participants)
	return result
}

// AddParticipant добавляет участника в группу и увеличивает версию
func (g *Group) AddParticipant(participant string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Проверяем, что участник еще не добавлен
	for _, prt := range g.Participants {
		if prt == participant {
			return
		}
	}

	g.Participants = append(g.Participants, participant)
	g.Version++
}

// RemoveParticipant удаляет участника из группы и увеличивает версию
func (g *Group) RemoveParticipant(participant string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, prt := range g.Participants {
		if prt == participant {
			g.Participants = append(g.Participants[:i], g.Participants[i+1:]...)
			g.Version++
			return
		}
	}
}

// IsReady возвращает готовность группы
func (g *Group) IsReady() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Ready
}

// SetReady устанавливает готовность группы и увеличивает версию
func (g *Group) SetReady(ready bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Ready = ready
	g.Version++
}

// MarkDeleted помечает группу как удаленную и увеличивает версию
func (g *Group) MarkDeleted() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Deleted = true
	g.Version++
}

// GroupFromProto создает Group из protobuf сообщения
func GroupFromProto(p *p.Group) *Group {
	return &Group{
		ID:           p.Id,
		Goal:         p.Goal,
		MinSize:      p.MinSize,
		MaxSize:      p.MaxSize,
		Participants: p.Participants,
		Ready:        p.Ready,
		Version:      p.Version,
		Deleted:      p.Deleted,
	}
}

// GroupFromProtoBytes десериализует Group из байтов
func GroupFromProtoBytes(data []byte) (*Group, error) {
	protoGroup := &p.Group{}
	err := proto.Unmarshal(data, protoGroup)
	if err != nil {
		return nil, err
	}
	return GroupFromProto(protoGroup), nil
}
