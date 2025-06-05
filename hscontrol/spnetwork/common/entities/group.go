package entities

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/google/uuid"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

// Participant представляет участника группы с временем присоединения
type Participant struct {
	ID           string
	JoinDateUnix int64
}

type Group struct {
	ID               string
	Goal             string
	Participants     []Participant
	Version          uint64
	Deleted          bool
	CreationDateUnix int64
	mu               sync.RWMutex
}

func NewGroup() *Group {
	return &Group{
		ID:               uuid.New().String(),
		Participants:     make([]Participant, 0),
		Version:          0,
		Deleted:          false,
		CreationDateUnix: time.Now().Unix(),
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

// GetHash вычисляет хеш для группы, используя только ID и Version
func (g *Group) GetHash() []byte {
	g.mu.RLock()
	defer g.mu.RUnlock()

	h := md5.New()
	h.Write([]byte(g.ID))

	err := binary.Write(h, binary.LittleEndian, g.Version)
	if err != nil {
		return nil
	}

	return h.Sum(nil)
}

// ToProto конвертирует Group в protobuf сообщение
func (g *Group) ToProto() *p.Group {
	g.mu.RLock()
	defer g.mu.RUnlock()

	protoParticipants := make([]*p.Participant, len(g.Participants))
	for i, participant := range g.Participants {
		protoParticipants[i] = &p.Participant{
			Id:           participant.ID,
			JoinDateUnix: participant.JoinDateUnix,
		}
	}

	return &p.Group{
		Id:               g.ID,
		Goal:             g.Goal,
		Participants:     protoParticipants,
		Version:          g.Version,
		Deleted:          g.Deleted,
		CreationDateUnix: g.CreationDateUnix,
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

// GetParticipants возвращает список идентификаторов участников группы
func (g *Group) GetParticipants() []Participant {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Participants
}

// GetParticipantObjects возвращает список объектов участников группы
func (g *Group) GetParticipantObjects() []Participant {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]Participant, len(g.Participants))
	copy(result, g.Participants)
	return result
}

// AddParticipant добавляет участника в группу и увеличивает версию
func (g *Group) AddParticipant(participantID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Проверяем, что участник еще не добавлен
	for _, prt := range g.Participants {
		if prt.ID == participantID {
			return
		}
	}

	// Создаем нового участника с текущим временем
	newParticipant := Participant{
		ID:           participantID,
		JoinDateUnix: time.Now().Unix(),
	}

	g.Participants = append(g.Participants, newParticipant)
	g.Version++
}

// RemoveParticipant удаляет участника из группы и увеличивает версию
func (g *Group) RemoveParticipant(participantID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, prt := range g.Participants {
		if prt.ID == participantID {
			g.Participants = append(g.Participants[:i], g.Participants[i+1:]...)
			g.Version++
			return
		}
	}
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
	participants := make([]Participant, len(p.Participants))
	for i, participant := range p.Participants {
		participants[i] = Participant{
			ID:           participant.Id,
			JoinDateUnix: participant.JoinDateUnix,
		}
	}

	return &Group{
		ID:               p.Id,
		Goal:             p.Goal,
		Participants:     participants,
		Version:          p.Version,
		Deleted:          p.Deleted,
		CreationDateUnix: p.CreationDateUnix,
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

// GetParticipantByID возвращает участника по его ID и флаг, найден ли участник
func (g *Group) GetParticipantByID(participantID string) (Participant, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, participant := range g.Participants {
		if participant.ID == participantID {
			return participant, true
		}
	}

	return Participant{}, false
}

// GetParticipantJoinDate возвращает время присоединения участника и флаг, найден ли участник
func (g *Group) GetParticipantJoinDate(participantID string) (int64, bool) {
	participant, found := g.GetParticipantByID(participantID)
	if !found {
		return 0, false
	}
	return participant.JoinDateUnix, true
}

// GetCreationDate возвращает время создания группы
func (g *Group) GetCreationDate() int64 {
	return g.CreationDateUnix
}
