package entities

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/google/uuid"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"time"
)

// LeadershipResign представляет сущность отказа от лидерства в сети
type LeadershipResign struct {
	id            string
	owner         string
	voteRequestId string
	dateUnix      int64
	version       uint64
	deleted       bool
}

// NewLeadershipResign создает новый экземпляр LeadershipResign
func NewLeadershipResign(owner, voteRequestId string) *LeadershipResign {
	return &LeadershipResign{
		id:            uuid.New().String(),
		owner:         owner,
		voteRequestId: voteRequestId,
		dateUnix:      time.Now().Unix(),
		version:       0,
		deleted:       false,
	}
}

// GetID возвращает идентификатор отказа от лидерства
func (lr *LeadershipResign) GetID() string {
	return lr.id
}

// GetOwner возвращает владельца отказа от лидерства
func (lr *LeadershipResign) GetOwner() string {
	return lr.owner
}

// GetVoteRequestID возвращает идентификатор связанного запроса на голосование
func (lr *LeadershipResign) GetVoteRequestID() string {
	return lr.voteRequestId
}

// GetDateUnix возвращает время создания в формате Unix timestamp
func (lr *LeadershipResign) GetDateUnix() int64 {
	return lr.dateUnix
}

// GetVersion возвращает версию отказа от лидерства
func (lr *LeadershipResign) GetVersion() uint64 {
	return lr.version
}

// IsDeleted возвращает признак удаления отказа от лидерства
func (lr *LeadershipResign) IsDeleted() bool {
	return lr.deleted
}

// GetHash возвращает хеш отказа от лидерства
func (lr *LeadershipResign) GetHash() []byte {
	h := md5.New()
	h.Write([]byte(lr.id))
	
	err := binary.Write(h, binary.LittleEndian, lr.version)
	if err != nil {
		return nil
	}
	
	return h.Sum(nil)
}

// ToProto преобразует LeadershipResign в протобуф-объект
func (lr *LeadershipResign) ToProto() *p.LeadershipResign {
	return &p.LeadershipResign{
		Id:            lr.id,
		Owner:         lr.owner,
		VoteRequestId: lr.voteRequestId,
		DateUnix:      lr.dateUnix,
		Version:       lr.version,
		Deleted:       lr.deleted,
	}
}

// Serialize сериализует LeadershipResign в байты
func (lr *LeadershipResign) Serialize() ([]byte, error) {
	return proto.Marshal(lr.ToProto())
}

// LeadershipResignFromProto создает LeadershipResign из протобуф-объекта
func LeadershipResignFromProto(p *p.LeadershipResign) *LeadershipResign {
	return &LeadershipResign{
		id:            p.Id,
		owner:         p.Owner,
		voteRequestId: p.VoteRequestId,
		dateUnix:      p.DateUnix,
		version:       p.Version,
		deleted:       p.Deleted,
	}
}

// LeadershipResignFromProtoBytes создает LeadershipResign из сериализованных байтов
func LeadershipResignFromProtoBytes(data []byte) (*LeadershipResign, error) {
	protoResign := &p.LeadershipResign{}
	err := proto.Unmarshal(data, protoResign)
	if err != nil {
		return nil, err
	}
	return LeadershipResignFromProto(protoResign), nil
}
