package entities

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/google/uuid"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"time"
)

type VoteKind string

const (
	VoteKindNetworkLeadership VoteKind = "network_leadership"
)

type Vote struct {
	id        string
	requestId string
	kind      VoteKind
	target    string
	value     int
	dateUnix  int64
	version   uint64
	deleted   bool
	voter     string // ID ноды, которая проголосовала
}

// NewVote создает новый экземпляр Vote
func NewVote(kind VoteKind, target string, value int, voter string) *Vote {
	return &Vote{
		id:       uuid.New().String(),
		kind:     kind,
		target:   target,
		value:    value,
		dateUnix: time.Now().Unix(),
		version:  0,
		deleted:  false,
		voter:    voter,
	}
}

// NewVoteForRequest создает новый экземпляр Vote для конкретного запроса
func NewVoteForRequest(request *VoteRequest, value int, voter string) *Vote {
	return &Vote{
		id:        uuid.New().String(),
		requestId: request.GetID(),
		kind:      request.GetKind(),
		target:    request.GetTarget(),
		value:     value,
		dateUnix:  time.Now().Unix(),
		version:   0,
		deleted:   false,
		voter:     voter,
	}
}

// GetID возвращает идентификатор голоса
func (v *Vote) GetID() string {
	return v.id
}

// GetRequestID возвращает идентификатор запроса, связанного с голосом
func (v *Vote) GetRequestID() string {
	return v.requestId
}

// SetRequestID устанавливает идентификатор запроса
func (v *Vote) SetRequestID(requestId string) {
	v.requestId = requestId
	v.version++
}

// GetKind возвращает тип голоса
func (v *Vote) GetKind() string {
	return string(v.kind)
}

// GetTarget возвращает цель голоса
func (v *Vote) GetTarget() string {
	return v.target
}

// GetValue возвращает значение голоса
func (v *Vote) GetValue() int {
	return v.value
}

// GetDateUnix возвращает время голоса в формате Unix timestamp
func (v *Vote) GetDateUnix() int64 {
	return v.dateUnix
}

// GetVersion возвращает версию голоса
func (v *Vote) GetVersion() uint64 {
	return v.version
}

// IsDeleted возвращает признак удаления голоса
func (v *Vote) IsDeleted() bool {
	return v.deleted
}

// GetVoter возвращает ID ноды, отдавшей голос
func (v *Vote) GetVoter() string {
	return v.voter
}

// GetHash возвращает хеш голоса
func (v *Vote) GetHash() []byte {
	h := md5.New()
	h.Write([]byte(v.id))

	err := binary.Write(h, binary.LittleEndian, v.version)
	if err != nil {
		return nil
	}

	return h.Sum(nil)
}

// ToProto преобразует Vote в протобуф-объект
func (v *Vote) ToProto() *p.Vote {
	return &p.Vote{
		Id:        v.id,
		RequestId: v.requestId,
		Kind:      string(v.kind),
		Target:    v.target,
		Value:     int32(v.value),
		DateUnix:  v.dateUnix,
		Version:   v.version,
		Deleted:   v.deleted,
		Voter:     v.voter,
	}
}

// Serialize сериализует Vote в байты
func (v *Vote) Serialize() ([]byte, error) {
	return proto.Marshal(v.ToProto())
}

// VoteFromProto создает Vote из протобуф-объекта
func VoteFromProto(p *p.Vote) *Vote {
	return &Vote{
		id:        p.Id,
		requestId: p.RequestId,
		kind:      VoteKind(p.Kind),
		target:    p.Target,
		value:     int(p.Value),
		dateUnix:  p.DateUnix,
		version:   p.Version,
		deleted:   p.Deleted,
		voter:     p.Voter,
	}
}

// VoteFromProtoBytes создает Vote из сериализованных байтов
func VoteFromProtoBytes(data []byte) (*Vote, error) {
	protoVote := &p.Vote{}
	err := proto.Unmarshal(data, protoVote)
	if err != nil {
		return nil, err
	}
	return VoteFromProto(protoVote), nil
}
