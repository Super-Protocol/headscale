package entities

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/google/uuid"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
)

// DefaultVoteRequestTimeoutSecs - время ожидания голосования по умолчанию (30 минут)
const DefaultVoteRequestTimeoutSecs int64 = 1800

// VoteRequest представляет запрос на голосование
type VoteRequest struct {
	id            string
	kind          VoteKind
	target        string
	dateUnix      int64
	version       uint64
	deleted       bool
	timeoutSecs   int64
	quorumReached bool
}

// NewVoteRequest создает новый экземпляр VoteRequest
func NewVoteRequest(kind VoteKind, target string, dateUnix int64) *VoteRequest {
	return &VoteRequest{
		id:            uuid.New().String(),
		kind:          kind,
		target:        target,
		dateUnix:      dateUnix,
		version:       0,
		deleted:       false,
		timeoutSecs:   DefaultVoteRequestTimeoutSecs,
		quorumReached: false,
	}
}

// NewVoteRequestWithTimeout создает новый экземпляр VoteRequest с указанным таймаутом
func NewVoteRequestWithTimeout(kind VoteKind, target string, dateUnix int64, timeoutSecs int64) *VoteRequest {
	return &VoteRequest{
		id:            uuid.New().String(),
		kind:          kind,
		target:        target,
		dateUnix:      dateUnix,
		version:       0,
		deleted:       false,
		timeoutSecs:   timeoutSecs,
		quorumReached: false,
	}
}

// GetID возвращает идентификатор запроса на голосование
func (vr *VoteRequest) GetID() string {
	return vr.id
}

// GetKind возвращает тип запроса на голосование
func (vr *VoteRequest) GetKind() VoteKind {
	return vr.kind
}

// GetTarget возвращает цель запроса на голосование
func (vr *VoteRequest) GetTarget() string {
	return vr.target
}

// GetDateUnix возвращает время создания запроса в формате Unix timestamp
func (vr *VoteRequest) GetDateUnix() int64 {
	return vr.dateUnix
}

// GetTimeoutSecs возвращает время ожидания голосования в секундах
func (vr *VoteRequest) GetTimeoutSecs() int64 {
	return vr.timeoutSecs
}

// GetVersion возвращает версию запроса на голосование
func (vr *VoteRequest) GetVersion() uint64 {
	return vr.version
}

// IsDeleted возвращает признак удаления запроса на голосование
func (vr *VoteRequest) IsDeleted() bool {
	return vr.deleted
}

// SetDeleted устанавливает статус удаления запроса на голосование и увеличивает версию
func (vr *VoteRequest) SetDeleted(deleted bool) {
	if vr.deleted != deleted {
		vr.deleted = deleted
		vr.version++
	}
}

// IsQuorumReached возвращает признак достижения кворума для запроса на голосование
func (vr *VoteRequest) IsQuorumReached() bool {
	return vr.quorumReached
}

// SetQuorumReached устанавливает статус достижения кворума и увеличивает версию
func (vr *VoteRequest) SetQuorumReached(quorumReached bool) {
	if vr.quorumReached != quorumReached {
		vr.quorumReached = quorumReached
		vr.version++
	}
}

// IsActive проверяет, является ли запрос на голосование активным в данный момент
// (не удален и не истек срок таймаута)
func (vr *VoteRequest) IsActive(currentTimeUnix int64) bool {
	if vr.deleted {
		return false
	}

	// Проверяем, не истек ли таймаут
	return currentTimeUnix <= vr.dateUnix+vr.timeoutSecs
}

// GetHash возвращает хеш запроса на голосование
func (vr *VoteRequest) GetHash() []byte {
	h := md5.New()
	h.Write([]byte(vr.id))

	err := binary.Write(h, binary.LittleEndian, vr.version)
	if err != nil {
		return nil
	}

	return h.Sum(nil)
}

// ToProto преобразует VoteRequest в протобуф-объект
func (vr *VoteRequest) ToProto() *p.VoteRequest {
	return &p.VoteRequest{
		Id:            vr.id,
		Kind:          string(vr.kind),
		Target:        vr.target,
		DateUnix:      vr.dateUnix,
		Version:       vr.version,
		Deleted:       vr.deleted,
		TimeoutSecs:   vr.timeoutSecs,
		QuorumReached: vr.quorumReached,
	}
}

// Serialize сериализует VoteRequest в байты
func (vr *VoteRequest) Serialize() ([]byte, error) {
	return proto.Marshal(vr.ToProto())
}

// VoteRequestFromProto создает VoteRequest из протобуф-объекта
func VoteRequestFromProto(p *p.VoteRequest) *VoteRequest {
	return &VoteRequest{
		id:            p.Id,
		kind:          VoteKind(p.Kind),
		target:        p.Target,
		dateUnix:      p.DateUnix,
		version:       p.Version,
		deleted:       p.Deleted,
		timeoutSecs:   p.TimeoutSecs,
		quorumReached: p.QuorumReached,
	}
}

// VoteRequestFromProtoBytes создает VoteRequest из сериализованных байтов
func VoteRequestFromProtoBytes(data []byte) (*VoteRequest, error) {
	protoVoteRequest := &p.VoteRequest{}
	err := proto.Unmarshal(data, protoVoteRequest)
	if err != nil {
		return nil, err
	}
	return VoteRequestFromProto(protoVoteRequest), nil
}
