package entities

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/google/uuid"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"strconv"
	"sync"
)

type Node struct {
	ID         string
	properties map[string]string
	version    uint64
	deleted    bool
	mu         sync.RWMutex
}

func NewNode() *Node {
	return &Node{
		ID:         uuid.New().String(),
		properties: make(map[string]string),
		version:    0,
		deleted:    false,
	}
}

func (n *Node) GetID() string {
	return n.ID
}

func (n *Node) GetVersion() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.version
}

func (n *Node) IsDeleted() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.deleted
}

// SetDeleted устанавливает статус удаления ноды и увеличивает версию
func (n *Node) SetDeleted(deleted bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.deleted != deleted {
		n.deleted = deleted
		n.version++
	}
}

func (n *Node) GetHash() []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	h := md5.New()
	h.Write([]byte(n.ID))

	err := binary.Write(h, binary.LittleEndian, n.version)
	if err != nil {
		return nil
	}

	return h.Sum(nil)
}

func (n *Node) ToProto() *p.Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &p.Node{
		Id:         n.ID,
		Properties: n.properties,
		Version:    n.version,
		Deleted:    n.deleted,
	}
}

func (n *Node) Serialize() ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return proto.Marshal(n.ToProto())
}

func (n *Node) GetHost() (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	value, ok := n.properties["host"]
	if !ok {
		return "", false
	}
	return value, true
}

func (n *Node) SetHost(value string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.properties["host"] = value
	n.version++
}

func (n *Node) GetGossipPort() (uint16, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	value, ok := n.properties["gossip_port"]
	if !ok {
		return 0, false
	}
	intVal, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0, false
	}

	return uint16(intVal), true
}

func (n *Node) SetGossipPort(value uint16) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.properties["gossip_port"] = strconv.Itoa(int(value))
	n.version++
}

func (n *Node) GetUdpPingPort() (uint16, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	value, ok := n.properties["udp_ping_port"]
	if !ok {
		return 0, false
	}
	intVal, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0, false
	}

	return uint16(intVal), true
}

func (n *Node) SetUdpPingPort(value uint16) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.properties["udp_ping_port"] = strconv.Itoa(int(value))
	n.version++
}

func NodeFromProto(p *p.Node) *Node {
	return &Node{
		ID:         p.Id,
		properties: p.Properties,
		version:    p.Version,
		deleted:    p.Deleted,
	}
}

func NodeFromProtoBytes(data []byte) (*Node, error) {
	protoNode := &p.Node{}
	err := proto.Unmarshal(data, protoNode)
	if err != nil {
		return nil, err
	}
	return NodeFromProto(protoNode), nil
}
