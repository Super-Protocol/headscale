package entities

import (
	"crypto/md5"
	"encoding/binary"
	p "github.com/juanfont/headscale/gen/go/spnetwork/v1"
	"google.golang.org/protobuf/proto"
	"sort"
	"strconv"
	"sync"
)

type Node struct {
	ID         string
	Properties map[string]string
	Version    uint64
	Deleted    bool
	mu         sync.RWMutex
}

func NewNode(id string) *Node {
	return &Node{
		ID:         id,
		Properties: make(map[string]string),
		Version:    0,
		Deleted:    false,
	}
}

func (n *Node) GetID() string {
	return n.ID
}

func (n *Node) GetVersion() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Version
}

func (n *Node) IsDeleted() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Deleted
}

func (n *Node) GetHash() []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	h := md5.New()
	h.Write([]byte(n.ID))

	keys := make([]string, 0, len(n.Properties))
	for k := range n.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(n.Properties[k]))
	}

	err := binary.Write(h, binary.LittleEndian, n.Version)
	if err != nil {
		return nil
	}
	if n.Deleted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return h.Sum(nil)
}

func (n *Node) ToProto() *p.Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &p.Node{
		Id:         n.ID,
		Properties: n.Properties,
		Version:    n.Version,
		Deleted:    n.Deleted,
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
	value, ok := n.Properties["host"]
	if !ok {
		return "", false
	}
	return value, true
}

func (n *Node) SetHost(value string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Properties["host"] = value
}

func (n *Node) GetGossipPort() (uint16, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	value, ok := n.Properties["gossip_port"]
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
	n.Properties["gossip_port"] = strconv.Itoa(int(value))
}

func NodeFromProto(p *p.Node) *Node {
	return &Node{
		ID:         p.Id,
		Properties: p.Properties,
		Version:    p.Version,
		Deleted:    p.Deleted,
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
