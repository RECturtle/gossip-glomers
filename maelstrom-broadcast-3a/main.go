package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	m := &MaelNode{maelstromNode: n}

	n.Handle("broadcast", m.broadcastHandler)
	n.Handle("read", m.readHandler)
	n.Handle("topology", m.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MaelNode struct {
	maelstromNode *maelstrom.Node
	messages      []int
	nodeTopology  map[string][]string
}

type TopologyMessage struct {
	Topology map[string][]string `json:"topology"`
}

type Broadcaster struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func (m *MaelNode) broadcastHandler(msg maelstrom.Message) error {
	var body Broadcaster
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	m.messages = append(m.messages, body.Message)

	return m.maelstromNode.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (m *MaelNode) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return m.maelstromNode.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": m.messages,
	})
}

func (m *MaelNode) topologyHandler(msg maelstrom.Message) error {
	var t TopologyMessage
	if err := json.Unmarshal(msg.Body, &t); err != nil {
		return err
	}

	m.nodeTopology = t.Topology

	return m.maelstromNode.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
