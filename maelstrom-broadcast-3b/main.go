package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message interface {
	TopologyBody | BroadcastBody | map[string]any
}

type MaelNode struct {
	maelstromNode *maelstrom.Node
	maelNodeID    string
	maelMessages  []int
	maelTopo      map[string][]string
	maelMutex     sync.Mutex
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	m := MaelNode{maelstromNode: n, maelNodeID: n.ID()}
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		body, err := UnmarshalMessage(msg, body)
		if err != nil {
			return err
		}

		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		body, err := UnmarshalMessage(msg, body)
		if err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = uuid.New()
		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		body, err := UnmarshalMessage(msg, body)
		if err != nil {
			return err
		}

		// Add message to node's messages
		m.maelMutex.Lock()
		if slices.Contains(m.maelMessages, body.Message) {
			m.maelMutex.Unlock()
			return nil
		}
		m.maelMessages = append(m.maelMessages, body.Message)
		m.maelMutex.Unlock()

		// forward message to all other nodes
		if err := forwardMessage(&m, msg); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		body, err := UnmarshalMessage(msg, body)
		if err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = m.maelMessages
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyBody
		body, err := UnmarshalMessage(msg, body)
		if err != nil {
			return err
		}

		m.maelTopo = body.Topology

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func forwardMessage(maelNode *MaelNode, msg maelstrom.Message) error {
	for node := range maelNode.maelTopo {
		if node == msg.Src || node == maelNode.maelNodeID {
			continue
		}
		if err := maelNode.maelstromNode.Send(node, msg.Body); err != nil {
			return err
		}
	}
	return nil
}

func UnmarshalMessage[B Message](msg maelstrom.Message, body B) (B, error) {
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		var emptyBody B
		return emptyBody, err
	}
	return body, nil
}
