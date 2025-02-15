package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message interface {
	TopologyBody | BroadcastBody | map[string]any
}

type MaelNode struct {
	maelstromNode *maelstrom.Node
	maelMessages  []int
	maelTopo      map[string][]string
}

type BroadcastBody struct {
	Message int `json:"message"`
}

type TopologyBody struct {
	Topology map[string][]string `json:"typology"`
}

func main() {
	n := maelstrom.NewNode()
	m := MaelNode{maelstromNode: n}
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

		m.maelMessages = append(m.maelMessages, body.Message)
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

func UnmarshalMessage[B Message](msg maelstrom.Message, body B) (B, error) {
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		var emptyBody B
		return emptyBody, err
	}
	return body, nil
}
