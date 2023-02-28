package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	messages := []int64{}
	node := maelstrom.NewNode()
	init := false

	broadcastMessage := func(message int64) {
		if !init {
			return
		}
		for _, node_id := range node.NodeIDs() {
			if node_id == node.ID() {
				return
			}
			err := node.Send(node_id, map[string]any{
				"type":    "broadcast",
				"message": message,
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	node.Handle("init", func(msg maelstrom.Message) error {
		init = true
		return nil
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int64(body["message"].(float64))

		messages = append(messages, message)
		broadcastMessage(message)

		response := map[string]any{
			"type": "broadcast_ok",
		}
		return node.Reply(msg, response)
	})

	node.Handle("broadcast_ok", func(msg maelstrom.Message) error { return nil })

	node.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
