package main

import (
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Logs struct {
	node_name   string
	message_map map[int64]bool
}

type SyncMessage struct {
	Messages []int64 `json:"messages"`
}

type BroadcastMessage struct {
	Message int64 `json:"message"`
}

func syncMessages(node *maelstrom.Node, message_log *MessageLog) {
	messages := message_log.Keys()
	if len(messages) == 0 {
		return
	}
	for _, node_id := range node.NodeIDs() {
		if node_id == node.ID() {
			continue
		}
		err := node.Send(node_id, map[string]any{
			"type":     "sync",
			"messages": messages,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	node := maelstrom.NewNode()
	message_log := &MessageLog{}

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			syncMessages(node, message_log)
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("got broadcast %v", body)

		message := body.Message
		message_log.Add(message)
		syncMessages(node, message_log)

		response := map[string]any{
			"type": "broadcast_ok",
		}
		return node.Reply(msg, response)
	})

	node.Handle("sync", func(msg maelstrom.Message) error {
		var body SyncMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := body.Messages
		for _, message := range messages {
			message_log.Add(message)
		}
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type":     "read_ok",
			"messages": message_log.Keys(),
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
