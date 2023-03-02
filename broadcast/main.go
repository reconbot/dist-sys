package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Logs struct {
	node_name   string
	message_map map[int64]bool
}

type MessageLog struct {
	message_map sync.Map
}

func (m *MessageLog) Add(value int64) {
	m.message_map.Store(value, true)
}

func (m *MessageLog) Has(value int64) bool {
	exists, ok := m.message_map.Load(value)
	if !ok {
		return false
	}
	if exists == nil {
		return false
	}
	return exists.(bool)
}

func (m *MessageLog) Keys() []int64 {
	messages := []int64{}
	m.message_map.Range(func(key any, value any) bool {
		if value.(bool) {
			messages = append(messages, key.(int64))
		}
		return true
	})
	sort.Slice(messages, func(i, j int) bool {
		return messages[i] < messages[j]
	})
	return messages
}

type SyncMessage struct {
	Messages []int64 `json:"messages"`
}

type BroadcastMessage struct {
	Message int64 `json:"messages"`
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
