package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	message_map := map[int64]bool{}
	message_mutex := sync.Mutex{}
	node := maelstrom.NewNode()

	allMessages := func() []int64 {
		messages := []int64{}
		message_mutex.Lock()
		for key := range message_map {
			messages = append(messages, key)
		}
		message_mutex.Unlock()
		sort.Slice(messages, func(i, j int) bool {
			return messages[i] < messages[j]
		})
		return messages
	}

	syncMessages := func() {
		messages := allMessages()
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

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			syncMessages()
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int64(body["message"].(float64))
		message_mutex.Lock()
		message_map[message] = true
		message_mutex.Unlock()
		syncMessages()

		response := map[string]any{
			"type": "broadcast_ok",
		}
		return node.Reply(msg, response)
	})

	node.Handle("sync", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := body["messages"].([]interface{})
		message_mutex.Lock()
		for _, message := range messages {
			message_map[int64(message.(float64))] = true
		}
		message_mutex.Unlock()
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type":     "read_ok",
			"messages": allMessages(),
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
