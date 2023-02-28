package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	nanoid "github.com/matoous/go-nanoid/v2"
)

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		body := map[string]any{
			"type": "generate_ok",
		}
		id, err := nanoid.New()
		if err != nil {
			log.Fatal(err)
		}
		body["id"] = id
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
