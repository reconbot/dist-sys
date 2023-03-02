package main

import (
	"sort"
	"sync"
)

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
