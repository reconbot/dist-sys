package main

import (
	"reflect"
	"testing"
)

func TestMapWrapper(t *testing.T) {
	message_log := MessageLog{}

	// Set a value and check that it's retrieved correctly
	message_log.Add(42)
	got := message_log.Has(42)
	if !got {
		t.Errorf("mw.Has(42) = %v, want %v", got, true)
	}

	message_log.Add(41)
	keys := message_log.Keys()
	if !reflect.DeepEqual(keys, []int64{41, 42}) {
		t.Errorf("mw.Keys() = %v, want %v", keys, []int64{41, 42})
	}

}
