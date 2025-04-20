package main

import (
	"sync/atomic"

	"github.com/huandu/skiplist"
)

// basic LSM memtable using skiplist
type MemTable struct {
	kv        skiplist.SkipList
	approxLen atomic.Uint64
}

func NewMemTable() *MemTable {
	return &MemTable{
		kv: *skiplist.New(skiplist.Byte),
	}
}

func (m *MemTable) Put(key, value []byte) error {
	// Insert the key-value pair into the skiplist
	if len(value) > 0 {
		m.approxLen.Add(uint64(len(key) + len(value)))
	}

	m.kv.Set(key, value)
	return nil
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	// Get the value for the key from the skiplist
	v, ok := m.kv.GetValue(key)
	if !ok || v == nil {
		return nil, false
	}
	return v.([]byte), true
}

func (m *MemTable) ApproxLen() uint64 {
	return m.approxLen.Load()
}
