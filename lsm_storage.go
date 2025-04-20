package main

import (
	"sync"
)

type LsmStorage struct {
	// memtable Lock
	mtLock sync.RWMutex
	// current memtable
	memtable *MemTable

	// immutable memtables
	// from latest to earliest
	immMemtables []*MemTable

	opt *LsmStorageOption
}

type LsmStorageOption struct {
	// memtable size limit is not a hard limit
	// we should freeze it at best effort
	memtableSizeLimit int
}

func NewLsmStorage(opt *LsmStorageOption) *LsmStorage {
	return &LsmStorage{
		memtable:     NewMemTable(),
		immMemtables: make([]*MemTable, 0),
		opt:          opt,
	}
}

// Put inserts the key-value pair into the memtable
func (l *LsmStorage) Put(key, value []byte) error {
	approxLen, err := func() (uint64, error) {
		l.mtLock.RLock()
		defer l.mtLock.RUnlock()

		err := l.memtable.Put(key, value)
		approxLen := l.memtable.ApproxLen()

		return approxLen, err
	}()
	if err != nil {
		return err
	}
	if approxLen >= uint64(l.opt.memtableSizeLimit) {
		l.tryFreezeMemtable()
	}
	return nil
}

// Get the value for the key from the memtable
func (l *LsmStorage) Get(key []byte) ([]byte, bool) {
	l.mtLock.RLock()
	defer l.mtLock.RUnlock()
	return l.memtable.Get(key)
}

// Delete the key-value pair from the memtable
func (l *LsmStorage) Delete(key []byte) error {
	return l.Put(key, nil)
}

// Freeze the current memtable and create a new one
func (l *LsmStorage) tryFreezeMemtable() {
	newMemtable := NewMemTable()

	l.mtLock.Lock()
	defer l.mtLock.Unlock()

	// check if the current memtable is already frozen
	// by another goroutine
	if l.memtable.ApproxLen() < uint64(l.opt.memtableSizeLimit) {
		return
	}

	l.immMemtables = append(l.immMemtables, l.memtable)
	l.memtable = newMemtable
}

type MiniLSM struct {
	lsm *LsmStorage
}
