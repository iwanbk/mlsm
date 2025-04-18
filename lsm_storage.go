package main

type LsmStorageState struct {
	// current memtable
	memtable *MemTable
}

type LsmStorage struct {
	state *LsmStorageState
	opt   *LsmStorageOption
}

type LsmStorageOption struct {
	// memtable size limit is not a hard limit
	// we should freeze it at best effort
	memtableSizeLimit int
}

func NewLsmStorage(opt *LsmStorageOption) *LsmStorage {
	return &LsmStorage{
		state: &LsmStorageState{
			memtable: NewMemTable(),
		},
		opt: opt,
	}
}

func (l *LsmStorage) Put(key, value []byte) error {
	// Insert the key-value pair into the memtable
	return l.state.memtable.Put(key, value)
}
func (l *LsmStorage) Get(key []byte) ([]byte, bool) {
	// Get the value for the key from the memtable
	return l.state.memtable.Get(key)
}

func (l *LsmStorage) Delete(key []byte) error {
	// Delete the key-value pair from the memtable
	return l.state.memtable.Put(key, nil)
}

type MiniLSM struct {
	lsm *LsmStorage
}
