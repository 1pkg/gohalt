package gohalt

import (
	"errors"
	"sync"

	badger "github.com/dgraph-io/badger/v2"
)

type Storage interface {
	Get() ([]byte, error)
	Set([]byte) error
	Close() error
}

type smemory struct {
	buffer []byte
	lock   sync.Mutex
}

func NewStorageMemory() *smemory {
	return &smemory{}
}

func (s *smemory) Get() ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.buffer == nil {
		return nil, errors.New("storage has no stored data")
	}
	return s.buffer, nil
}

func (s *smemory) Set(data []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if data == nil {
		return errors.New("storage can't store empty data")
	}
	s.buffer = data
	return nil
}

func (s *smemory) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.buffer = nil
	return nil
}

type sbadger struct {
	db  *badger.DB
	key []byte
}

func NewStorageBadger(path string, key string) (sbadger, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	return sbadger{
		db:  db,
		key: []byte(key),
	}, err
}

func (s sbadger) Get() ([]byte, error) {
	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.key)
		if err == nil {
			result, err = item.ValueCopy(nil)
		}
		return err
	})
	return result, err
}

func (s sbadger) Set(data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key, data)
	})
}

func (s sbadger) Close() error {
	return s.db.Close()
}
