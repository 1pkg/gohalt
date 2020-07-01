package gohalt

import (
	"context"
	"errors"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

type Storage interface {
	Get(context.Context) ([]byte, error)
	Set(context.Context, []byte) error
}

type smemory struct {
	buffer []byte
	lock   sync.Mutex
}

func NewStorageMemory() *smemory {
	return &smemory{}
}

func (s *smemory) Get(context.Context) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.buffer == nil {
		return nil, errors.New("storage has no stored data")
	}
	return s.buffer, nil
}

func (s *smemory) Set(ctx context.Context, data []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if data == nil {
		return errors.New("storage can't store empty data")
	}
	s.buffer = data
	return nil
}

type sbadger struct {
	memconnect Runnable
	db         *badger.DB
	key        []byte
}

func NewStorageBadger(path string, key string, cache time.Duration) sbadger {
	s := sbadger{key: []byte(key)}
	var lock sync.Mutex
	s.memconnect = cached(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		if err := s.close(ctx); err != nil {
			return err
		}
		return s.connect(ctx, path)
	})
	return s
}

func (s sbadger) Get(ctx context.Context) ([]byte, error) {
	if err := s.memconnect(ctx); err != nil {
		return nil, err
	}
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

func (s sbadger) Set(ctx context.Context, data []byte) error {
	if err := s.memconnect(ctx); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key, data)
	})
}

func (s sbadger) close(context.Context) error {
	return s.db.Close()
}

func (s sbadger) connect(ctx context.Context, path string) error {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return err
	}
	s.db = db
	return nil
}
