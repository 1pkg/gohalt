package gohalt

import (
	"errors"
	"sync"
)

type Storage interface {
	Get() ([]byte, error)
	Set([]byte) error
}

type smemory struct {
	buffer []byte
	mutex  sync.Mutex
}

func NewStorageMemory() *smemory {
	return &smemory{}
}

func (s *smemory) Get() ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.buffer == nil {
		return nil, errors.New("storage has no stored data")
	}
	return s.buffer, nil
}

func (s *smemory) Set(data []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if data == nil {
		return errors.New("storage can't store empty data")
	}
	s.buffer = data
	return nil
}
