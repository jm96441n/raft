package raft

import (
	pb "github.com/jm96441n/raft/gen/go/raft/v1"
)

type logStore interface {
	Get(index int) *pb.LogEntry
	GetRange(start, end int) []*pb.LogEntry
	Append(entry ...*pb.LogEntry)
	NextIndex() int
	Length() int
}

var _ = logStore(&InMemStore{})

type InMemStore struct {
	entries []*pb.LogEntry
}

func NewInMemStore() *InMemStore {
	return &InMemStore{
		entries: []*pb.LogEntry{},
	}
}

func (s *InMemStore) Get(index int) *pb.LogEntry {
	if index < 0 || index >= len(s.entries) {
		return nil
	}
	return s.entries[index]
}

func (s *InMemStore) GetRange(start, end int) []*pb.LogEntry {
	if start < 0 || start >= len(s.entries) {
		return nil
	}
	if end < 0 || end > len(s.entries) {
		return nil
	}
	return s.entries[start:end]
}

func (s *InMemStore) Append(entry ...*pb.LogEntry) {
	s.entries = append(s.entries, entry...)
}

func (s *InMemStore) LastIndex() int {
	return len(s.entries) - 1
}

func (s *InMemStore) NextIndex() int {
	return len(s.entries) + 1
}

func (s *InMemStore) Length() int {
	return len(s.entries)
}
