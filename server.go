package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
)

type server struct {
	pb.UnimplementedRaftServer
	logger *slog.Logger

	// leader specific
	followers  []*follower
	leaderAddr pb.RaftClient
	isLeader   bool

	// on all
	entries         []*pb.LogEntry
	committedVals   map[int]string
	term            int
	commitIndex     int
	lastApplied     int
	heartbeatTicker *time.Ticker
}

type follower struct {
	client    pb.RaftClient
	nextIndex int
}

func NewServer(logger *slog.Logger, leaderAddr pb.RaftClient, followers []*follower, envVars env) *server {
	srv := &server{
		logger:        logger,
		followers:     followers,
		leaderAddr:    leaderAddr,
		committedVals: make(map[int]string),
		entries:       []*pb.LogEntry{},
		term:          0,
	}

	// handle actual leader election later
	if envVars.port == 8080 {
		ticker := time.NewTicker(300 * time.Millisecond)
		srv.isLeader = true
		srv.heartbeatTicker = ticker
	}

	return srv
}

func heartbeat(ctx context.Context, srv *server) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-srv.heartbeatTicker.C:
			if srv.isLeader {
				_, err := srv.AppendEntries(ctx, &pb.AppendEntriesRequest{})
				if err != nil {
					srv.logger.Error("failed to send heartbeat", slog.Any("err", err))
				}
			}
		}
	}
}

func (s *server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.logger.Info("getting value")
	if !s.isLeader {
		s.logger.Info("forwarding request to leader")
		// forward request to leader
		return s.leaderAddr.Read(ctx, in)
	}

	val, ok := s.committedVals[int(in.Key)]
	if !ok {
		return &pb.ReadResponse{}, nil
	}
	s.logger.Info("got value", slog.Any("val", val), slog.Any("key", in.Key))
	return &pb.ReadResponse{Value: val}, nil
}

func (s *server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.logger.Info("writing value")
	if !s.isLeader {
		s.logger.Info("forwarding request to leader")
		// forward request to leader
		return s.leaderAddr.Write(ctx, in)
	}

	s.logger.Info("input", slog.Any("key", in.Key), slog.Any("value", in.Value))

	entry := &pb.LogEntry{
		Term:    int32(s.term),
		Command: fmt.Sprintf("%d:%s", in.Key, in.Value),
	}
	s.entries = append(s.entries, entry)
	s.logger.Info("appended uncommitted entry", slog.Any("entry", entry))

	_, err := s.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:     int32(s.term),
		LeaderId: "",
		Entries:  []*pb.LogEntry{entry},
	})
	if err != nil {
		s.logger.Error("failed to append entries", slog.Any("err", err))
		return nil, err
	}
	s.heartbeatTicker.Reset(300 * time.Millisecond)
	s.logger.Info("appended entries")
	return &pb.WriteResponse{Success: true}, nil
}

func (s *server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.isLeader {
		err := s.leaderAppendEntries(ctx, in)
		if err != nil {
			return &pb.AppendEntriesResponse{Term: int32(s.term), Success: false}, err
		}

		return &pb.AppendEntriesResponse{Term: int32(s.term), Success: true}, nil
	} else {
		s.entries = append(s.entries, in.Entries...)
		if in.LeaderCommit > int32(s.commitIndex) {
			for i := s.commitIndex + 1; i <= int(in.LeaderCommit); i++ {
				key, val := parseCommand(s.entries[i])
				s.committedVals[key] = val
				s.commitIndex = i
				s.logger.Info("committed entry", slog.Any("entry", s.entries[i]))
			}
		}

		s.logger.Info("appended entries in follower")
		return &pb.AppendEntriesResponse{Term: int32(s.term), Success: true}, nil
	}
}

func (s *server) leaderAppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) error {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	doneCh := make(chan struct{})
	for _, conn := range s.followers {
		go s.followerAppendEntries(ctx, conn, in, doneCh)
	}

	go waitForDone(doneCh, wg)

	wg.Wait()

	for _, entry := range in.Entries {
		key, val := parseCommand(entry)
		s.committedVals[key] = val
		s.logger.Info("committed entry", slog.Any("entry", entry))
		in.LeaderCommit++
	}
	return nil
}

func waitForDone(doneCh chan struct{}, wg *sync.WaitGroup) {
	i := 0
	for range doneCh {
		i += 1
		if i == 1 {
			wg.Done()
		}
		if i == 2 {
			break
		}
	}
	close(doneCh)
}

func (s *server) followerAppendEntries(ctx context.Context, conn *follower, in *pb.AppendEntriesRequest, doneCh chan struct{}) error {
	prevIndex := max(0, conn.nextIndex-1)
	var prevTerm int32 = 0
	if prevIndex > 0 {
		prevTerm = s.entries[prevIndex].Term
	}
	resp, err := conn.client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         int32(s.term),
		LeaderId:     "",
		PrevLogIndex: int32(prevIndex),
		PrevLogTerm:  prevTerm,
		Entries:      in.Entries,
		LeaderCommit: int32(s.commitIndex),
	})
	if err != nil {
		return err
	}

	if resp.Success {
		conn.nextIndex = conn.nextIndex + len(in.Entries)
	}

	doneCh <- struct{}{}
	return nil
}

func parseCommand(entry *pb.LogEntry) (int, string) {
	var (
		key int
		val string
	)

	// TODO: check for error
	_, _ = fmt.Sscanf(entry.Command, "%d:%s", &key, &val)
	return key, val
}

func (s *server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.logger.Info("requesting vote")
	return &pb.RequestVoteResponse{}, nil
}
