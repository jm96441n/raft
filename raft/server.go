package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxHeartbeat = 300
	minHeartbeat = 150
)

type server struct {
	pb.UnimplementedRaftServer
	logger *slog.Logger

	// leader specific
	followers  []*follower
	leaderAddr pb.RaftClient
	IsLeader   bool

	// on all
	entries         logStore
	committedVals   map[int]string
	term            int
	commitIndex     int
	lastApplied     int
	heartbeatTicker *time.Ticker
	heartbeatTime   time.Duration
}

type follower struct {
	client    pb.RaftClient
	addr      string
	nextIndex int
}

type ServerConfig struct {
	Logger        *slog.Logger
	LeaderAddr    string
	FollowerAddrs []string
	IsLeader      bool
	LogStore      logStore
}

func NewServer(cfg ServerConfig) (*server, error) {
	leader, err := createLeaderConn(cfg.LeaderAddr)
	if err != nil {
		return nil, err
	}

	followers, err := createFollowerConns(cfg.FollowerAddrs)
	if err != nil {
		return nil, err
	}

	srv := &server{
		logger:        cfg.Logger,
		followers:     followers,
		leaderAddr:    leader,
		committedVals: make(map[int]string),
		entries:       cfg.LogStore,
		term:          0,
	}

	// handle actual leader election later
	if cfg.IsLeader {
		sleepTime := rand.IntN((maxHeartbeat+1)-minHeartbeat) + minHeartbeat
		ticker := time.NewTicker(time.Duration(sleepTime) * time.Millisecond)
		srv.IsLeader = true
		srv.heartbeatTime = time.Duration(sleepTime) * time.Millisecond
		srv.heartbeatTicker = ticker
	}

	return srv, nil
}

func createLeaderConn(leaderAddr string) (pb.RaftClient, error) {
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewRaftClient(conn), nil
}

func createFollowerConns(followerAddrs []string) ([]*follower, error) {
	var conns []*follower
	for _, addr := range followerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		follower := &follower{
			client:    pb.NewRaftClient(conn),
			addr:      addr,
			nextIndex: 0,
		}
		conns = append(conns, follower)
	}
	return conns, nil
}

func Heartbeat(ctx context.Context, srv *server) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-srv.heartbeatTicker.C:
			if srv.IsLeader {
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

	val, ok := s.committedVals[int(in.Key)]
	if !ok {
		return &pb.ReadResponse{}, nil
	}
	s.logger.Info("got value", slog.Any("val", val), slog.Any("key", in.Key))
	return &pb.ReadResponse{Value: val}, nil
}

func (s *server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.logger.Info("writing value")
	if !s.IsLeader {
		s.logger.Info("forwarding request to leader")
		// forward request to leader
		return s.leaderAddr.Write(ctx, in)
	}

	s.logger.Info("input", slog.Any("key", in.Key), slog.Any("value", in.Value))

	entry := &pb.LogEntry{
		Term:    int32(s.term),
		Command: fmt.Sprintf("%d:%s", in.Key, in.Value),
		Index:   int64(s.entries.NextIndex()),
	}
	s.entries.Append(entry)
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
	s.heartbeatTicker.Reset(s.heartbeatTime)
	s.logger.Info("appended entries")
	return &pb.WriteResponse{Success: true}, nil
}

func (s *server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.IsLeader {
		err := s.leaderAppendEntries(ctx, in)
		if err != nil {
			return &pb.AppendEntriesResponse{Term: int32(s.term), Success: false}, err
		}

		return &pb.AppendEntriesResponse{Term: int32(s.term), Success: true}, nil
	} else {
		s.entries.Append(in.Entries...)
		if len(in.Entries) != 0 {
			s.logger.Info("appended uncommitted entries in follower", slog.Any("entries", in.Entries))
		}

		if in.LeaderCommit > int64(s.commitIndex) {
			for i := s.commitIndex; i < int(in.LeaderCommit); i++ {
				entry := s.entries.Get(i)
				s.logger.Info("entry to commit", slog.Any("entry", entry))
				key, val := parseCommand(entry)
				s.committedVals[key] = val
				s.commitIndex = int(entry.Index)
				s.logger.Info("committed entry", slog.Any("entry", entry))
			}
		}

		return &pb.AppendEntriesResponse{Term: int32(s.term), Success: true}, nil
	}
}

func (s *server) leaderAppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) error {
	threshold := New(len(s.followers) / 2)
	for _, conn := range s.followers {
		conn := conn
		threshold.Go(s.replicateToFollowers(ctx, conn, in))
	}

	threshold.Wait()

	for _, entry := range in.Entries {
		key, val := parseCommand(entry)
		s.committedVals[key] = val
		s.logger.Info("committed entry", slog.Any("entry", entry))
		s.commitIndex = int(entry.Index)
	}
	return nil
}

func (s *server) replicateToFollowers(ctx context.Context, conn *follower, in *pb.AppendEntriesRequest) func() {
	return func() {
		entries := []*pb.LogEntry{}
		if len(in.Entries) != 0 {
			entries = s.entries.GetRange(conn.nextIndex, int(in.Entries[len(in.Entries)-1].Index))
			s.logger.Info("entries replicated", slog.Any("entries", entries))
		}
		prevIndex := max(0, conn.nextIndex-1)
		var prevTerm int32 = 0
		if prevIndex > 0 {
			prevTerm = s.entries.Get(prevIndex).Term
		}
		resp, err := conn.client.AppendEntries(ctx, &pb.AppendEntriesRequest{
			Term:         int32(s.term),
			LeaderId:     "",
			PrevLogIndex: int64(prevIndex),
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: int64(s.commitIndex),
		})
		if err != nil {
			s.logger.Error("failed to append entries", slog.Any("err", err), slog.Any("conn Addr", conn.client))
			return
		}

		if resp.Success && len(in.Entries) != 0 {
			conn.nextIndex = int(in.Entries[len(in.Entries)-1].Index)
		}
	}
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
