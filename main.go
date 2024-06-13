package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		logger.Error("failed to parse port", slog.Any("err", err))
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Error("failed to construct listener on port", slog.Any("err", err), slog.Any("port", port))
	}

	leaderAddrStr := os.Getenv("LEADER_ADDR")

	conn, err := grpc.NewClient(leaderAddrStr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to create client connection", slog.Any("err", err))
		os.Exit(1)
	}
	leaderAddr := pb.NewRaftClient(conn)

	serverAddrs := parseServerAddrs()
	clientConns, err := createClientConns(serverAddrs)
	if err != nil {
		logger.Error("failed to create client connections", slog.Any("err", err))
		os.Exit(1)
	}

	srv := &server{
		logger:        logger,
		followers:     clientConns,
		leaderAddr:    leaderAddr,
		committedVals: make(map[int]string),
		entries:       []*pb.LogEntry{},
		term:          0,
	}

	// handle actual leader election later
	if port == 8080 {
		ticker := time.NewTicker(300 * time.Millisecond)
		srv.isLeader = true
		srv.heartbeatTicker = ticker
		go heartbeat(context.Background(), srv)
	}

	s := grpc.NewServer()
	pb.RegisterRaftServer(s, srv)
	reflection.Register(s)

	logger.Info("server listening", slog.Any("addr", listener.Addr().String()))
	if err := s.Serve(listener); err != nil {
		logger.Error("failed to serve", slog.Any("err", err))
		os.Exit(1)
	}
}

type follower struct {
	client    pb.RaftClient
	nextIndex int
}

func parseServerAddrs() []string {
	// server addrs are comma separated
	addrString := os.Getenv("SERVER_ADDRS")
	addrs := strings.Split(addrString, ",")
	return addrs
}

func createClientConns(addrs []string) ([]follower, error) {
	var conns []follower
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		follower := follower{
			client:    pb.NewRaftClient(conn),
			nextIndex: 0,
		}
		conns = append(conns, follower)
	}
	return conns, nil
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

type server struct {
	pb.UnimplementedRaftServer
	logger *slog.Logger

	// leader specific
	followers  []follower
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

func parseCommand(entry *pb.LogEntry) (int, string) {
	var (
		key int
		val string
	)

	// TODO: check for error
	_, _ = fmt.Sscanf(entry.Command, "%d:%s", &key, &val)
	return key, val
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
	s.logger.Info("calling append entries")
	_, err := s.AppendEntries(ctx, &pb.AppendEntriesRequest{})
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
		for idx, conn := range s.followers {
			entries := s.entries[conn.nextIndex:]
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
				Entries:      entries,
				LeaderCommit: int32(s.commitIndex),
			})
			if err != nil {
				return nil, err
			}

			if resp.Success {
				s.followers[idx].nextIndex = conn.nextIndex + len(entries)
				for _, entry := range entries {
					key, val := parseCommand(entry)
					s.committedVals[key] = val
					s.logger.Info("committed entry", slog.Any("entry", entry))
					in.LeaderCommit++
				}
			}
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

		return &pb.AppendEntriesResponse{Term: int32(s.term), Success: true}, nil
	}
}

func (s *server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.logger.Info("requesting vote")
	return &pb.RequestVoteResponse{}, nil
}
