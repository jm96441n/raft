package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxHeartbeat = 300
	minHeartbeat = 150
)

type nodeState int

const (
	FOLLOWER nodeState = iota
	LEADER
	CANDIDATE
)

type node struct {
	pb.UnimplementedRaftServer
	logger *slog.Logger

	// leader specific
	servers map[string]*server

	leaderAddr pb.RaftClient
	state      nodeState

	// on all
	id            string
	entries       logStore
	committedVals map[int]string
	term          int
	commitIndex   int
	lastApplied   int

	heartbeatTicker *time.Ticker
	heartbeatTime   time.Duration
	votedFor        string
}

type server struct {
	client     pb.RaftClient
	addr       string
	nextIndex  int
	matchIndex int
}

type NodeConfig struct {
	Logger      *slog.Logger
	Addr        string
	ServerAddrs []string
	IsLeader    bool
	LogStore    logStore
}

func NewNode(cfg NodeConfig) (*node, error) {
	servers, err := createServerConns(cfg.ServerAddrs)
	if err != nil {
		return nil, err
	}

	id, _, _ := strings.Cut(cfg.Addr, ":")

	node := &node{
		logger:        cfg.Logger,
		servers:       servers,
		committedVals: make(map[int]string),
		entries:       cfg.LogStore,
		id:            id,
		term:          0,
	}

	// handle actual leader election later
	heartbeatTime := rand.IntN((maxHeartbeat+1)-minHeartbeat) + minHeartbeat
	heartbeatTimer := time.NewTicker(time.Duration(heartbeatTime) * time.Millisecond)
	node.heartbeatTime = time.Duration(heartbeatTime) * time.Millisecond
	node.heartbeatTicker = heartbeatTimer

	return node, nil
}

func (n *node) IsLeader() bool {
	return n.state == LEADER
}

func (n *node) Run(ctx context.Context) {
	n.logger.Info("running node")
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.heartbeatTicker.C:
			switch n.state {
			case LEADER:
				n.Heartbeat(ctx)
			default:
				n.logger.Info("getting votes")
				n.getVotes(ctx)
			}
		}
	}
}

func (n *node) getVotes(ctx context.Context) {
	n.logger.Info("getting votes")
	if n.votedFor != "" {
		return
	}

	n.logger.Info("has not voted yet")
	threshold := NewThreshold(len(n.servers) / 2)
	for _, conn := range n.servers {
		threshold.Go(func() {
			conn.client.RequestVote(ctx, &pb.RequestVoteRequest{CandidateId: n.id})
		})
	}
	threshold.Wait()
	n.state = LEADER
	n.leaderAppendEntries(ctx, &pb.AppendEntriesRequest{})
}

func createServerConns(serverAddrs []string) (map[string]*server, error) {
	conns := make(map[string]*server, len(serverAddrs))
	for _, addr := range serverAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		server := &server{
			client:    pb.NewRaftClient(conn),
			addr:      addr,
			nextIndex: 0,
		}
		id, _, _ := strings.Cut(addr, ":")
		conns[id] = server
	}
	return conns, nil
}

func (srv *node) Heartbeat(ctx context.Context) {
	_, err := srv.AppendEntries(ctx, &pb.AppendEntriesRequest{})
	if err != nil {
		srv.logger.Error("failed to send heartbeat", slog.Any("err", err))
	}
}

func (n *node) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadResponse, error) {
	n.logger.Info("getting value")

	val, ok := n.committedVals[int(in.Key)]
	if !ok {
		return &pb.ReadResponse{}, nil
	}
	n.logger.Info("got value", slog.Any("val", val), slog.Any("key", in.Key))
	return &pb.ReadResponse{Value: val}, nil
}

func (n *node) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteResponse, error) {
	n.logger.Info("writing value")
	if !n.IsLeader() {
		n.logger.Info("forwarding request to leader")
		// forward request to leader
		return n.leaderAddr.Write(ctx, in)
	}

	n.logger.Info("input", slog.Any("key", in.Key), slog.Any("value", in.Value))

	entry := &pb.LogEntry{
		Term:    int32(n.term),
		Command: fmt.Sprintf("%d:%s", in.Key, in.Value),
		Index:   int64(n.entries.NextIndex()),
	}

	_, err := n.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:     int32(n.term),
		LeaderId: "",
		Entries:  []*pb.LogEntry{entry},
	})
	if err != nil {
		n.logger.Error("failed to append entries", slog.Any("err", err))
		return nil, err
	}
	n.heartbeatTicker.Reset(n.heartbeatTime)
	n.logger.Info("appended entries")
	return &pb.WriteResponse{Success: true}, nil
}

func (n *node) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if ctx.Err() != nil {
		return &pb.AppendEntriesResponse{Term: int32(n.term), Success: false}, ctx.Err()
	}

	if n.votedFor != "" && !n.IsLeader() {
		n.state = FOLLOWER
		n.leaderAddr = n.servers[n.votedFor].client
		n.votedFor = ""
	}

	if n.IsLeader() {
		err := n.leaderAppendEntries(ctx, in)
		if err != nil {
			return &pb.AppendEntriesResponse{Term: int32(n.term), Success: false}, err
		}

		return &pb.AppendEntriesResponse{Term: int32(n.term), Success: true}, nil
	}

	n.followerApendEntries(in)

	return &pb.AppendEntriesResponse{Term: int32(n.term), Success: true}, nil
}

func (n *node) leaderAppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) error {
	if len(in.Entries) != 0 {
		n.entries.Append(in.Entries...)
		n.logger.Info("appended uncommitted entry in leader", slog.Any("entries", in.Entries))
	}

	threshold := NewThreshold(len(n.servers) / 2)
	for _, conn := range n.servers {
		conn := conn
		threshold.Go(n.replicateToFollowers(ctx, conn, in))
	}

	threshold.Wait()

	for _, entry := range in.Entries {
		key, val := parseCommand(entry)
		n.committedVals[key] = val
		n.logger.Info("committed entry", slog.Any("entry", entry))
		n.commitIndex = int(entry.Index)
	}
	return nil
}

func (n *node) replicateToFollowers(ctx context.Context, conn *server, in *pb.AppendEntriesRequest) func() {
	return func() {
		entries := []*pb.LogEntry{}
		if len(in.Entries) != 0 {
			entries = n.entries.GetRange(conn.nextIndex, int(in.Entries[len(in.Entries)-1].Index))
			n.logger.Info("entries replicated", slog.Any("entries", entries))
		}
		prevIndex := max(0, conn.nextIndex-1)
		var prevTerm int32 = 0
		if prevIndex > 0 {
			prevTerm = n.entries.Get(prevIndex).Term
		}
		resp, err := conn.client.AppendEntries(ctx, &pb.AppendEntriesRequest{
			Term:         int32(n.term),
			LeaderId:     "",
			PrevLogIndex: int64(prevIndex),
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: int64(n.commitIndex),
		})
		if err != nil {
			panic(err)
			n.logger.Error("failed to append entries", slog.Any("err", err), slog.Any("conn Addr", conn.client))
			return
		}

		if resp.Success && len(in.Entries) != 0 {
			conn.nextIndex = int(in.Entries[len(in.Entries)-1].Index)
			conn.matchIndex = conn.nextIndex
		}
	}
}

func (n *node) followerApendEntries(in *pb.AppendEntriesRequest) {
	n.heartbeatTicker.Reset(n.heartbeatTime)
	n.entries.Append(in.Entries...)
	if len(in.Entries) != 0 {
		n.logger.Info("appended uncommitted entries in follower", slog.Any("entries", in.Entries))
	}

	if in.LeaderCommit > int64(n.commitIndex) {
		for i := n.commitIndex; i < int(in.LeaderCommit); i++ {
			entry := n.entries.Get(i)
			n.logger.Info("entry to commit", slog.Any("entry", entry))
			key, val := parseCommand(entry)
			n.committedVals[key] = val
			n.commitIndex = int(entry.Index)
			n.logger.Info("committed entry", slog.Any("entry", entry))
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

func (n *node) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if n.votedFor == "" {
		n.votedFor = in.CandidateId
		return &pb.RequestVoteResponse{VoteGranted: true}, nil
	}

	return &pb.RequestVoteResponse{VoteGranted: false}, nil
}
