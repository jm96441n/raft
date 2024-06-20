package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
	"github.com/jm96441n/raft/raft"
)

type Config struct {
	port        int
	serverAddrs []string
	leaderAddr  string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	envVars, err := parseEnvVars()
	if err != nil {
		logger.Error("failed to parse env vars", slog.Any("err", err))
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", envVars.port))
	if err != nil {
		logger.Error("failed to construct listener on port", slog.Any("err", err), slog.Any("port", envVars.port))
	}

	srv, err := raft.NewNode(raft.NodeConfig{
		Logger:        logger,
		LeaderAddr:    envVars.leaderAddr,
		FollowerAddrs: envVars.serverAddrs,
		IsLeader:      envVars.port == 8080,
		LogStore:      raft.NewInMemStore(),
	})
	if err != nil {
		logger.Error("failed to create server", slog.Any("err", err))
	}

	if srv.IsLeader {
		go raft.Heartbeat(context.Background(), srv)
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

func parseEnvVars() (Config, error) {
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		port:        port,
		leaderAddr:  os.Getenv("LEADER_ADDR"),
		serverAddrs: strings.Split(os.Getenv("SERVER_ADDRS"), ","),
	}, nil
}
