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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/jm96441n/raft/gen/go/raft/v1"
)

type env struct {
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

	leaderAddr, err := createLeaderConn(envVars)
	if err != nil {
		logger.Error("failed to create leader connection", slog.Any("err", err))
		os.Exit(1)
	}

	followerConns, err := createFollowerConns(envVars)
	if err != nil {
		logger.Error("failed to create client connections", slog.Any("err", err))
		os.Exit(1)
	}

	srv := NewServer(logger, leaderAddr, followerConns, envVars)

	if srv.isLeader {
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

func parseEnvVars() (env, error) {
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		return env{}, err
	}

	return env{
		port:        port,
		leaderAddr:  os.Getenv("LEADER_ADDR"),
		serverAddrs: strings.Split(os.Getenv("SERVER_ADDRS"), ","),
	}, nil
}

func createLeaderConn(envVars env) (pb.RaftClient, error) {
	conn, err := grpc.NewClient(envVars.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewRaftClient(conn), nil
}

func createFollowerConns(envVars env) ([]follower, error) {
	var conns []follower
	for _, addr := range envVars.serverAddrs {
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
