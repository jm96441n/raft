FROM golang:1.22 AS builder

WORKDIR /app

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY main.go ./
COPY ./gen ./gen
COPY ./raft ./raft

RUN CGO_ENABLED=0 go build -o /app/bin/raft

FROM scratch

ARG PORT
ENV PORT=$PORT

ARG LEADER_ADDR
ENV LEADER_ADDR=$LEADER_ADDR

ARG SERVER_ADDRS
ENV SERVER_ADDRS=$SERVER_ADDRS

COPY --from=builder /app/bin/raft /raft

ENTRYPOINT ["/raft"]


