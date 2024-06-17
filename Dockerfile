FROM golang:1.22 as builder

WORKDIR /app

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY main.go server.go ./
COPY ./gen ./gen

RUN CGO_ENABLED=0 go build -o /app/raft

FROM scratch

ARG PORT
ENV PORT=$PORT

ARG LEADER_ADDR
ENV LEADER_ADDR=$LEADER_ADDR

ARG SERVER_ADDRS
ENV SERVER_ADDRS=$SERVER_ADDRS

COPY --from=builder /app/raft /app/raft

ENTRYPOINT ["/app/raft"]


