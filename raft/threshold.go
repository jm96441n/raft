package raft

import "sync/atomic"

type Threshold struct {
	wait      chan struct{}
	threshold atomic.Int32
}

func New(threshold int) *Threshold {
	t := &Threshold{
		wait: make(chan struct{}),
	}
	t.threshold.Store(int32(threshold))
	return t
}

func (t *Threshold) Go(fn func()) {
	go func() {
		defer func() {
			if t.threshold.Add(-1) == 0 {
				close(t.wait)
			}
		}()
		fn()
	}()
}

func (t *Threshold) Wait() {
	<-t.wait
}
