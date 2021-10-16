package redis_rate_limiter

import (
	"context"
	"time"
)

type Request struct {
	Key      string
	Limit    int64
	Duration time.Duration
}

type State int64

const (
	Deny  State = 0
	Allow       = 1
)

type Result struct {
	State         State
	TotalRequests int64
}

type Strategy interface {
	Run(ctx context.Context, r *Request) (*Result, error)
}
