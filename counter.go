package redis_rate_limiter

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

var (
	_ Strategy = &counterStrategy{}
)

func NewCounterStrategy(client *redis.Client) *counterStrategy {
	return &counterStrategy{
		client: client,
	}
}

type counterStrategy struct {
	client *redis.Client
}

func (c counterStrategy) Run(ctx context.Context, r *Request) (*Result, error) {
	p := c.client.Pipeline()
	incrResult := p.Incr(ctx, r.Key)
	ttlResult := p.TTL(ctx, r.Key)
	_, err := p.Exec(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute increment to key %v", r.Key)
	}

	if d, err := ttlResult.Result(); err != nil || d == 0 {
		if err := c.client.Expire(ctx, r.Key, r.Duration).Err(); err != nil {
			return nil, errors.Wrapf(err, "failed to set an expiration to key %v", r.Key)
		}
	}

	totalRequests, err := incrResult.Result()

	if err != nil {
		return nil, errors.Wrapf(err, "failed to increment key %v", r.Key)
	}

	if totalRequests >= r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: totalRequests,
		}, nil
	}

	return &Result{
		State:         Allow,
		TotalRequests: totalRequests,
	}, nil
}
