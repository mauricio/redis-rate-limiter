package redis_rate_limiter

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"time"
)

var (
	_ Strategy = &counterStrategy{}
)

const (
	keyThatDoesNotExist = -2
	keyWithoutExpire    = -1
)

func NewCounterStrategy(client *redis.Client, now func() time.Time) *counterStrategy {
	return &counterStrategy{
		client: client,
		now:    now,
	}
}

type counterStrategy struct {
	client *redis.Client
	now    func() time.Time
}

// Run this implementation uses a simple counter with an expiration set to the rate limit duration.
// This implementation is funtional but not very effective if you have to deal with bursty traffic as
// it will still allow a client to burn through it's full limit quickly once the key expires.
func (c *counterStrategy) Run(ctx context.Context, r *Request) (*Result, error) {

	// a pipeline in redis is a way to send multiple commands that will all be run together.
	// this is not a transaction and there are many ways in which these commands could fail
	// (only the first, only the second) so we have to make sure all errors are handled, this
	// is a network performance optimization.

	// here we try to get the current value and also try to set an expiration on it
	getPipeline := c.client.Pipeline()
	getResult := getPipeline.Get(ctx, r.Key)
	ttlResult := getPipeline.TTL(ctx, r.Key)

	if _, err := getPipeline.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, errors.Wrapf(err, "failed to execute pipeline with get and ttl to key %v", r.Key)
	}

	var ttlDuration time.Duration

	// we want to make sure there is always an expiration set on the key, so on every
	// increment we check again to make sure it has a TTl and if it doesn't we add one.
	// a duration of -1 means that the key has no expiration so we need to make sure there
	// is one set, this should, most of the time, happen when we increment for the
	// first time but there could be cases where we fail at the previous commands so we should
	// check for the TTL on every request.
	// a duration of -2 means that the key does not exist, given we're already here we should set an expiration
	// to it anyway as it means this is a new key that will be incremented below.
	if d, err := ttlResult.Result(); err != nil || d == keyWithoutExpire || d == keyThatDoesNotExist {
		ttlDuration = r.Duration
		if err := c.client.Expire(ctx, r.Key, r.Duration).Err(); err != nil {
			return nil, errors.Wrapf(err, "failed to set an expiration to key %v", r.Key)
		}
	} else {
		ttlDuration = d
	}

	expiresAt := c.now().Add(ttlDuration)

	if total, err := getResult.Uint64(); err != nil && errors.Is(err, redis.Nil) {

	} else if total >= r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: total,
			ExpiresAt:     expiresAt,
		}, nil
	}

	incrResult := c.client.Incr(ctx, r.Key)

	totalRequests, err := incrResult.Uint64()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to increment key %v", r.Key)
	}

	if totalRequests > r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: totalRequests,
			ExpiresAt:     expiresAt,
		}, nil
	}

	return &Result{
		State:         Allow,
		TotalRequests: totalRequests,
		ExpiresAt:     expiresAt,
	}, nil
}
