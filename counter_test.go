package redis_rate_limiter

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCounterStrategy_Run(t *testing.T) {
	tt := []struct {
		name       string
		runs       int64
		request    *Request
		lastResult *Result
		lastErr    string
		advance    time.Duration
	}{
		{
			name: "returns Allow for requests under limit",
			request: &Request{
				Key:      "some-user",
				Limit:    100,
				Duration: time.Minute,
			},
			lastResult: &Result{
				State:         Allow,
				TotalRequests: 50,
			},
			runs: 50,
		},
		{
			name: "returns Deny for requests over limit",
			request: &Request{
				Key:      "some-user",
				Limit:    100,
				Duration: time.Minute,
			},
			lastResult: &Result{
				State:         Deny,
				TotalRequests: 100,
			},
			runs: 100,
		},
		{
			name: "expires and starts again as it goes over the TTL",
			request: &Request{
				Key:      "some-user",
				Limit:    100,
				Duration: time.Minute,
			},
			lastResult: &Result{
				State:         Allow,
				TotalRequests: 40,
			},
			runs:    100,
			advance: time.Second,
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			server, err := miniredis.Run()
			require.NoError(t, err)
			defer server.Close()

			client := redis.NewClient(&redis.Options{
				Addr: server.Addr(),
			})

			counter := NewCounterStrategy(client)
			var lastResult *Result
			var lastErr error

			for x := int64(0); x < ts.runs; x++ {
				lastResult, lastErr = counter.Run(context.Background(), ts.request)
				if ts.advance != 0 {
					server.FastForward(ts.advance)
				}
			}

			assert.Equal(t, ts.lastResult, lastResult)

			if ts.lastErr != "" {
				assert.EqualError(t, lastErr, ts.lastErr)
			} else {
				assert.NoError(t, lastErr)
			}
		})
	}
}
