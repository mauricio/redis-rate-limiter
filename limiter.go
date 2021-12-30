package redis_rate_limiter

import (
	"context"
	"time"
)

// Request defines a request that needs to be checked if it will be rate limited or not.
// The `Key` is the identifier you're using for the client making calls. This could be a user/account ID if the user is
// signed into your application, the IP of the client making requests (this might not be reliable if you're not behind a
// proxy like Cloudflare, as clients can try to spoof IPs). The `Key` should be the same for multiple calls of the
// same client so we can correctly identify that this is the same app calling anywhere.
// `Limit` is the amount of requests the client is allowed to make over the `Duration` period. If you set this to
// 100 and `Duration` to `1m` you'd have at most 100 requests over a minute.
type Request struct {
	Key      string
	Limit    uint64
	Duration time.Duration
}

// State is the result of evaluating the rate limit, either `Deny` or `Allow` a request.
type State int64

const (
	Deny  State = 0
	Allow       = 1
)

// Result represents the response to a check if a client should be rate limited or not. The `State` will be either
// `Allow` or `Deny`, `TotalRequests` holds the number of requests this specific caller has already made over
// the current period of time and `ExpiresAt` defines when the rate limit will expire/roll over for clients that
// have gone over the limit.
type Result struct {
	State         State
	TotalRequests uint64
	ExpiresAt     time.Time
}

// Strategy is the interface the rate limit implementations must implement to be used, it takes a `Request` and
// returns a `Result` and an `error`, any errors the rate-limiter finds should be bubbled up so the code can make a
// decision about what it wants to do with the request.
type Strategy interface {
	Run(ctx context.Context, r *Request) (*Result, error)
}
