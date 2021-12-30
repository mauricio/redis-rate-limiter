package redis_rate_limiter

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	forwardedFor = "X-Forwarded-For"
)

var (
	_ http.Handler = &handleFuncWrapper{}
)

type handleFuncWrapper struct {
	handleFunc http.HandlerFunc
}

func (h *handleFuncWrapper) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.handleFunc(writer, request)
}

func TestNewHTTPHeadersExtractor(t *testing.T) {
	tt := []struct {
		name               string
		builder            func(r *http.Request)
		totalRequests      int
		config             func(client *redis.Client, now func() time.Time) *RateLimiterConfig
		advance            time.Duration
		lastResponseStatus int
		matchedHeaders     map[string]string
	}{
		{
			name: "a request that is not rate limited",
			builder: func(r *http.Request) {
				r.Header.Set(forwardedFor, "10.10.10.10")
			},
			totalRequests:      10,
			lastResponseStatus: http.StatusOK,
			advance:            time.Second,
			matchedHeaders: map[string]string{
				rateLimitingState:         "Allow",
				rateLimitingTotalRequests: "10",
			},
			config: func(client *redis.Client, now func() time.Time) *RateLimiterConfig {
				return &RateLimiterConfig{
					Extractor:   NewHTTPHeadersExtractor(forwardedFor),
					Strategy:    NewCounterStrategy(client, now),
					Expiration:  time.Minute,
					MaxRequests: 50,
				}
			},
		},
		{
			name: "a request that is rate limited",
			builder: func(r *http.Request) {
				r.Header.Set(forwardedFor, "10.10.10.10")
			},
			totalRequests:      55,
			lastResponseStatus: http.StatusTooManyRequests,
			advance:            time.Second,
			matchedHeaders: map[string]string{
				rateLimitingState:         "Deny",
				rateLimitingTotalRequests: "50",
			},
			config: func(client *redis.Client, now func() time.Time) *RateLimiterConfig {
				return &RateLimiterConfig{
					Extractor:   NewHTTPHeadersExtractor(forwardedFor),
					Strategy:    NewSortedSetCounterStrategy(client, now),
					Expiration:  time.Minute,
					MaxRequests: 50,
				}
			},
		},
		{
			name: "a request that fails because of missing headers",
			builder: func(r *http.Request) {
				r.Header.Set("User-Agent", "Netscape Navigator")
			},
			totalRequests:      1,
			lastResponseStatus: http.StatusBadRequest,
			advance:            time.Second,
			config: func(client *redis.Client, now func() time.Time) *RateLimiterConfig {
				return &RateLimiterConfig{
					Extractor:   NewHTTPHeadersExtractor(forwardedFor),
					Strategy:    NewSortedSetCounterStrategy(client, now),
					Expiration:  time.Minute,
					MaxRequests: 50,
				}
			},
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
			defer client.Close()

			now := time.Now()
			nowGenerator := func() time.Time {
				return now
			}

			handler := func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "<html><body>Request received!</body></html>")
			}

			wrapper := NewHTTPRateLimiterHandler(&handleFuncWrapper{handleFunc: handler}, ts.config(client, nowGenerator))

			var lastResponse *http.Response

			for x := 0; x < ts.totalRequests; x++ {
				req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
				ts.builder(req)

				w := httptest.NewRecorder()
				wrapper.ServeHTTP(w, req)
				lastResponse = w.Result()

				now = now.Add(ts.advance)
			}

			assert.Equal(t, ts.lastResponseStatus, lastResponse.StatusCode)
			for key, value := range ts.matchedHeaders {
				got := lastResponse.Header.Get(key)
				assert.Equalf(t, value, got, "expected header %v to have value %v but was %v", key, value, got)
			}
		})
	}
}
