package rask

// The retry budget and backoff, on a fake clock: the real thing takes two minutes (S3.total-budget, RUN_SLOW=1).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/braskai/rask-sdk-go/v3/api"
)

// fakeClock replaces a client's clock and sleep, and records the waits.
type fakeClock struct {
	now   time.Time
	waits []time.Duration
}

func (f *fakeClock) install(c *Client) {
	c.now = func() time.Time { return f.now }
	c.sleep = func(_ context.Context, d time.Duration) error {
		f.waits = append(f.waits, d)
		f.now = f.now.Add(d)
		if len(f.waits) > 1000 {
			return errors.New("fake clock: retrying without end") // Fail the test rather than hang it.
		}
		return nil
	}
	c.rand = func() float64 { return 0.5 } // Jitter factor exactly 1.
}

func hermeticClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	t.Setenv(envAPIKey, "")
	t.Setenv(envBaseURL, "")
	t.Setenv(envLog, "")
	c, err := New(WithAPIKey("k"), WithBaseURL(baseURL))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func alwaysStatus(t *testing.T, status int, retryAfter string, requests *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"code":"temporarily_unavailable","title":"T","status":503}`)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestRetryBudgetEndsAtOneHundredTwentySeconds(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 503, "", &requests)
	c := hermeticClient(t, server.URL)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)

	_, err := c.Account.GetMe(context.Background())
	var serverError *ServerError
	if !errors.As(err, &serverError) || serverError.Status != 503 {
		t.Fatalf("got %T %v", err, err)
	}

	// The schedule the design asks for: 0.5 s growing by 1.5 up to 30 s, stopping before 120 s would pass.
	var want []time.Duration
	total := time.Duration(0)
	for attempt := 0; ; attempt++ {
		wait := min(time.Duration(float64(500*time.Millisecond)*math.Pow(1.5, float64(attempt))), 30*time.Second)
		if total+wait > 120*time.Second {
			break
		}
		want = append(want, wait)
		total += wait
	}
	if fmt.Sprint(clock.waits) != fmt.Sprint(want) {
		t.Errorf("waits %v, want %v", clock.waits, want)
	}
	if elapsed := clock.now.Sub(time.Unix(1000, 0)); elapsed > 120*time.Second {
		t.Errorf("retried for %s", elapsed)
	}
	if got := int(requests.Load()); got != len(want)+1 {
		t.Errorf("%d requests, want %d", got, len(want)+1)
	}
}

func TestJitterStaysWithinItsBand(t *testing.T) {
	c := hermeticClient(t, "http://127.0.0.1:1")
	for _, tc := range []struct {
		random float64
		want   time.Duration
	}{{0, 375 * time.Millisecond}, {0.5, 500 * time.Millisecond}, {0.999999, 625 * time.Millisecond}} {
		c.rand = func() float64 { return tc.random }
		if got := c.backoff(0); got.Round(time.Millisecond) != tc.want {
			t.Errorf("random %v: backoff %s, want %s", tc.random, got, tc.want)
		}
	}
	c.rand = func() float64 { return 0.999999 }
	if got := c.backoff(100); got != 30*time.Second {
		t.Errorf("backoff is not capped: %s", got)
	}
}

func TestRetryAfterBeyondTheBudgetEndsRetrying(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 429, "130", &requests)
	c := hermeticClient(t, server.URL)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	_, err := c.Account.GetMe(context.Background())
	var limited *RateLimitError
	if !errors.As(err, &limited) || limited.RetryAfter != 130*time.Second {
		t.Fatalf("got %T %v", err, err)
	}
	if got := requests.Load(); got != 1 || len(clock.waits) != 0 {
		t.Fatalf("%d requests, waits %v", got, clock.waits)
	}
}

func TestRetryAfterOutranksTheBackoff(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 503, "7", &requests)
	c := hermeticClient(t, server.URL)
	c.maxRetries = 1
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	_, _ = c.Account.GetMe(context.Background())
	if len(clock.waits) != 1 || clock.waits[0] != 7*time.Second {
		t.Fatalf("waits %v", clock.waits)
	}
}

func TestRetryAfterForms(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{
		"5":                             5 * time.Second,
		"0":                             0,
		"1.5":                           1500 * time.Millisecond,
		"Thu, 05 Nov 2026 12:00:30 GMT": 30 * time.Second,
		"Thu, 05 Nov 2026 11:00:00 GMT": 0,
	} {
		got, ok := parseRetryAfter(http.Header{"Retry-After": {value}}, now)
		if !ok || got != want {
			t.Errorf("%q: %s %v, want %s", value, got, ok, want)
		}
	}
	for _, value := range []string{"", "soon", "-1"} {
		if _, ok := parseRetryAfter(http.Header{"Retry-After": {value}}, now); ok {
			t.Errorf("%q: expected no value", value)
		}
	}
}

// A number beyond a Duration must not come out as whatever the processor gives: a negative wait on amd64, the largest
// one on arm64. Neither is a wait the server meant, and the code after it adds to the value.
func TestRetryAfterBeyondADurationIsCapped(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	const longest = (1 << 31) * time.Second
	for _, value := range []string{"2147483648", "2147483649", "9223372037", "1e20", "1e300", "Inf", "+Infinity", "Fri, 31 Dec 9999 23:59:59 GMT"} {
		got, ok := parseRetryAfter(http.Header{"Retry-After": {value}}, now)
		if !ok || got != longest {
			t.Errorf("%q: %s %v, want %s", value, got, ok, longest)
		}
	}
	if got, ok := parseRetryAfter(http.Header{"Retry-After": {"2147483647"}}, now); !ok || got != longest-time.Second {
		t.Errorf("a number under the cap changed: %s %v", got, ok)
	}
	for _, value := range []string{"NaN", "-Inf"} {
		if got, ok := parseRetryAfter(http.Header{"Retry-After": {value}}, now); ok {
			t.Errorf("%q: expected no value, got %s", value, got)
		}
	}
}

// The same number on the wire: the call ends at once with the server's wait on the error, because no budget holds it.
func TestRetryAfterBeyondADurationEndsRetrying(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 429, "1e20", &requests)
	c := hermeticClient(t, server.URL)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	c.now = func() time.Time { // Time passes between two readings, so the wait is added to a time that is not zero.
		clock.now = clock.now.Add(time.Millisecond)
		return clock.now
	}
	_, err := c.Account.GetMe(context.Background())
	var limited *RateLimitError
	if !errors.As(err, &limited) || limited.RetryAfter != (1<<31)*time.Second {
		t.Fatalf("got %T %v", err, err)
	}
	if got := requests.Load(); got != 1 || len(clock.waits) != 0 {
		t.Fatalf("%d requests, waits %v", got, clock.waits)
	}
}

// A failure that no retry can fix ends a wait as it ends a single call. Taken for an outage, it would come back hours
// later as a timeout, without its cause.
func TestWaitEndsOnATransportFailureThatCannotPass(t *testing.T) {
	loop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, req.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer loop.Close()
	untrusted := httptest.NewUnstartedServer(http.NotFoundHandler())
	untrusted.Config.ErrorLog = log.New(io.Discard, "", 0) // It would report every handshake this test has refused.
	untrusted.StartTLS()
	defer untrusted.Close()
	for name, baseURL := range map[string]string{"redirect loop": loop.URL, "untrusted certificate": untrusted.URL} {
		c := hermeticClient(t, baseURL)
		clock := &fakeClock{now: time.Unix(1000, 0)}
		clock.install(c)
		_, err := c.Jobs.Wait(context.Background(), "j")
		var connection *ConnectionError
		if !errors.As(err, &connection) || connection.Attempts != 1 || len(clock.waits) != 0 {
			t.Errorf("%s: got %T %v after %d waits", name, err, err, len(clock.waits))
		}
	}
}

// What may pass still does: a wait outlasts a connection that drops.
func TestWaitOutlastsADroppedConnection(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = io.WriteString(w, `{"object":"job","id":"j","status":"succeeded"}`)
	}))
	defer server.Close()
	c := hermeticClient(t, server.URL)
	c.maxRetries = 0 // The call itself gives up, so it is the wait that must go on.
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	if _, err := c.Jobs.Wait(context.Background(), "j"); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || len(clock.waits) != 1 {
		t.Fatalf("%d requests, waits %v", requests.Load(), clock.waits)
	}
}

func TestMaxRetriesZeroTurnsRetryingOff(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 503, "", &requests)
	t.Setenv(envAPIKey, "")
	t.Setenv(envBaseURL, "")
	c, err := New(WithAPIKey("k"), WithBaseURL(server.URL), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Account.GetMe(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("%d requests", got)
	}
}

func TestCallerCancelEndsTheBackoffAndIsReadable(t *testing.T) {
	var requests atomic.Int64
	server := alwaysStatus(t, 503, "60", &requests)
	c := hermeticClient(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := c.Account.GetMe(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %T %v", err, err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("the backoff outlived the context")
	}
}

// One call's budget is 120 s, but a wait lasts hours: an outage longer than the budget must not end it.
func TestWaitOutlastsAnOutageLongerThanOneCallsBudget(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) <= 2 {
			w.Header().Set("Retry-After", "130") // Past the budget, so each GetJob gives up at once.
			w.WriteHeader(503)
			return
		}
		_, _ = io.WriteString(w, `{"object":"job","id":"j","status":"succeeded"}`)
	}))
	defer server.Close()
	c := hermeticClient(t, server.URL)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	response, err := c.Jobs.Wait(context.Background(), "j")
	if err != nil {
		t.Fatal(err)
	}
	if response.Data.Status != api.JobStatusSucceeded || requests.Load() != 3 {
		t.Fatalf("status %q after %d requests", response.Data.Status, requests.Load())
	}
	if fmt.Sprint(clock.waits) != fmt.Sprint([]time.Duration{130 * time.Second, 130 * time.Second}) {
		t.Fatalf("waits %v", clock.waits)
	}
}

func TestKeyStaysTheSameAcrossAllRetriesAndIsExposedOnConnectionErrors(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		keys = append(keys, req.Header.Get("Idempotency-Key"))
		mu.Unlock()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()
	c := hermeticClient(t, server.URL)
	c.maxRetries = 2
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.install(c)
	_, err := c.Folders.CreateFolder(context.Background(), apiFolderRequest())
	var connection *ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("got %T %v", err, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 || keys[0] == "" || keys[0] != keys[1] || keys[1] != keys[2] || connection.IdempotencyKey != keys[0] {
		t.Fatalf("keys %v, exposed %q", keys, connection.IdempotencyKey)
	}
}

func TestDefaults(t *testing.T) {
	c := hermeticClient(t, "http://127.0.0.1:1")
	if c.Timeout() != 60*time.Second {
		t.Errorf("timeout %s", c.Timeout())
	}
	if c.Jobs.DefaultWaitTimeout() != 14400*time.Second {
		t.Errorf("wait timeout %s", c.Jobs.DefaultWaitTimeout())
	}
	t.Setenv(envBaseURL, "")
	d, err := New(WithAPIKey("k"))
	if err != nil {
		t.Fatal(err)
	}
	if d.BaseURL() != "https://api.rask.ai/v3" {
		t.Errorf("default base URL %s", d.BaseURL())
	}
}

func TestUnderBaseURL(t *testing.T) {
	c := hermeticClient(t, "https://gw.example.com/rask/v3")
	for raw, want := range map[string]bool{
		"https://gw.example.com/rask/v3/me":      true,
		"https://GW.example.com:443/rask/v3/me":  true,
		"https://gw.example.com/rask/v3":         false,
		"https://gw.example.com/rask/v33/me":     false,
		"https://gw.example.com/other":           false,
		"http://gw.example.com/rask/v3/me":       false,
		"https://gw.example.com:8443/rask/v3/me": false,
		"https://sub.gw.example.com/rask/v3/me":  false,
		"https://example.com/rask/v3/me":         false,
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.underBaseURL(u); got != want {
			t.Errorf("%s: %v, want %v", raw, got, want)
		}
	}
}

func TestRedactURL(t *testing.T) {
	c := hermeticClient(t, "https://api.example.com/v3")
	for raw, want := range map[string]string{
		"https://api.example.com/v3/projects?limit=2&cursor=abc": "https://api.example.com/v3/projects?limit=2&cursor=abc",
		"https://api.example.com/v3/x?X-Amz-Signature=s&a=1":     "https://api.example.com/v3/x?X-Amz-Signature=REDACTED&a=1",
		"https://storage.example.com/o?X-Amz-Signature=s&a=1":    "https://storage.example.com/o?REDACTED",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.redactURL(u); got != want {
			t.Errorf("%s: %s, want %s", raw, got, want)
		}
	}
}

func apiFolderRequest() api.CreateFolderRequest { return api.CreateFolderRequest{Name: "x"} }
