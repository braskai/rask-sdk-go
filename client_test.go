package rask_test

// Hermetic tests of the owner's base URL requirement and of what the acceptance scenarios cannot reach: same host name
// on another port, subdomains, hops back to the API, printing a client. These need no harness.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/braskai/rask-sdk-go/v3"
	"github.com/braskai/rask-sdk-go/v3/api"
)

const testKey = "raskai_test_key_ThisIsNotARealKeyButItMustNeverShowUpAnywhereAtAll"

// recorded is one request a test server saw.
type recorded struct {
	Host, Method, Path, RawQuery string
	Authorization, Client        string
	Key, ContentType, UserAgent  string
	ContentLength                int64
	Body                         []byte
}

// recorder is a test server that records what it gets and answers through handler.
type recorder struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recorded
	handler  http.HandlerFunc
}

func newRecorder(t *testing.T, handler http.HandlerFunc) *recorder {
	t.Helper()
	r := &recorder{handler: handler}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, recorded{
			Host: req.Host, Method: req.Method, Path: req.URL.EscapedPath(), RawQuery: req.URL.RawQuery,
			Authorization: req.Header.Get("Authorization"), Client: req.Header.Get("X-Rask-Client"),
			Key: req.Header.Get("Idempotency-Key"), ContentType: req.Header.Get("Content-Type"),
			UserAgent: req.Header.Get("User-Agent"), ContentLength: req.ContentLength, Body: body,
		})
		r.mu.Unlock()
		req.Body = io.NopCloser(bytes.NewReader(body))
		r.handler(w, req)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) seen() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.requests...)
}

func clearEnv(t *testing.T) {
	t.Helper()
	setEnv(t, nil)
}

func newTestClient(t *testing.T, baseURL string, opts ...rask.Option) *rask.Client {
	t.Helper()
	clearEnv(t)
	all := append([]rask.Option{
		rask.WithAPIKey(testKey),
		rask.WithBaseURL(baseURL),
		rask.WithHTTPClient(safeHTTPClient()),
	}, opts...)
	client, err := rask.New(all...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

const (
	folderJSON = `{"object":"folder","id":"fld-1","name":"x","is_pinned":false,"project_count":0,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`
	meJSON     = `{"object":"me"}`
)

func TestBaseURLResolution(t *testing.T) {
	for _, tc := range []struct {
		name, argument, env, want string
	}{
		{"default", "", "", "https://api.rask.ai/v3"},
		{"argument", "https://gw.example.com/rask/v3", "", "https://gw.example.com/rask/v3"},
		{"environment", "", "https://env.example.com/v3", "https://env.example.com/v3"},
		{"argument beats environment", "https://arg.example.com/v3", "https://env.example.com/v3", "https://arg.example.com/v3"},
		{"trailing slash", "https://gw.example.com/rask/v3/", "", "https://gw.example.com/rask/v3"},
		{"trailing slashes", "https://gw.example.com/rask/v3///", "", "https://gw.example.com/rask/v3"},
		{"environment trailing slash", "", "https://env.example.com/rask/v3/", "https://env.example.com/rask/v3"},
		{"no path", "https://gw.example.com", "", "https://gw.example.com"},
		{"root path", "https://gw.example.com/", "", "https://gw.example.com"},
		{"port", "http://127.0.0.1:8080/v3", "", "http://127.0.0.1:8080/v3"},
		{"surrounding space", " https://gw.example.com/v3 ", "", "https://gw.example.com/v3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			if tc.env != "" {
				t.Setenv("RASK_BASE_URL", tc.env)
			}
			client, err := rask.New(rask.WithAPIKey(testKey), rask.WithBaseURL(tc.argument))
			if err != nil {
				t.Fatal(err)
			}
			if got := client.BaseURL(); got != tc.want {
				t.Fatalf("BaseURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBaseURLRefused(t *testing.T) {
	for _, bad := range []string{
		"gw.example.com/v3",
		"ftp://gw.example.com/v3",
		"https://",
		"https://gw.example.com/v3?x=1",
		"https://gw.example.com/v3#frag",
		"https://user:secret-password@gw.example.com/v3",
		"://nonsense",
	} {
		clearEnv(t)
		_, err := rask.New(rask.WithAPIKey(testKey), rask.WithBaseURL(bad))
		if err == nil {
			t.Errorf("%q: expected an error", bad)
			continue
		}
		if strings.Contains(err.Error(), "secret-password") {
			t.Errorf("%q: the error repeats the password: %v", bad, err)
		}
	}
}

func TestPrefixAndTrailingSlashOnTheWire(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, meJSON) })
	for _, base := range []string{server.URL + "/rask/v3", server.URL + "/rask/v3/", server.URL + "/rask/v3//"} {
		client := newTestClient(t, base)
		if _, err := client.Account.GetMe(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Projects.GetProject(context.Background(), "a/b c"); err != nil {
			var decode *rask.DecodeError
			if !errors.As(err, &decode) {
				t.Fatal(err)
			}
		}
	}
	for _, rec := range server.seen() {
		if rec.Path != "/rask/v3/me" && rec.Path != "/rask/v3/projects/a%2Fb%20c" {
			t.Errorf("path %q left the prefix, or doubled a slash", rec.Path)
		}
	}
	if got := len(server.seen()); got != 6 {
		t.Fatalf("%d requests, want 6", got)
	}
}

func TestEnvironmentBaseURLOnTheWire(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, meJSON) })
	clearEnv(t)
	t.Setenv("RASK_API_KEY", testKey)
	t.Setenv("RASK_BASE_URL", server.URL+"/from/env/v3/")
	client, err := rask.New(rask.WithHTTPClient(safeHTTPClient()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Account.GetMe(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := server.seen()[0]
	if rec.Path != "/from/env/v3/me" || rec.Authorization != "Bearer "+testKey {
		t.Fatalf("got %+v", rec)
	}
}

func TestDotSegmentsAndEmptyIDsNeverLeaveTheSDK(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, meJSON) })
	client := newTestClient(t, server.URL+"/rask/v3")
	for _, id := range []string{"", ".", ".."} {
		if _, err := client.Projects.GetProject(context.Background(), id); err == nil {
			t.Errorf("project id %q: expected an error", id)
		}
		if _, err := client.Segments.UpdateSegment(context.Background(), "p", id, `"e"`, api.UpdateSegmentRequest{}); err == nil {
			t.Errorf("segment id %q: expected an error", id)
		}
	}
	if got := len(server.seen()); got != 0 {
		t.Fatalf("%d requests went out", got)
	}
}

func TestOptionsAnOperationDoesNotTakeNeverLeaveTheSDK(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, meJSON) })
	client := newTestClient(t, server.URL)
	if _, err := client.Account.GetMe(context.Background(), rask.WithIdempotencyKey("k")); err == nil {
		t.Error("a key on getMe: expected an error")
	}
	if _, err := client.Account.GetMe(context.Background(), rask.WithIfMatch(`"e"`)); err == nil {
		t.Error("if_match on getMe: expected an error")
	}
	if _, err := client.Folders.CreateFolder(context.Background(), api.CreateFolderRequest{Name: "x"}, rask.WithIdempotencyKey("")); err == nil {
		t.Error("an empty key: expected an error")
	}
	if got := len(server.seen()); got != 0 {
		t.Fatalf("%d requests went out", got)
	}
}

// TestRedirectsNeverCarryTheKeyAway covers what net/http alone gets wrong: its rule keeps Authorization for another
// port of the same host name and for subdomains.
func TestRedirectsNeverCarryTheKeyAway(t *testing.T) {
	type hop struct {
		name      string
		target    func(api, other *recorder) string
		wantOnAPI int // Requests the API sees with the key, the first one included.
	}
	for _, tc := range []hop{
		{"another host name and port", func(_, other *recorder) string {
			return "http://localhost:" + portOf(other) + "/landing"
		}, 1},
		{"same host name, another port", func(_, other *recorder) string {
			return "http://127.0.0.1:" + portOf(other) + "/landing"
		}, 1},
	} {
		for _, status := range []int{301, 302, 303, 307, 308} {
			t.Run(fmt.Sprintf("%s %d", tc.name, status), func(t *testing.T) {
				var apiServer, other *recorder
				other = newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, folderJSON) })
				apiServer = newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Location", tc.target(apiServer, other))
					w.WriteHeader(status)
				})
				client := newTestClient(t, apiServer.URL+"/rask/v3")
				if _, err := client.Folders.CreateFolder(context.Background(), api.CreateFolderRequest{Name: "x"}); err != nil {
					t.Fatal(err)
				}
				if got := other.seen(); len(got) != 1 || got[0].Authorization != "" || got[0].Client != "" {
					t.Fatalf("other host saw %+v", got)
				}
				if got := apiServer.seen(); len(got) != tc.wantOnAPI || got[0].Authorization != "Bearer "+testKey {
					t.Fatalf("API saw %+v", got)
				}
			})
		}
	}
}

func portOf(r *recorder) string {
	u, _ := url.Parse(r.URL)
	return u.Port()
}

func TestRedirectWithinTheBaseURLKeepsTheKey(t *testing.T) {
	var server *recorder
	server = newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/rask/v3/me" {
			w.Header().Set("Location", server.URL+"/rask/v3/moved")
			w.WriteHeader(307)
			return
		}
		writeJSON(w, 200, meJSON)
	})
	client := newTestClient(t, server.URL+"/rask/v3")
	if _, err := client.Account.GetMe(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := server.seen()
	if len(seen) != 2 || seen[1].Authorization != "Bearer "+testKey {
		t.Fatalf("got %+v", seen)
	}
}

func TestRedirectOutOfThePrefixDropsTheKey(t *testing.T) {
	var server *recorder
	server = newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/rask/v3/me" {
			w.Header().Set("Location", server.URL+"/someone-else/me")
			w.WriteHeader(307)
			return
		}
		writeJSON(w, 200, meJSON)
	})
	client := newTestClient(t, server.URL+"/rask/v3")
	if _, err := client.Account.GetMe(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := server.seen()
	if len(seen) != 2 || seen[1].Authorization != "" {
		t.Fatalf("got %+v", seen)
	}
}

// A host outside the API that redirects back must not get the key sent on its say-so.
func TestRedirectBackToTheAPIStaysWithoutTheKey(t *testing.T) {
	var apiServer, other *recorder
	other = newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", apiServer.URL+"/rask/v3/back")
		w.WriteHeader(307)
	})
	apiServer = newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/rask/v3/me" {
			w.Header().Set("Location", "http://localhost:"+portOf(other)+"/away")
			w.WriteHeader(307)
			return
		}
		writeJSON(w, 200, meJSON)
	})
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	if _, err := client.Account.GetMe(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := apiServer.seen()
	if len(seen) != 2 || seen[0].Authorization == "" || seen[1].Authorization != "" {
		t.Fatalf("API saw %+v", seen)
	}
	if got := other.seen(); len(got) != 1 || got[0].Authorization != "" {
		t.Fatalf("other host saw %+v", got)
	}
}

// net/http would put the key back here by itself: it treats another port of the same host name as the same host.
func TestRedirectBackFromAnotherPortStaysWithoutTheKey(t *testing.T) {
	var apiServer, other *recorder
	other = newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", apiServer.URL+"/rask/v3/back")
		w.WriteHeader(307)
	})
	apiServer = newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/rask/v3/me" {
			w.Header().Set("Location", "http://127.0.0.1:"+portOf(other)+"/away")
			w.WriteHeader(307)
			return
		}
		writeJSON(w, 200, meJSON)
	})
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	if _, err := client.Account.GetMe(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := apiServer.seen()
	if len(seen) != 2 || seen[0].Authorization == "" || seen[1].Authorization != "" {
		t.Fatalf("API saw %+v", seen)
	}
	if got := other.seen(); len(got) != 1 || got[0].Authorization != "" {
		t.Fatalf("other host saw %+v", got)
	}
}

// dialOnly sends every host name to one listener, so that tests can use names without leaving the machine.
func dialOnly(addr string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}}
}

func TestRedirectToASubdomainOrDowngradeDropsTheKey(t *testing.T) {
	for _, target := range []string{"http://sub.api.example.test/rask/v3/x", "http://api.example.test.evil.test/rask/v3/x"} {
		server := newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/rask/v3/me" {
				w.Header().Set("Location", target)
				w.WriteHeader(307)
				return
			}
			writeJSON(w, 200, meJSON)
		})
		clearEnv(t)
		client, err := rask.New(rask.WithAPIKey(testKey), rask.WithBaseURL("http://api.example.test/rask/v3"),
			rask.WithHTTPClient(dialOnly(server.Listener.Addr().String())))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Account.GetMe(context.Background()); err != nil {
			t.Fatal(err)
		}
		seen := server.seen()
		if len(seen) != 2 || seen[0].Authorization == "" || seen[1].Authorization != "" {
			t.Fatalf("%s: got %+v", target, seen)
		}
	}
}

func TestRedirectLoopEndsWithoutRetrying(t *testing.T) {
	var server *recorder
	server = newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", server.URL+"/rask/v3/me")
		w.WriteHeader(307)
	})
	client := newTestClient(t, server.URL+"/rask/v3")
	_, err := client.Account.GetMe(context.Background())
	var connection *rask.ConnectionError
	if !errors.As(err, &connection) || connection.Attempts != 1 {
		t.Fatalf("got %T %v", err, err)
	}
}

// TestPresignedPutCarriesNoKey is the upload's own check, on top of BU6: storage sees the signed headers and no key, and
// the API calls around it stay under the prefix.
func TestPresignedPutCarriesNoKey(t *testing.T) {
	storage := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	apiServer := newRecorder(t, uploadAPI(t, func() string { return storage.URL + "/bucket/obj?X-Amz-Signature=abc123secret" }))
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	file := writeTemp(t, "clip.mp4", 2048)
	if _, err := client.Uploads.UploadFile(context.Background(), rask.UploadFileParams{Path: file}); err != nil {
		t.Fatal(err)
	}
	puts := storage.seen()
	if len(puts) != 1 {
		t.Fatalf("%d PUTs", len(puts))
	}
	put := puts[0]
	if put.Authorization != "" || put.Client != "" {
		t.Fatalf("storage saw the API's headers: %+v", put)
	}
	if put.ContentType != "video/mp4" || put.ContentLength != 2048 || len(put.Body) != 2048 {
		t.Fatalf("the PUT is not what was signed: type %q length %d body %d", put.ContentType, put.ContentLength, len(put.Body))
	}
	for _, rec := range apiServer.seen() {
		if !strings.HasPrefix(rec.Path, "/rask/v3/uploads") || rec.Authorization != "Bearer "+testKey {
			t.Errorf("API request %+v", rec)
		}
	}
}

// uploadAPI answers createUpload, getUpload and completeUpload.
func uploadAPI(t *testing.T, putURL func() string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/uploads"):
			var body struct {
				ContentType string `json:"content_type"`
				SizeBytes   int64  `json:"size_bytes"`
			}
			raw, _ := io.ReadAll(req.Body)
			_ = json.Unmarshal(raw, &body)
			writeJSON(w, 201, uploadJSON(putURL(), body.ContentType, body.SizeBytes))
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/complete"):
			writeJSON(w, 202, `{"object":"job","id":"job-1","status":"queued"}`)
		default:
			writeJSON(w, 404, `{"code":"upload_not_found","title":"x"}`)
		}
	}
}

func uploadJSON(putURL, contentType string, size int64) string {
	return fmt.Sprintf(`{"object":"upload","id":"upl-1","status":"pending","transfer":{"method":"single","url":%q,"headers":{"Content-Type":%q,"Content-Length":"%d"},"expires_at":"2999-01-01T00:00:00Z"}}`, putURL, contentType, size)
}

func writeTemp(t *testing.T, name string, size int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, bytes.Repeat([]byte{7}, size), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The presigned URL is a credential too: it stays out of errors and logs.
func TestUploadFailureHidesThePresignedURL(t *testing.T) {
	const secret = "abc123secret"
	storage := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		hijacker, _ := w.(http.Hijacker)
		conn, _, err := hijacker.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	apiServer := newRecorder(t, uploadAPI(t, func() string { return storage.URL + "/bucket/obj?X-Amz-Signature=" + secret }))
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := newTestClient(t, apiServer.URL+"/rask/v3", rask.WithLogger(logger), rask.WithDebug(true))
	_, err := client.Uploads.UploadFile(context.Background(), rask.UploadFileParams{Path: writeTemp(t, "clip.mp4", 64)})
	var uploadError *rask.UploadError
	if !errors.As(err, &uploadError) || uploadError.Attempts != 4 {
		t.Fatalf("got %T %v", err, err)
	}
	if got := len(storage.seen()); got != 4 {
		t.Fatalf("%d PUTs, want 4", got)
	}
	for name, text := range map[string]string{"error": err.Error(), "logs": logs.String()} {
		if strings.Contains(text, secret) {
			t.Errorf("%s contain the signature: %s", name, text)
		}
		if strings.Contains(text, testKey) {
			t.Errorf("%s contain the key: %s", name, text)
		}
	}
	if !strings.Contains(logs.String(), "/bucket/obj") {
		t.Errorf("the logs do not say where the PUT went: %s", logs.String())
	}
}

func TestUploadNonRetryableRefusalIsNotRepeated(t *testing.T) {
	storage := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(403)
		_, _ = io.WriteString(w, "<Error><Code>SignatureDoesNotMatch</Code></Error>")
	})
	apiServer := newRecorder(t, uploadAPI(t, func() string { return storage.URL + "/o?X-Amz-Signature=s" }))
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	_, err := client.Uploads.UploadFile(context.Background(), rask.UploadFileParams{Path: writeTemp(t, "clip.mp4", 64)})
	var uploadError *rask.UploadError
	if !errors.As(err, &uploadError) || uploadError.Status != 403 || !strings.Contains(uploadError.Detail, "SignatureDoesNotMatch") {
		t.Fatalf("got %T %v", err, err)
	}
	if got := len(storage.seen()); got != 1 {
		t.Fatalf("%d PUTs, want 1", got)
	}
}

func TestUploadSeekableStreamIsReadFromItsOffsetOnEveryAttempt(t *testing.T) {
	const prefix = "skipped-"
	content := strings.Repeat("x", 100)
	var attempts atomic.Int64
	storage := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	apiServer := newRecorder(t, uploadAPI(t, func() string { return storage.URL + "/o?X-Amz-Signature=s" }))
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	reader := strings.NewReader(prefix + content)
	if _, err := reader.Seek(int64(len(prefix)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Uploads.UploadFile(context.Background(), rask.UploadFileParams{Reader: reader, Filename: "a.mp3"}); err != nil {
		t.Fatal(err)
	}
	puts := storage.seen()
	if len(puts) != 2 {
		t.Fatalf("%d PUTs", len(puts))
	}
	for _, put := range puts {
		if string(put.Body) != content || put.ContentType != "audio/mpeg" {
			t.Errorf("PUT body %q type %q", put.Body, put.ContentType)
		}
	}
}

// A pipe is an *os.File, so it has a Seek method, and the method fails: os.Stdin fed by another program is one.
func TestUploadStreamWhoseSeekFailsIsSentWholeOnEveryAttempt(t *testing.T) {
	content := strings.Repeat("x", 100)
	var attempts atomic.Int64
	storage := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	apiServer := newRecorder(t, uploadAPI(t, func() string { return storage.URL + "/o?X-Amz-Signature=s" }))
	client := newTestClient(t, apiServer.URL+"/rask/v3")
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := io.WriteString(writer, content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Uploads.UploadFile(context.Background(), rask.UploadFileParams{Reader: reader, Filename: "a.mp3"}); err != nil {
		t.Fatal(err)
	}
	puts := storage.seen()
	if len(puts) != 2 {
		t.Fatalf("%d PUTs", len(puts))
	}
	for _, put := range puts {
		if string(put.Body) != content {
			t.Errorf("PUT body %q", put.Body)
		}
	}
}

func TestUploadArgumentsAreCheckedBeforeAnyRequest(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, meJSON) })
	client := newTestClient(t, server.URL)
	for name, params := range map[string]rask.UploadFileParams{
		"nothing":           {},
		"both":              {Path: "a.mp4", Reader: strings.NewReader("x")},
		"no filename":       {Reader: strings.NewReader("x")},
		"unknown extension": {Reader: strings.NewReader("x"), Filename: "a.bin"},
		"empty":             {Reader: strings.NewReader(""), Filename: "a.mp4"},
		"wrong size":        {Reader: strings.NewReader("xx"), Filename: "a.mp4", Size: 3},
		"wrong size stream": {Reader: io.MultiReader(strings.NewReader("xx")), Filename: "a.mp4", Size: 3},
		"missing file":      {Path: filepath.Join(t.TempDir(), "gone.mp4")},
	} {
		if _, err := client.Uploads.UploadFile(context.Background(), params); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if got := len(server.seen()); got != 0 {
		t.Fatalf("%d requests went out", got)
	}
}

func TestKeyIsRedacted(t *testing.T) {
	var logs bytes.Buffer
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 401, `{"code":"invalid_token","title":"Invalid","detail":"The key is invalid.","status":401}`)
	})
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := newTestClient(t, server.URL, rask.WithLogger(logger), rask.WithDebug(true))
	_, err := client.Account.GetMe(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	for name, text := range map[string]string{
		"dump":     clientDump(client),
		"error":    err.Error(),
		"error %+": fmt.Sprintf("%+v %#v", err, err),
		"logs":     logs.String(),
	} {
		if strings.Contains(text, testKey) {
			t.Errorf("%s shows the key", name)
		}
	}
	if !strings.Contains(logs.String(), "[REDACTED]") {
		t.Errorf("the debug log says nothing about Authorization: %s", logs.String())
	}
}

func TestMissingKeyNamesTheVariable(t *testing.T) {
	clearEnv(t)
	_, err := rask.New()
	if err == nil || !strings.Contains(err.Error(), "RASK_API_KEY") {
		t.Fatalf("got %v", err)
	}
}

func TestMalformedKeyIsNotRepeated(t *testing.T) {
	clearEnv(t)
	_, err := rask.New(rask.WithAPIKey("raskai_abc\ndef"))
	if err == nil || strings.Contains(err.Error(), "raskai_abc") {
		t.Fatalf("got %v", err)
	}
}

// TestUnknownValuesReadThroughTheTypes shows what a caller gets: an unknown enum value in the typed field, an unknown
// union variant as raw JSON, and an unknown field through Lookup.
func TestUnknownValuesReadThroughTheTypes(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"object":"job","id":"j","type":"brand_new_type","status":"brand_new_status",
			"params":{"type":"brand_new_type","some_param":1},"future_field":{"nested":[1,2]}}`)
	})
	client := newTestClient(t, server.URL)
	response, err := client.Jobs.GetJob(context.Background(), "j", nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Data.Status != "brand_new_status" || response.Data.Type != "brand_new_type" {
		t.Errorf("enums: %q %q", response.Data.Status, response.Data.Type)
	}
	params, err := response.Data.Params.MarshalJSON()
	if err != nil || !strings.Contains(string(params), `"some_param":1`) {
		t.Errorf("union: %s %v", params, err)
	}
	if discriminator, err := response.Data.Params.Discriminator(); err != nil || discriminator != "brand_new_type" {
		t.Errorf("discriminator: %s %v", discriminator, err)
	}
	if got, ok := response.Lookup("/future_field/nested/1"); !ok || got != json.Number("2") {
		t.Errorf("unknown field: %v %v", got, ok)
	}
	if _, ok := response.Lookup("/nope"); ok {
		t.Error("a missing pointer was found")
	}
}

// A body that breaks a type (a contract change, not an unknown value) must not lose an operation that took effect.
func TestABodyThatBreaksATypeKeepsTheResponse(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 201, `{"object":"folder","id":"fld-1","created_at":"not a time"}`)
	})
	client := newTestClient(t, server.URL)
	response, err := client.Folders.CreateFolder(context.Background(), api.CreateFolderRequest{Name: "x"})
	var decode *rask.DecodeError
	if !errors.As(err, &decode) {
		t.Fatalf("got %T %v", err, err)
	}
	if response == nil || response.StatusCode != 201 || response.IdempotencyKey == "" {
		t.Fatalf("got %+v", response)
	}
	if got, _ := response.Lookup("/id"); got != "fld-1" {
		t.Fatalf("id is %v", got)
	}
	if got := len(server.seen()); got != 1 {
		t.Fatalf("%d requests: a decode failure was retried", got)
	}
}

func TestErrorTypesByStatus(t *testing.T) {
	var current atomic.Int64
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		status := int(current.Load())
		writeJSON(w, status, fmt.Sprintf(`{"code":"some_code","title":"T","status":%d,"detail":"D","retryable":false,"retry":"none","errors":[]}`, status))
	})
	client := newTestClient(t, server.URL, rask.WithMaxRetries(0))
	for code, as := range map[int]any{
		400: new(*rask.BadRequestError),
		401: new(*rask.AuthenticationError),
		402: new(*rask.InsufficientBalanceError),
		403: new(*rask.PermissionDeniedError),
		404: new(*rask.NotFoundError),
		409: new(*rask.ConflictError),
		410: new(*rask.GoneError),
		412: new(*rask.PreconditionFailedError),
		413: new(*rask.RequestTooLargeError),
		415: new(*rask.UnsupportedMediaTypeError),
		422: new(*rask.ValidationError),
		428: new(*rask.PreconditionRequiredError),
		429: new(*rask.RateLimitError),
		500: new(*rask.ServerError),
		503: new(*rask.ServerError),
	} {
		current.Store(int64(code))
		_, err := client.Account.GetMe(context.Background())
		if !errors.As(err, as) {
			t.Errorf("%d: got %T", code, err)
		}
		var base *rask.RaskError
		if !errors.As(err, &base) || base.Status != code || base.Code != "some_code" {
			t.Errorf("%d: base %+v", code, base)
		}
	}
}

func TestJobErrorsCarryTheJob(t *testing.T) {
	body := `{"object":"job","id":"j","status":"failed","error":{"code":"dub_failed","title":"T","status":500,"detail":"D","retryable":true,"retry":"new_key","errors":[]}}`
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "10")
		writeJSON(w, 200, body)
	})
	client := newTestClient(t, server.URL)
	_, err := client.Jobs.Wait(context.Background(), "j")
	var failed *rask.JobFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("got %T %v", err, err)
	}
	if failed.Code != "dub_failed" || failed.Status != 500 || failed.Retry != "new_key" || failed.RetryAfter != 0 {
		t.Fatalf("got %+v", failed.RaskError)
	}
	var base *rask.RaskError
	if !errors.As(err, &base) || failed.Job.Id != "j" {
		t.Fatalf("base %v job %v", base, failed.Job.Id)
	}
	if !strings.Contains(err.Error(), "dub_failed") {
		t.Fatalf("message: %s", err)
	}
}

func TestWaitStopsOnANonTransientError(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 404, `{"code":"job_not_found","title":"T","status":404}`)
	})
	client := newTestClient(t, server.URL)
	_, err := client.Jobs.Wait(context.Background(), "j", rask.WithWaitTimeout(time.Minute))
	var notFound *rask.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("got %T %v", err, err)
	}
}

func TestWaitHonoursTheCallersContext(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		writeJSON(w, 200, `{"object":"job","id":"j","status":"running"}`)
	})
	client := newTestClient(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.Jobs.Wait(ctx, "j")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %T %v", err, err)
	}
	var timedOut *rask.WaitTimeoutError
	if errors.As(err, &timedOut) {
		t.Fatal("the caller's own context is not the wait's timeout")
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("the wait outlived the context")
	}
}

func TestPagersStopWhenTheLoopStops(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"object":"list","data":[{"id":"a"},{"id":"b"}],"next_cursor":"more","has_more":true}`)
	})
	client := newTestClient(t, server.URL)
	for item, err := range client.Folders.ListFoldersAll(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		if item.Id != "a" {
			t.Fatalf("item %q", item.Id)
		}
		break
	}
	if got := len(server.seen()); got != 1 {
		t.Fatalf("%d requests after breaking on the first item", got)
	}
}

func TestPagerRefusesARepeatedCursor(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, `{"object":"list","data":[],"next_cursor":"same","has_more":true}`)
	})
	client := newTestClient(t, server.URL)
	var last error
	for _, err := range client.Folders.ListFoldersAll(context.Background(), nil) {
		last = err
	}
	if last == nil || !strings.Contains(last.Error(), "repeated cursor") {
		t.Fatalf("got %v", last)
	}
}

// A cycle repeats a cursor too, though never twice in a row: A, B, then A again.
func TestPagerRefusesACursorCycle(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		next := "A"
		if req.URL.Query().Get("cursor") == "A" {
			next = "B"
		}
		writeJSON(w, 200, fmt.Sprintf(`{"object":"list","data":[{"id":"a"}],"next_cursor":%q,"has_more":true}`, next))
	})
	client := newTestClient(t, server.URL)
	var last error
	items := 0
	for _, err := range client.Folders.ListFoldersAll(context.Background(), nil) {
		last = err
		if items++; items > 10 {
			t.Fatal("the pager follows the cycle")
		}
	}
	if last == nil || !strings.Contains(last.Error(), `repeated cursor "A"`) {
		t.Fatalf("got %v", last)
	}
	if got := len(server.seen()); got != 3 {
		t.Fatalf("%d requests, want 3", got)
	}
}

// TestSmokeProgram runs examples/smoke against the harness, as the lead will run it on the test stand.
func TestSmokeProgram(t *testing.T) {
	h := useHarness(t)
	if err := h.request(http.MethodPost, "/reset", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.request(http.MethodPost, "/pages", map[string]any{"operationId": "listProjects", "pages": 3, "page_size": 2}, nil); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "run", "./examples/smoke")
	cmd.Env = append(os.Environ(), "RASK_API_KEY="+h.apiKey, "RASK_BASE_URL="+h.baseURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	if strings.Contains(string(output), h.apiKey) {
		t.Fatalf("the output shows the key:\n%s", output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	want := []string{"getMe", "listLanguages", "listVoices", "listProjects page=1", "listProjects page=2", "getBalance", "getPlan", "createFolder", "getFolder", "deleteFolder"}
	if len(lines) != len(want) {
		t.Fatalf("%d lines, want %d:\n%s", len(lines), len(want), output)
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, want[i]) || !strings.Contains(line, "status=2") {
			t.Errorf("line %d: %q, want %s with a 2xx status", i, line, want[i])
		}
	}
	for _, rec := range h.requests(t) {
		if len(rec.Violations) != 0 {
			t.Errorf("%s broke the contract: %v", rec.OperationID, rec.Violations)
		}
	}
}

func TestSmokeRefusesToDefaultToProduction(t *testing.T) {
	cmd := exec.Command("go", "run", "./examples/smoke")
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "RASK_") {
			env = append(env, entry)
		}
	}
	cmd.Env = append(env, "RASK_API_KEY="+testKey)
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "RASK_BASE_URL") {
		t.Fatalf("err %v, output:\n%s", err, output)
	}
}

// TestReadmeShowsTheCompiledExample keeps the README's worked example the same as examples/quickstart, which CI builds.
func TestReadmeShowsTheCompiledExample(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile("examples/quickstart/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), string(example)) {
		t.Fatal("README.md does not contain examples/quickstart/main.go verbatim")
	}
}

func TestWaitSendsIfNoneMatchAndKeepsWaitingOn304(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	server := newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		seen = append(seen, req.Header.Get("If-None-Match"))
		calls := len(seen)
		mu.Unlock()
		w.Header().Set("Retry-After", "0")
		switch calls {
		case 1:
			w.Header().Set("ETag", `"j-1"`)
			writeJSON(w, 200, `{"object":"job","id":"j","status":"running"}`)
		case 2:
			w.WriteHeader(http.StatusNotModified)
		default:
			writeJSON(w, 200, `{"object":"job","id":"j","status":"succeeded"}`)
		}
	})
	client := newTestClient(t, server.URL)
	response, err := client.Jobs.Wait(context.Background(), "j")
	if err != nil {
		t.Fatal(err)
	}
	if response.Data.Status != api.JobStatusSucceeded {
		t.Fatalf("status %q", response.Data.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"", `"j-1"`, `"j-1"`}; fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("If-None-Match %q, want %q", seen, want)
	}
}

func TestGetJobReturnsNotModifiedAsAResult(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) })
	client := newTestClient(t, server.URL)
	response, err := client.Jobs.GetJob(context.Background(), "j", &api.GetJobParams{IfNoneMatch: rask.Ptr(`"j-1"`)})
	if err != nil {
		t.Fatal(err)
	}
	if !response.NotModified() || response.Data.Id != "" {
		t.Fatalf("got %+v", response)
	}
}

func TestMetaReadsTheHeadersWithoutASecondCall(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"e"`)
		w.Header().Set("Location", "/v3/folders/fld-1")
		w.Header().Set("X-Request-Id", "req-1")
		w.Header().Set("Idempotent-Replayed", "true")
		w.Header().Set("Retry-After", "3")
		writeJSON(w, 201, folderJSON)
	})
	client := newTestClient(t, server.URL)
	response, err := client.Folders.CreateFolder(context.Background(), api.CreateFolderRequest{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	wait, ok := response.RetryAfter()
	if response.ETag() != `"e"` || response.Location() != "/v3/folders/fld-1" || response.RequestID() != "req-1" ||
		!response.IdempotentReplayed() || !ok || wait != 3*time.Second || response.StatusCode != 201 {
		t.Fatalf("got %+v", response.Meta)
	}
	if response.Data.Id != "fld-1" {
		t.Fatalf("typed data: %+v", response.Data)
	}
	if got := len(server.seen()); got != 1 {
		t.Fatalf("%d requests", got)
	}
}

// An optional body left out goes out as no body at all, without the Content-Type the generated client would set.
func TestOptionalBodyLeftOutSendsNoContentType(t *testing.T) {
	var mu sync.Mutex
	var header http.Header
	lastHeader := func() http.Header {
		mu.Lock()
		defer mu.Unlock()
		return header
	}
	server := newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		header = req.Header.Clone()
		mu.Unlock()
		writeJSON(w, 202, `{"object":"job","id":"j","status":"queued"}`)
	})
	client := newTestClient(t, server.URL)
	if _, err := client.Uploads.CompleteUpload(context.Background(), "upl-1", nil); err != nil {
		t.Fatal(err)
	}
	if value, present := lastHeader()["Content-Type"]; present {
		t.Errorf("Content-Type %q on a request without a body", value)
	}
	if rec := server.seen()[0]; len(rec.Body) != 0 || rec.Key == "" {
		t.Errorf("got %+v", rec)
	}
	if _, err := client.Uploads.CompleteUpload(context.Background(), "upl-1", &api.CompleteUploadRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := lastHeader().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type %q with a body", got)
	}
	if rec := server.seen()[1]; string(rec.Body) != "{}" {
		t.Errorf("body %q", rec.Body)
	}
}
