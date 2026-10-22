package rask_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rask "github.com/braskai/rask-sdk-go"
	"github.com/google/uuid"
)

// fakeRask serves the Cognito token endpoint and the API on one test server.
type fakeRask struct {
	t          *testing.T
	server     *httptest.Server
	tokenCalls atomic.Int32
	// api handles API requests that carried a valid bearer token.
	api func(w http.ResponseWriter, r *http.Request)
	// rejectFirst makes the first API request answer 401, as if its token was revoked.
	rejectFirst atomic.Bool
}

func newFake(t *testing.T) *fakeRask {
	f := &fakeRask{t: t}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeRask) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/oauth2/token" {
		n := f.tokenCalls.Add(1)
		if err := r.ParseForm(); err != nil {
			f.t.Errorf("parse token form: %v", err)
		}
		want := map[string]string{
			"grant_type":    "client_credentials",
			"client_id":     "id",
			"client_secret": "secret",
			"scope":         "api/source api/input api/output api/limit",
		}
		for k, v := range want {
			if got := r.PostForm.Get(k); got != v {
				f.t.Errorf("token form %s = %q, want %q", k, got, v)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token-" + string(rune('0'+n)),
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
		return
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer token-") {
		f.t.Errorf("%s %s: Authorization = %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
	}
	if !strings.HasPrefix(r.Header.Get("User-Agent"), "rask-sdk-go/") {
		f.t.Errorf("User-Agent = %q", r.Header.Get("User-Agent"))
	}
	if f.rejectFirst.CompareAndSwap(true, false) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.api(w, r)
}

func (f *fakeRask) client(t *testing.T, opts ...rask.Option) *rask.Client {
	opts = append([]rask.Option{
		rask.WithBaseURL(f.server.URL),
		rask.WithTokenURL(f.server.URL + "/oauth2/token"),
	}, opts...)
	c, err := rask.NewClient("id", "secret", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var projectID = uuid.MustParse("6f1c2b8e-4d4b-4a0e-9a59-0d6a3f6c1a01")

func projectJSON() map[string]any {
	return map[string]any{
		"id":          projectID.String(),
		"name":        "demo",
		"source_type": "local",
		"status":      "merging_done",
		"created_at":  "2026-10-23T10:00:00Z",
		"dst_lang":    "es",
	}
}

func TestTokenIsFetchedOnceAndCached(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"minutes": map[string]any{"total": 10}})
	}
	c := f.client(t)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.GetCredits(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if _, err := c.GetCredits(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := f.tokenCalls.Load(); n != 1 {
		t.Fatalf("token requests = %d, want 1", n)
	}
}

func TestUnauthorizedRefreshesTokenAndRetries(t *testing.T) {
	f := newFake(t)
	var bodies []string
	f.api = func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if got := r.Header.Get("Authorization"); got != "Bearer token-2" {
			t.Errorf("retry used %q, want refreshed token-2", got)
		}
		writeJSON(w, 200, projectJSON())
	}
	f.rejectFirst.Store(true)
	c := f.client(t)

	p, err := c.CreateProject(context.Background(), rask.ProjectCreate{VideoId: projectID, DstLang: "es"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Id != projectID {
		t.Fatalf("project id = %s", p.Id)
	}
	if n := f.tokenCalls.Load(); n != 2 {
		t.Fatalf("token requests = %d, want 2", n)
	}
	// The JSON body is replayed on retry; unset optional fields are omitted.
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"dst_lang":"es"`) || strings.Contains(bodies[0], "glossary_id") {
		t.Fatalf("retried body = %q", bodies)
	}
}

func TestStaticAccessToken(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-static" {
			t.Errorf("Authorization = %q", got)
		}
		writeJSON(w, 200, projectJSON())
	}
	c, err := rask.NewClient("", "", rask.WithBaseURL(f.server.URL), rask.WithAccessToken("token-static"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetProject(context.Background(), projectID); err != nil {
		t.Fatal(err)
	}
	if f.tokenCalls.Load() != 0 {
		t.Fatal("static token must not call the token endpoint")
	}
	if _, err := rask.NewClient("", ""); err == nil {
		t.Fatal("expected an error without credentials")
	}
}

func TestQueryParameters(t *testing.T) {
	f := newFake(t)
	seg1, seg2 := uuid.New(), uuid.New()
	f.api = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/projects":
			q := r.URL.Query()
			if q.Get("offset") != "20" || q.Get("limit") != "10" || q.Get("name") != "demo" {
				t.Errorf("query = %s", r.URL.RawQuery)
			}
			writeJSON(w, 200, map[string]any{"total": 1, "offset": 20, "projects": []any{projectJSON()}})
		case "/v2/projects/" + projectID.String() + "/transcription":
			// FastAPI expects list query parameters repeated.
			if got := r.URL.Query()["segment_ids"]; len(got) != 2 || got[0] != seg1.String() || got[1] != seg2.String() {
				t.Errorf("segment_ids = %v (raw %s)", got, r.URL.RawQuery)
			}
			writeJSON(w, 200, map[string]any{"segments": []any{map[string]any{
				"id": "s1", "start": "00:00:01,000", "end": "00:00:02,000", "status": "done",
				"src": map[string]any{"text": "Hello", "lang": "en"},
			}}})
		default:
			t.Errorf("unexpected %s", r.URL)
		}
	}
	c := f.client(t)
	ctx := context.Background()

	name := "demo"
	list, err := c.GetProjects(ctx, &rask.GetProjectsParams{Offset: 20, Limit: 10, Name: &name})
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Projects[0].Name != "demo" || *list.Projects[0].Status != rask.ProjectStatusMergingDone {
		t.Fatalf("list = %+v", list)
	}

	ids := []rask.UUID{seg1, seg2}
	tr, err := c.GetProjectTranscription(ctx, projectID, &rask.GetProjectTranscriptionParams{SegmentIds: &ids})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Segments[0].Src.Text != "Hello" || tr.Segments[0].Status != rask.SegmentStatusDone {
		t.Fatalf("transcription = %+v", tr)
	}
}

func TestSliceResult(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, []any{map[string]any{"id": "voice-1", "name": "Anna"}})
	}
	voices, err := f.client(t).GetProjectVoices(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(voices) != 1 {
		t.Fatalf("voices = %+v", voices)
	}
}

func readMultipart(t *testing.T, r *http.Request) map[string]*multipartFile {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		t.Fatalf("Content-Type = %q", r.Header.Get("Content-Type"))
	}
	files := map[string]*multipartFile{}
	mr := multipart.NewReader(r.Body, params["boundary"])
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(p)
		files[p.FormName()] = &multipartFile{name: p.FileName(), contentType: p.Header.Get("Content-Type"), data: string(b)}
	}
}

type multipartFile struct{ name, contentType, data string }

func TestCreateMediaFileStreamsMultipart(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/library/v1/media" || r.URL.Query().Get("kind") != "video" {
			t.Errorf("request = %s %s", r.Method, r.URL)
		}
		files := readMultipart(t, r)
		data := files["data"]
		if data == nil || data.name != "clip.mp4" || data.contentType != "video/mp4" || data.data != "fake video" {
			t.Errorf("data part = %+v", data)
		}
		writeJSON(w, 200, map[string]any{
			"id": uuid.NewString(), "kind": "video", "status": "processing", "mime_type": "video/mp4",
			"user_id": uuid.NewString(), "created_at": "2026-10-23T10:00:00Z", "updated_at": "2026-10-23T10:00:00Z",
			"path": "media/clip.mp4", "meta": map[string]any{"source": "upload"},
		})
	}
	kind := rask.MediaKindVideo
	media, err := f.client(t).CreateMediaFile(context.Background(), "/tmp/clip.mp4", strings.NewReader("fake video"),
		&rask.CreateMediaFileParams{Kind: &kind})
	if err != nil {
		t.Fatal(err)
	}
	if media.Status != rask.MediaStatusProcessing {
		t.Fatalf("media = %+v", media)
	}
	// While processing, meta is an arbitrary object.
	raw, err := media.Meta.AsMediaGetMeta3()
	if err != nil || raw["source"] != "upload" {
		t.Fatalf("meta = %v, %v", raw, err)
	}
}

func TestCreateTranscriptionSrt(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("src_lang") != "en" || r.URL.Query().Get("dst_lang") != "" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		files := readMultipart(t, r)
		if files["src"] == nil || files["src"].data != "1\n00:00:01,000 --> 00:00:02,000\nHello\n" || files["dst"] != nil {
			t.Errorf("parts = %+v", files)
		}
		writeJSON(w, 200, map[string]any{"id": uuid.NewString(), "segments": []any{}})
	}
	c := f.client(t)
	src := "en"
	_, err := c.CreateTranscriptionSrt(context.Background(), &rask.CreateTranscriptionSrtParams{SrcLang: &src},
		strings.NewReader("1\n00:00:01,000 --> 00:00:02,000\nHello\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateTranscriptionSrt(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("expected an error without files")
	}
}

func TestAPIErrors(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, 404, map[string]any{"status": "not_found", "status_code": 404, "detail": "Project not found."})
		default:
			writeJSON(w, 422, map[string]any{"detail": []any{
				map[string]any{"loc": []any{"body", "dst_lang"}, "msg": "Field required", "type": "missing"},
			}})
		}
	}
	c := f.client(t)

	_, err := c.GetProject(context.Background(), projectID)
	var apiErr *rask.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 || apiErr.Status != "not_found" || apiErr.Detail != "Project not found." {
		t.Fatalf("err = %#v", err)
	}
	if err.Error() != "rask: API error 404 not_found: Project not found." {
		t.Fatalf("message = %q", err.Error())
	}

	_, err = c.CreateProject(context.Background(), rask.ProjectCreate{VideoId: projectID})
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 422 || apiErr.Detail != "body.dst_lang: Field required" {
		t.Fatalf("err = %#v", err)
	}
}

func TestAuthError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, map[string]any{"error": "invalid_client"})
	}))
	defer srv.Close()
	c, err := rask.NewClient("id", "bad", rask.WithBaseURL(srv.URL), rask.WithTokenURL(srv.URL+"/oauth2/token"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetCredits(context.Background())
	var authErr *rask.AuthError
	if !errors.As(err, &authErr) || !strings.Contains(err.Error(), "invalid_client") {
		t.Fatalf("err = %v", err)
	}
}

func TestUploadUnauthorizedDropsToken(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-2" {
			t.Errorf("request after a rejected upload used %q, want a new token", got)
		}
		writeJSON(w, 200, map[string]any{"minutes": map[string]any{"total": 1}})
	}
	f.rejectFirst.Store(true)
	c := f.client(t)

	// A streamed upload can't be replayed, so its 401 is returned...
	_, err := c.CreateMediaFile(context.Background(), "clip.mp4", strings.NewReader("video"), nil)
	var apiErr *rask.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err = %v", err)
	}
	// ...but the rejected token is dropped, so the next request gets a new one.
	if _, err := c.GetCredits(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := f.tokenCalls.Load(); n != 2 {
		t.Fatalf("token requests = %d, want 2", n)
	}
}

func TestUploadInputValidation(t *testing.T) {
	f := newFake(t)
	c := f.client(t)
	if _, err := c.CreateMediaFile(context.Background(), "clip.mp4", nil, nil); err == nil {
		t.Fatal("expected an error for a nil file")
	}
}

func TestUploadNotSentDoesNotLeakWriter(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) { t.Error("request must not be sent") }
	c := f.client(t)
	before := runtime.NumGoroutine()

	failEditor := func(ctx context.Context, req *http.Request) error { return errors.New("editor failed") }
	_, err := c.CreateMediaFile(context.Background(), "clip.mp4", strings.NewReader(strings.Repeat("x", 1<<20)), nil, failEditor)
	if err == nil || !strings.Contains(err.Error(), "editor failed") {
		t.Fatalf("err = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines = %d, want <= %d: multipart writer still blocked", n, before)
	}
}

func TestAPIErrorWithoutRaskFields(t *testing.T) {
	f := newFake(t)
	f.api = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"message": "Bad gateway"})
	}
	_, err := f.client(t).GetCredits(context.Background())
	var apiErr *rask.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadGateway || !strings.Contains(apiErr.Detail, "Bad gateway") {
		t.Fatalf("err = %#v", err)
	}
}
