package rask_test

// The acceptance suite: every scenario of the shared harness (DESIGN 11.3, S1-S17, and the custom base URL), over real
// HTTP on a custom base URL with a path prefix, through the typed methods. The harness judges what crossed the wire;
// this file judges what only the SDK's caller can see.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/braskai/rask-sdk-go/v3"
)

const lang = "go"

// loopbackOnly is the HTTP transport of every test: these tests must never leave the machine, even when a bug loses the
// base URL, because the default base URL is production.
type loopbackOnly struct{ next http.RoundTripper }

func (l loopbackOnly) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.URL.Hostname() {
	case "127.0.0.1", "localhost", "::1":
		return l.next.RoundTrip(req)
	}
	return nil, fmt.Errorf("test transport: refusing %s, which is not loopback", req.URL.Host)
}

func safeHTTPClient() *http.Client {
	return &http.Client{Transport: loopbackOnly{http.DefaultTransport}}
}

// harness is a running rask-sdk-acceptance.
type harness struct {
	control      string
	baseURL      string
	otherBaseURL string
	apiKey       string
	cmd          *exec.Cmd
}

var (
	harnessOnce sync.Once
	shared      *harness
	harnessErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if shared != nil && shared.cmd != nil {
		_ = shared.cmd.Process.Kill()
		_, _ = shared.cmd.Process.Wait()
	}
	os.Exit(code)
}

// useHarness returns the shared harness. RASK_SDK_ACCEPTANCE_URL points at a running one; otherwise it is started from
// RASK_SDK_ACCEPTANCE_BIN or from a sibling checkout. Without either the test skips, unless
// RASK_SDK_ACCEPTANCE_REQUIRED=1 says it must not.
func useHarness(t *testing.T) *harness {
	t.Helper()
	harnessOnce.Do(func() { shared, harnessErr = startHarness() })
	if harnessErr != nil {
		if os.Getenv("RASK_SDK_ACCEPTANCE_REQUIRED") == "1" {
			t.Fatalf("acceptance harness: %v", harnessErr)
		}
		t.Skipf("acceptance harness: %v (set RASK_SDK_ACCEPTANCE_BIN)", harnessErr)
	}
	return shared
}

func findHarnessBin() string {
	if bin := os.Getenv("RASK_SDK_ACCEPTANCE_BIN"); bin != "" {
		return bin
	}
	for _, dir := range []string{"../rask-sdk-acceptance", "../../rask-sdk-acceptance"} {
		bin := filepath.Join(dir, "bin", "rask-sdk-acceptance.mjs")
		if _, err := os.Stat(bin); err == nil {
			return bin
		}
	}
	return ""
}

func startHarness() (*harness, error) {
	if control := os.Getenv("RASK_SDK_ACCEPTANCE_URL"); control != "" {
		return describeHarness(strings.TrimRight(control, "/"), nil)
	}
	bin := findHarnessBin()
	if bin == "" {
		return nil, errors.New("not found")
	}
	cmd := exec.Command("node", bin, "--port", "0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(stdout)
	pattern := regexp.MustCompile(`^\s*control\s+(\S+)`)
	for scanner.Scan() {
		if match := pattern.FindStringSubmatch(scanner.Text()); match != nil {
			go func() { _, _ = io.Copy(io.Discard, stdout) }()
			return describeHarness(match[1], cmd)
		}
	}
	_ = cmd.Process.Kill()
	return nil, errors.New("it did not print its control URL")
}

func describeHarness(control string, cmd *exec.Cmd) (*harness, error) {
	h := &harness{control: control, cmd: cmd}
	var health struct {
		BaseURL      string `json:"base_url"`
		OtherBaseURL string `json:"other_base_url"`
		TestAPIKey   string `json:"test_api_key"`
	}
	if err := h.request(http.MethodGet, "/health", nil, &health); err != nil {
		return nil, err
	}
	h.baseURL, h.otherBaseURL, h.apiKey = health.BaseURL, health.OtherBaseURL, health.TestAPIKey
	return h, nil
}

func (h *harness) request(method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, h.control+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := safeHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: %d %s", method, path, response.StatusCode, raw)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// requests returns the harness's recording.
func (h *harness) requests(t *testing.T) []record {
	t.Helper()
	var list struct {
		Requests []record `json:"requests"`
	}
	if err := h.request(http.MethodGet, "/requests", nil, &list); err != nil {
		t.Fatal(err)
	}
	return list.Requests
}

type record struct {
	Listener       string            `json:"listener"`
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	OperationID    string            `json:"operationId"`
	Headers        map[string]string `json:"headers"`
	RawQueryParams map[string]string `json:"raw_query_params"`
	Violations     []any             `json:"violations"`
	Response       *struct {
		Status     int               `json:"status"`
		Headers    map[string]string `json:"headers"`
		Injections []struct {
			Kind    string `json:"kind"`
			Pointer string `json:"pointer"`
			Value   any    `json:"value"`
		} `json:"injections"`
	} `json:"response"`
}

// operationInfo is an entry of GET /__control/operations.
type operationInfo struct {
	OperationID     string `json:"operationId"`
	IfMatch         string `json:"if_match"`
	HasJSONResponse bool   `json:"has_json_response"`
	Sample          struct {
		Path    map[string]any    `json:"path"`
		Query   map[string]any    `json:"query"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"sample"`
}

func (h *harness) operations(t *testing.T) []operationInfo {
	t.Helper()
	var list struct {
		Operations []operationInfo `json:"operations"`
	}
	if err := h.request(http.MethodGet, "/operations", nil, &list); err != nil {
		t.Fatal(err)
	}
	return list.Operations
}

// opCall is the arguments of one operation, as JSON from a scenario; the generated dispatch table binds them to the
// typed method. Strict decoding means the typed request structs can express everything the scenarios send.
type opCall struct {
	path  map[string]any
	query map[string]any
	body  json.RawMessage
	etag  string
}

func (c *opCall) pathString(name string) string { return fmt.Sprint(c.path[name]) }

func (c *opCall) pathInt32(name string) int32 {
	if number, ok := c.path[name].(float64); ok {
		return int32(number)
	}
	return 0
}

func (c *opCall) ifMatch() string { return c.etag }

func (c *opCall) hasBody() bool { return len(c.body) > 0 && string(c.body) != "null" }

func (c *opCall) decodeBody(target any) error {
	if !c.hasBody() {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(c.body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("the typed body cannot express the scenario's: %w", err)
	}
	return nil
}

// decodeQuery fills a generated Params struct. A list parameter may arrive as one string.
func (c *opCall) decodeQuery(target any) error {
	fields := map[string]reflect.Type{}
	structType := reflect.TypeOf(target).Elem()
	for i := range structType.NumField() {
		tag, _, _ := strings.Cut(structType.Field(i).Tag.Get("json"), ",")
		fieldType := structType.Field(i).Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		fields[tag] = fieldType
	}
	values := map[string]any{}
	for name, value := range c.query {
		if text, isString := value.(string); isString && fields[name] != nil && fields[name].Kind() == reflect.Slice {
			values[name] = strings.Split(text, ",")
			continue
		}
		values[name] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func metaOf[T any](r *rask.Response[T]) *rask.Meta {
	if r == nil {
		return nil
	}
	return &r.Meta
}

func collect[T any](source iter.Seq2[T, error]) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for item, err := range source {
		if err != nil {
			return out, err
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			return out, err
		}
		out = append(out, encoded)
	}
	return out, nil
}

// scenario is what POST /scenarios/{id}/start returns.
type scenario struct {
	ID      string `json:"id"`
	Applies bool   `json:"applies"`
	Slow    bool   `json:"slow"`
	Status  string `json:"status"`
	Client  struct {
		Args struct {
			APIKey         string   `json:"api_key"`
			BaseURL        string   `json:"base_url"`
			TimeoutSeconds *float64 `json:"timeout_seconds"`
		} `json:"args"`
		Env map[string]string `json:"env"`
	} `json:"client"`
	Calls     []scenarioCall `json:"calls"`
	ExpectSDK []expectation  `json:"expect_sdk"`
}

type scenarioCall struct {
	Op             string          `json:"op"`
	Helper         string          `json:"helper"`
	Path           map[string]any  `json:"path"`
	Query          map[string]any  `json:"query"`
	Body           json.RawMessage `json:"body"`
	IfMatch        string          `json:"if_match"`
	IdempotencyKey string          `json:"idempotency_key"`
	JobID          string          `json:"job_id"`
	JobIDFromCall  *int            `json:"job_id_from_call"`
	TimeoutSeconds float64         `json:"timeout_seconds"`
	Source         string          `json:"source"`
	Filename       string          `json:"filename"`
	ContentType    string          `json:"content_type"`
	Content        struct {
		SizeBytes int `json:"size_bytes"`
	} `json:"content"`
	AfterCall int `json:"after_call"`
}

type expectation struct {
	Kind                 string         `json:"kind"`
	Call                 *int           `json:"call"`
	ErrorClass           string         `json:"error_class"`
	Status               *int           `json:"status"`
	Code                 *string        `json:"code"`
	Fields               map[string]any `json:"fields"`
	PresentNames         []string       `json:"-"`
	RequestIDMatchesWire bool           `json:"request_id_matches_wire"`
	MessageContains      string         `json:"message_contains"`
	Field                string         `json:"field"`
	Equals               any            `json:"equals"`
	Secret               string         `json:"secret"`
	Path                 string         `json:"path"`
	Present              any            `json:"present"`
	RawStatus            *int           `json:"raw_status"`
	Name                 string         `json:"name"`
	Matches              string         `json:"matches"`
	EqualsField          string         `json:"equals_field"`
	MatchesWire          bool           `json:"matches_wire"`
	Count                *int           `json:"count"`
	IDs                  []string       `json:"ids"`
	ItemCount            *int           `json:"item_count"`
	HasMore              *bool          `json:"has_more"`
	PerOperation         map[string]int `json:"per_operation"`
	FinalBytes           *int64         `json:"final_bytes"`
	MustContain          []string       `json:"must_contain"`
	MustNotContain       []string       `json:"must_not_contain"`
	MustNotContainWire   []struct {
		OperationID string `json:"operationId"`
		Query       string `json:"query"`
	} `json:"must_not_contain_wire"`
	Empty bool `json:"empty"`
}

// callOutcome is what one scenario call left behind.
type callOutcome struct {
	call     scenarioCall
	meta     *rask.Meta
	err      error
	items    []json.RawMessage
	progress int64
	results  map[string]*rask.Meta // sweep_operations
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type scenarioRun struct {
	h            *harness
	client       *rask.Client
	constructErr error
	calls        []*callOutcome
	logs         syncBuffer
	warnMu       sync.Mutex
	warnings     []string
	operations   []operationInfo
}

func (r *scenarioRun) newClient(s *scenario) {
	logger := slog.New(slog.NewTextHandler(&r.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts := []rask.Option{
		rask.WithHTTPClient(safeHTTPClient()),
		rask.WithLogger(logger),
		rask.WithDeprecationHandler(func(w rask.DeprecationWarning) {
			r.warnMu.Lock()
			defer r.warnMu.Unlock()
			r.warnings = append(r.warnings, w.Operation)
		}),
	}
	if s.Client.Args.APIKey != "" {
		opts = append(opts, rask.WithAPIKey(s.Client.Args.APIKey))
	}
	if s.Client.Args.BaseURL != "" {
		opts = append(opts, rask.WithBaseURL(s.Client.Args.BaseURL))
	}
	if s.Client.Args.TimeoutSeconds != nil {
		opts = append(opts, rask.WithTimeout(time.Duration(*s.Client.Args.TimeoutSeconds*float64(time.Second))))
	}
	r.client, r.constructErr = rask.New(opts...)
}

func (r *scenarioRun) optionsFor(call scenarioCall) []rask.RequestOption {
	var opts []rask.RequestOption
	if call.IdempotencyKey != "" {
		opts = append(opts, rask.WithIdempotencyKey(call.IdempotencyKey))
	}
	if call.IfMatch != "" {
		for _, op := range r.operations {
			if op.OperationID == call.Op && op.IfMatch == "optional" {
				opts = append(opts, rask.WithIfMatch(call.IfMatch))
			}
		}
	}
	return opts
}

// callOperation reaches a typed method through the dispatch table. An operation the table does not have (a harness
// newer than this SDK, a renamed operation) fails that call: calling the missing entry would panic.
func callOperation(ctx context.Context, client *rask.Client, id string, in *opCall, opts []rask.RequestOption) (*rask.Meta, error) {
	method, ok := dispatch[id]
	if !ok {
		return nil, fmt.Errorf("operation %s is not in the dispatch table", id)
	}
	return method(ctx, client, in, opts)
}

func (r *scenarioRun) runCall(ctx context.Context, call scenarioCall) *callOutcome {
	outcome := &callOutcome{call: call}
	in := &opCall{path: call.Path, query: call.Query, body: call.Body, etag: call.IfMatch}
	switch call.Helper {
	case "":
		outcome.meta, outcome.err = callOperation(ctx, r.client, call.Op, in, r.optionsFor(call))
	case "first_page":
		outcome.meta, outcome.err = callOperation(ctx, r.client, call.Op, in, nil)
	case "next_page":
		before := r.calls[call.AfterCall]
		cursor, _ := before.meta.Lookup("/next_cursor")
		// The next page is the earlier call again, path and filters included, with the new cursor in place of its own.
		query := map[string]any{}
		for name, value := range before.call.Query {
			query[name] = value
		}
		query["cursor"] = cursor
		// The call is kept as it was sent, for a next_page after this one.
		outcome.call.Op, outcome.call.Path, outcome.call.Query = before.call.Op, before.call.Path, query
		outcome.meta, outcome.err = callOperation(ctx, r.client, before.call.Op, &opCall{path: before.call.Path, query: query}, nil)
	case "iterate":
		iterate, ok := iterators[call.Op]
		if !ok {
			outcome.err = fmt.Errorf("operation %s has no iterator in the dispatch table", call.Op)
			break
		}
		outcome.items, outcome.err = iterate(ctx, r.client, in, nil)
	case "jobs.wait":
		jobID := call.JobID
		if call.JobIDFromCall != nil {
			id, _ := r.calls[*call.JobIDFromCall].meta.Lookup("/id")
			jobID = fmt.Sprint(id)
		}
		var opts []rask.WaitOption
		if call.TimeoutSeconds > 0 {
			opts = append(opts, rask.WithWaitTimeout(time.Duration(call.TimeoutSeconds*float64(time.Second))))
		}
		response, err := r.client.Jobs.Wait(ctx, jobID, opts...)
		outcome.err = err
		outcome.meta = metaOf(response)
	case "uploads.upload_file":
		outcome.meta, outcome.err = r.upload(ctx, call, outcome)
	case "sweep_operations":
		outcome.results = map[string]*rask.Meta{}
		var failures []string
		for _, op := range r.operations {
			if !op.HasJSONResponse {
				continue
			}
			input := &opCall{path: op.Sample.Path, query: op.Sample.Query, body: op.Sample.Body, etag: op.Sample.Headers["If-Match"]}
			meta, err := callOperation(ctx, r.client, op.OperationID, input, nil)
			if err != nil {
				failures = append(failures, op.OperationID+": "+err.Error())
			}
			outcome.results[op.OperationID] = meta
		}
		if failures != nil {
			outcome.err = errors.New(strings.Join(failures, "; "))
		}
	default:
		outcome.err = fmt.Errorf("unknown helper %s", call.Helper)
	}
	return outcome
}

func (r *scenarioRun) upload(ctx context.Context, call scenarioCall, outcome *callOutcome) (*rask.Meta, error) {
	content := make([]byte, call.Content.SizeBytes)
	for i := range content {
		content[i] = byte(i % 256)
	}
	params := rask.UploadFileParams{
		Filename:    call.Filename,
		ContentType: call.ContentType,
		OnProgress:  func(sent, _ int64) { outcome.progress = sent },
	}
	if call.Source == "stream" {
		// A plain reader on purpose: it cannot seek, so whole-file retries must work without that.
		params.Reader = io.MultiReader(bytes.NewReader(content))
	} else {
		path := filepath.Join(os.TempDir(), fmt.Sprintf("rask-acceptance-%d-%s", os.Getpid(), call.Filename))
		if err := os.WriteFile(path, content, 0o600); err != nil {
			return nil, err
		}
		defer func() { _ = os.Remove(path) }()
		params.Path = path
	}
	response, err := r.client.Uploads.UploadFile(ctx, params)
	return metaOf(response), err
}

func canonical(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return value
	}
	return out
}

func sameJSON(a, b any) bool { return reflect.DeepEqual(canonical(a), canonical(b)) }

func errorClassMatches(err error, class string) bool {
	var raskErr *rask.RaskError
	var balance *rask.InsufficientBalanceError
	var jobFailed *rask.JobFailedError
	var jobCanceled *rask.JobCanceledError
	var waitTimeout *rask.WaitTimeoutError
	switch class {
	case "RaskError":
		return errors.As(err, &raskErr)
	case "InsufficientBalanceError":
		return errors.As(err, &balance)
	case "JobFailedError":
		return errors.As(err, &jobFailed)
	case "JobCanceledError":
		return errors.As(err, &jobCanceled)
	case "WaitTimeoutError":
		return errors.As(err, &waitTimeout)
	}
	return false
}

// typedProblemField reads a member of RaskError the way a caller does, through its typed field.
func typedProblemField(e *rask.RaskError, name string) (any, bool) {
	switch name {
	case "type":
		return e.Type, true
	case "title":
		return e.Title, true
	case "retryable":
		return e.Retryable, true
	case "retry":
		return e.Retry, true
	case "errors":
		return e.Errors, true
	case "credit_type":
		return e.CreditType, true
	case "required_minutes":
		return e.RequiredMinutes, e.RequiredMinutes != nil
	case "balance_minutes":
		return e.BalanceMinutes, e.BalanceMinutes != nil
	case "current_etag":
		return e.CurrentETag, true
	case "detail":
		return e.Detail, e.Detail != ""
	case "hint":
		return e.Hint, e.Hint != ""
	case "retry_after":
		return e.RetryAfter.Seconds(), e.RetryAfter > 0
	}
	// Everything else is a member of the typed Problem.
	encoded, err := json.Marshal(e.Problem)
	if err != nil {
		return nil, false
	}
	var members map[string]any
	if err := json.Unmarshal(encoded, &members); err != nil {
		return nil, false
	}
	value, ok := members[name]
	return value, ok
}

func checkRaised(exp expectation, present []string, outcome *callOutcome, records []record) []string {
	if outcome == nil || outcome.err == nil {
		return []string{"expected an error, the call returned"}
	}
	var failures []string
	if exp.ErrorClass != "" && !errorClassMatches(outcome.err, exp.ErrorClass) {
		failures = append(failures, fmt.Sprintf("expected %s, got %T", exp.ErrorClass, outcome.err))
	}
	var e *rask.RaskError
	if !errors.As(outcome.err, &e) {
		if exp.Status != nil || exp.Code != nil || exp.Fields != nil || present != nil || exp.RequestIDMatchesWire {
			failures = append(failures, fmt.Sprintf("expected a RaskError, got %T: %v", outcome.err, outcome.err))
		}
		return failures
	}
	if exp.Status != nil && e.Status != *exp.Status {
		failures = append(failures, fmt.Sprintf("expected status %d, got %d", *exp.Status, e.Status))
	}
	if exp.Code != nil && e.Code != *exp.Code {
		failures = append(failures, fmt.Sprintf("expected code %s, got %q", *exp.Code, e.Code))
	}
	for name, want := range exp.Fields {
		got, ok := typedProblemField(e, name)
		if !ok || !sameJSON(got, want) {
			failures = append(failures, fmt.Sprintf("field %s: expected %v, got %v", name, want, got))
		}
	}
	for _, name := range present {
		if _, ok := typedProblemField(e, name); !ok {
			failures = append(failures, "expected "+name+" on the error")
		}
	}
	if exp.RequestIDMatchesWire {
		known := false
		for _, rec := range records {
			if rec.Response != nil && rec.Response.Headers["X-Request-Id"] == e.RequestID {
				known = true
			}
		}
		if !known || e.RequestID == "" {
			failures = append(failures, "request_id "+e.RequestID+" is not an X-Request-Id the server sent")
		}
	}
	return failures
}

func headersOf(outcome *callOutcome) (http.Header, *rask.Meta) {
	if outcome.meta != nil {
		return outcome.meta.Header, outcome.meta
	}
	var e *rask.RaskError
	if errors.As(outcome.err, &e) {
		return e.Header, &rask.Meta{Header: e.Header, Body: e.Body}
	}
	return nil, nil
}

func checkHeader(exp expectation, outcome *callOutcome, records []record) []string {
	header, meta := headersOf(outcome)
	value := header.Get(exp.Name)
	if value == "" {
		return []string{"header " + exp.Name + " is not exposed"}
	}
	if exp.Equals != nil && value != exp.Equals {
		return []string{fmt.Sprintf("header %s: expected %v, got %s", exp.Name, exp.Equals, value)}
	}
	if exp.Matches != "" && !regexp.MustCompile(exp.Matches).MatchString(value) {
		return []string{fmt.Sprintf("header %s: %s does not match %s", exp.Name, value, exp.Matches)}
	}
	if exp.EqualsField != "" {
		field, _ := meta.Lookup(exp.EqualsField)
		if field != value {
			return []string{fmt.Sprintf("header %s differs from %s", exp.Name, exp.EqualsField)}
		}
	}
	if exp.MatchesWire {
		for _, rec := range records {
			if rec.Response != nil && rec.Response.Headers[exp.Name] == value {
				return nil
			}
		}
		return []string{"header " + exp.Name + " is not what the server sent"}
	}
	return nil
}

func keyOf(outcome *callOutcome) string {
	if outcome.meta != nil {
		return outcome.meta.IdempotencyKey
	}
	var e *rask.RaskError
	if errors.As(outcome.err, &e) {
		return e.IdempotencyKey
	}
	return ""
}

// checkInjected reads each injected value back from the result, the way a caller reads a value it has no type for.
func checkInjected(outcome *callOutcome, records []record) []string {
	var failures []string
	any := false
	for _, rec := range records {
		if rec.Response == nil {
			continue
		}
		for _, injection := range rec.Response.Injections {
			any = true
			meta := outcome.meta
			if outcome.results != nil {
				meta = outcome.results[rec.OperationID]
			}
			if meta == nil {
				failures = append(failures, rec.OperationID+": no result")
				continue
			}
			pointer := injection.Pointer
			if pointer == "/" {
				pointer = ""
			}
			got, ok := meta.Lookup(pointer)
			if !ok || !sameJSON(got, injection.Value) {
				failures = append(failures, fmt.Sprintf("%s lost the %s injected at %s", rec.OperationID, injection.Kind, injection.Pointer))
			}
		}
	}
	if !any {
		failures = append(failures, "nothing was injected")
	}
	return failures
}

// clientDump prints a client every way a program might: fmt verbs, JSON, and a walk over every field, exported or not.
func clientDump(client *rask.Client) string {
	var b strings.Builder
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d", "%T"} {
		fmt.Fprintf(&b, format+"\n", client)
	}
	value := reflect.ValueOf(client).Elem() // The struct itself, which has no Format method.
	fmt.Fprintf(&b, "%v %+v %#v\n", value, value, client.Jobs)
	if encoded, err := json.Marshal(client); err == nil {
		b.Write(encoded)
	}
	walkStrings(reflect.ValueOf(client), map[uintptr]bool{}, &b)
	return b.String()
}

func walkStrings(v reflect.Value, seen map[uintptr]bool, b *strings.Builder) {
	// A live transport's connection pool changes under the walk, and the key is never kept there.
	if v.IsValid() && (v.Type().PkgPath() == "net/http" || v.Type().PkgPath() == "sync") {
		return
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return
		}
		if seen[v.Pointer()] {
			return
		}
		seen[v.Pointer()] = true
		switch v.Kind() {
		case reflect.Pointer:
			walkStrings(v.Elem(), seen, b)
		case reflect.Slice:
			for i := range v.Len() {
				walkStrings(v.Index(i), seen, b)
			}
		case reflect.Map:
			for _, key := range v.MapKeys() {
				walkStrings(key, seen, b)
				walkStrings(v.MapIndex(key), seen, b)
			}
		}
	case reflect.Interface:
		if !v.IsNil() {
			walkStrings(v.Elem(), seen, b)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			walkStrings(v.Field(i), seen, b)
		}
	case reflect.String:
		b.WriteString(v.String() + "\n")
	}
}

func (r *scenarioRun) check(exp expectation, present []string, records []record) []string {
	index := len(r.calls) - 1
	if exp.Call != nil {
		index = *exp.Call
	}
	var outcome *callOutcome
	if index >= 0 && index < len(r.calls) {
		outcome = r.calls[index]
	}
	switch exp.Kind {
	case "returned":
		if outcome == nil {
			return []string{fmt.Sprintf("no call %d", index)}
		}
		if outcome.err != nil {
			return []string{fmt.Sprintf("call %d raised: %v", index, outcome.err)}
		}
		return nil
	case "returned_or_raised", "manual":
		return nil
	case "raised":
		return checkRaised(exp, present, outcome, records)
	case "raised_before_network":
		err := r.constructErr
		if err == nil && len(r.calls) > 0 {
			err = r.calls[0].err
		}
		if err == nil {
			return []string{"expected an error before any request"}
		}
		if !strings.Contains(err.Error(), exp.MessageContains) {
			return []string{fmt.Sprintf("error %q does not mention %s", err, exp.MessageContains)}
		}
		return nil
	case "config":
		if r.client == nil {
			return []string{fmt.Sprintf("no client: %v", r.constructErr)}
		}
		var got any
		switch exp.Field {
		case "base_url":
			got = r.client.BaseURL()
		case "timeout_seconds":
			got = r.client.Timeout().Seconds()
		case "jobs.wait.timeout_seconds":
			got = r.client.Jobs.DefaultWaitTimeout().Seconds()
		default:
			return []string{"unknown config field " + exp.Field}
		}
		if !sameJSON(got, exp.Equals) {
			return []string{fmt.Sprintf("config %s is %v", exp.Field, got)}
		}
		return nil
	case "repr_redacts":
		if r.client == nil {
			return []string{fmt.Sprintf("no client: %v", r.constructErr)}
		}
		if strings.Contains(clientDump(r.client), exp.Secret) {
			return []string{"the key shows in a dump of the client"}
		}
		return nil
	case "field":
		if outcome == nil {
			return []string{fmt.Sprintf("no call %d", index)}
		}
		if outcome.meta == nil {
			return []string{fmt.Sprintf("call raised: %v", outcome.err)}
		}
		if exp.RawStatus != nil && outcome.meta.StatusCode != *exp.RawStatus {
			return []string{fmt.Sprintf("status is %d", outcome.meta.StatusCode)}
		}
		if exp.Path == "" {
			return nil
		}
		value, ok := outcome.meta.Lookup(exp.Path)
		if exp.Present != nil {
			if !ok {
				return []string{exp.Path + " is missing"}
			}
			return nil
		}
		if !ok || !sameJSON(value, exp.Equals) {
			return []string{fmt.Sprintf("%s is %v", exp.Path, value)}
		}
		return nil
	case "header_exposed":
		if outcome == nil {
			return []string{"no call"}
		}
		return checkHeader(exp, outcome, records)
	case "idempotency_key_exposed":
		if outcome == nil {
			return []string{fmt.Sprintf("no call %d", index)}
		}
		key := keyOf(outcome)
		if key == "" {
			return []string{"the key is not exposed"}
		}
		if exp.Equals != nil && key != exp.Equals {
			return []string{"key is " + key}
		}
		if exp.MatchesWire {
			for _, rec := range records {
				if rec.Headers["idempotency-key"] == key {
					return nil
				}
			}
			return []string{"key " + key + " was never sent"}
		}
		return nil
	case "items":
		if outcome == nil {
			return []string{fmt.Sprintf("no call %d", index)}
		}
		if outcome.err != nil {
			return []string{fmt.Sprintf("no items: %v", outcome.err)}
		}
		if exp.Count != nil && len(outcome.items) != *exp.Count {
			return []string{fmt.Sprintf("saw %d items, expected %d", len(outcome.items), *exp.Count)}
		}
		if exp.IDs != nil {
			var ids []string
			for _, item := range outcome.items {
				var parsed struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(item, &parsed); err != nil {
					return []string{err.Error()}
				}
				ids = append(ids, parsed.ID)
			}
			if !reflect.DeepEqual(ids, exp.IDs) {
				return []string{fmt.Sprintf("saw ids %v", ids)}
			}
		}
		return nil
	case "pages":
		if outcome == nil || outcome.meta == nil {
			return []string{"no page"}
		}
		data, _ := outcome.meta.Lookup("/data")
		list, _ := data.([]any)
		if exp.ItemCount != nil && len(list) != *exp.ItemCount {
			return []string{fmt.Sprintf("page has %d items", len(list))}
		}
		hasMore, _ := outcome.meta.Lookup("/has_more")
		if exp.HasMore != nil && hasMore != *exp.HasMore {
			return []string{fmt.Sprintf("has_more is %v", hasMore)}
		}
		return nil
	case "warnings":
		var failures []string
		r.warnMu.Lock()
		defer r.warnMu.Unlock()
		if exp.Count != nil && len(r.warnings) != *exp.Count {
			failures = append(failures, fmt.Sprintf("saw %d warnings, expected %d", len(r.warnings), *exp.Count))
		}
		for name, want := range exp.PerOperation {
			seen := 0
			for _, warning := range r.warnings {
				if warning == name {
					seen++
				}
			}
			if seen != want {
				failures = append(failures, fmt.Sprintf("%s: saw %d warnings, expected %d", name, seen, want))
			}
		}
		return failures
	case "progress":
		if outcome == nil || exp.FinalBytes == nil || outcome.progress != *exp.FinalBytes {
			return []string{"on_progress did not reach the file size"}
		}
		return nil
	case "logs":
		return r.checkLogs(exp, records)
	case "injected_values_preserved":
		if outcome == nil {
			return []string{"no call"}
		}
		return checkInjected(outcome, records)
	}
	return []string{"unknown expectation " + exp.Kind}
}

func (r *scenarioRun) checkLogs(exp expectation, records []record) []string {
	logs := r.logs.String()
	var failures []string
	if exp.Empty && logs != "" {
		failures = append(failures, "expected no logs, got "+logs)
	}
	for _, text := range exp.MustContain {
		if !strings.Contains(logs, text) {
			failures = append(failures, "logs do not contain "+text)
		}
	}
	for _, text := range exp.MustNotContain {
		if strings.Contains(logs, text) {
			failures = append(failures, "logs contain a secret")
		}
	}
	for _, wire := range exp.MustNotContainWire {
		for _, rec := range records {
			if rec.OperationID != wire.OperationID {
				continue
			}
			value := rec.RawQueryParams[wire.Query]
			if value == "" {
				failures = append(failures, "the wire has no "+wire.Query+" to look for")
			} else if strings.Contains(logs, value) {
				failures = append(failures, "logs contain "+wire.Query)
			}
		}
	}
	return failures
}

// setEnv sets the environment to exactly what the scenario says for the SDK's three variables.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, name := range []string{"RASK_API_KEY", "RASK_BASE_URL", "RASK_LOG"} {
		t.Setenv(name, env[name])
		if _, set := env[name]; !set {
			if err := os.Unsetenv(name); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (h *harness) runScenario(t *testing.T, id string, operations []operationInfo) {
	t.Helper()
	var raw json.RawMessage
	if err := h.request(http.MethodPost, "/scenarios/"+id+"/start", map[string]string{"lang": lang}, &raw); err != nil {
		t.Fatal(err)
	}
	var s scenario
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	// "present" means a list of names on raised, a flag on field.
	var shape struct {
		ExpectSDK []struct {
			Present any `json:"present"`
		} `json:"expect_sdk"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	setEnv(t, s.Client.Env)

	run := &scenarioRun{h: h, operations: operations}
	run.newClient(&s)
	if run.client != nil {
		for _, call := range s.Calls {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			run.calls = append(run.calls, run.runCall(ctx, call))
			cancel()
		}
	}
	records := h.requests(t)
	var failures []string
	for i, exp := range s.ExpectSDK {
		var present []string
		if names, ok := shape.ExpectSDK[i].Present.([]any); ok {
			for _, name := range names {
				present = append(present, fmt.Sprint(name))
			}
		}
		failures = append(failures, run.check(exp, present, records)...)
	}
	var verdict struct {
		Pass     bool  `json:"pass"`
		Failures []any `json:"failures"`
	}
	body := map[string]any{"lang": lang, "sdk_passed": len(failures) == 0, "sdk_detail": strings.Join(failures, "; ")}
	if err := h.request(http.MethodPost, "/scenarios/"+id+"/verify", body, &verdict); err != nil {
		t.Fatal(err)
	}
	if !verdict.Pass {
		t.Errorf("%s: %v", id, verdict.Failures)
	}
}

// TestAcceptance runs every scenario that applies to Go, then asks the harness whether any was left out.
func TestAcceptance(t *testing.T) {
	h := useHarness(t)
	runSlow := os.Getenv("RUN_SLOW") == "1"
	if err := h.request(http.MethodDelete, "/report", nil, nil); err != nil {
		t.Fatal(err)
	}
	var list struct {
		Scenarios []struct {
			ID      string `json:"id"`
			Applies bool   `json:"applies"`
			Slow    bool   `json:"slow"`
		} `json:"scenarios"`
	}
	if err := h.request(http.MethodGet, "/scenarios?lang="+lang, nil, &list); err != nil {
		t.Fatal(err)
	}
	operations := h.operations(t)
	only := os.Getenv("RASK_SCENARIO")
	for _, entry := range list.Scenarios {
		if !entry.Applies || (entry.Slow && !runSlow) || (only != "" && !strings.HasPrefix(entry.ID, only)) {
			continue
		}
		t.Run(entry.ID, func(t *testing.T) { h.runScenario(t, entry.ID, operations) })
	}
	if only != "" {
		return
	}
	var report struct {
		Complete bool     `json:"complete"`
		Passed   []string `json:"passed"`
		Failed   []any    `json:"failed"`
		NotRun   []string `json:"not_run"`
		SlowNot  []string `json:"slow_not_run"`
	}
	query := "/report?lang=" + lang
	if runSlow {
		query += "&slow=true"
	}
	if err := h.request(http.MethodGet, query, nil, &report); err != nil {
		t.Fatal(err)
	}
	t.Logf("harness report: %d passed, %d failed, %d not run, %d slow not run", len(report.Passed), len(report.Failed), len(report.NotRun), len(report.SlowNot))
	if !report.Complete {
		t.Errorf("the harness says the run is not complete: failed %v, not run %v", report.Failed, report.NotRun)
	}
}

// TestEveryOperationIsCallable calls all 82 operations through their typed methods on a prefixed base URL, and checks
// that each answered as declared, stayed under the prefix on the API host, carried the key, and broke no contract rule.
func TestEveryOperationIsCallable(t *testing.T) {
	h := useHarness(t)
	setEnv(t, nil)
	if err := h.request(http.MethodPost, "/reset", nil, nil); err != nil {
		t.Fatal(err)
	}
	client, err := rask.New(rask.WithAPIKey(h.apiKey), rask.WithBaseURL(h.baseURL), rask.WithHTTPClient(safeHTTPClient()))
	if err != nil {
		t.Fatal(err)
	}
	operations := h.operations(t)
	if len(operations) != 82 || len(dispatch) != 82 {
		t.Fatalf("%d operations in the contract, %d in the dispatch table, want 82", len(operations), len(dispatch))
	}
	prefix := h.baseURL[strings.Index(h.baseURL[len("http://"):], "/")+len("http://"):]
	for _, op := range operations {
		input := &opCall{path: op.Sample.Path, query: op.Sample.Query, body: op.Sample.Body, etag: op.Sample.Headers["If-Match"]}
		meta, err := callOperation(context.Background(), client, op.OperationID, input, nil)
		if err != nil {
			t.Errorf("%s: %v", op.OperationID, err)
			continue
		}
		if meta.StatusCode >= 300 {
			t.Errorf("%s: status %d", op.OperationID, meta.StatusCode)
		}
	}
	seen := map[string]bool{}
	for _, rec := range h.requests(t) {
		seen[rec.OperationID] = true
		if rec.Listener != "api" {
			t.Errorf("%s reached the %s listener", rec.OperationID, rec.Listener)
		}
		if !strings.HasPrefix(rec.Path, prefix+"/") {
			t.Errorf("%s left the prefix: %s", rec.OperationID, rec.Path)
		}
		if rec.Headers["authorization"] != "Bearer "+h.apiKey {
			t.Errorf("%s did not carry the key", rec.OperationID)
		}
		if !strings.HasPrefix(rec.Headers["x-rask-client"], "rask-sdk-go/") {
			t.Errorf("%s: X-Rask-Client is %q", rec.OperationID, rec.Headers["x-rask-client"])
		}
		if len(rec.Violations) != 0 {
			t.Errorf("%s broke the contract: %v", rec.OperationID, rec.Violations)
		}
	}
	if len(seen) != 82 {
		t.Errorf("the harness saw %d operations, want 82", len(seen))
	}
}

// contractParameter is a query parameter as the contract declares it.
type contractParameter struct {
	Name    string          `json:"name"`
	In      string          `json:"in"`
	Ref     string          `json:"$ref"`
	Example json.RawMessage `json:"example"`
	Schema  contractSchema  `json:"schema"`
}

type contractSchema struct {
	Ref     string          `json:"$ref"`
	Type    json.RawMessage `json:"type"`
	Format  string          `json:"format"`
	Enum    []any           `json:"enum"`
	Pattern string          `json:"pattern"`
	Minimum *float64        `json:"minimum"`
	Items   *contractSchema `json:"items"`
}

// sampleValue builds a valid value from a schema, for parameters whose example does not exist.
func sampleValue(schema contractSchema, schemas map[string]contractSchema) any {
	if schema.Ref != "" {
		return sampleValue(schemas[schema.Ref[strings.LastIndex(schema.Ref, "/")+1:]], schemas)
	}
	if len(schema.Enum) > 0 {
		return schema.Enum[0]
	}
	switch strings.Trim(string(schema.Type), `"`) {
	case "integer", "number":
		if schema.Minimum != nil {
			return *schema.Minimum
		}
		return 1
	case "boolean":
		return true
	case "array":
		return []any{sampleValue(*schema.Items, schemas)}
	}
	if strings.Contains(schema.Pattern, "SPEAKER_") {
		return "SPEAKER_01"
	}
	switch schema.Format {
	case "date-time":
		return "2026-01-02T03:04:05Z"
	case "date":
		return "2026-01-02"
	}
	return "sample"
}

// TestEveryQueryParameterEncodes sets every optional query parameter of every operation at once, through the typed
// Params structs, and lets the harness judge the encoding. The samples of TestEveryOperationIsCallable leave them out.
func TestEveryQueryParameterEncodes(t *testing.T) {
	h := useHarness(t)
	setEnv(t, nil)
	raw, err := os.ReadFile("api/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Parameters map[string]contractParameter `json:"parameters"`
			Schemas    map[string]contractSchema    `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	queryByOperation := map[string][]contractParameter{}
	for _, item := range document.Paths {
		for method, body := range item {
			if method == "parameters" {
				continue
			}
			var operation struct {
				OperationID string              `json:"operationId"`
				Parameters  []contractParameter `json:"parameters"`
			}
			if err := json.Unmarshal(body, &operation); err != nil {
				t.Fatal(err)
			}
			for _, parameter := range operation.Parameters {
				if parameter.Ref != "" {
					parameter = document.Components.Parameters[parameter.Ref[strings.LastIndex(parameter.Ref, "/")+1:]]
				}
				if parameter.In == "query" && parameter.Name != "cursor" {
					queryByOperation[operation.OperationID] = append(queryByOperation[operation.OperationID], parameter)
				}
			}
		}
	}
	client, err := rask.New(rask.WithAPIKey(h.apiKey), rask.WithBaseURL(h.baseURL), rask.WithHTTPClient(safeHTTPClient()))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, op := range h.operations(t) {
		parameters := queryByOperation[op.OperationID]
		if len(parameters) == 0 {
			continue
		}
		if err := h.request(http.MethodPost, "/reset", nil, nil); err != nil {
			t.Fatal(err)
		}
		query := map[string]any{}
		for _, parameter := range parameters {
			total++
			var example any
			if len(parameter.Example) > 0 && json.Unmarshal(parameter.Example, &example) == nil && example != nil {
				query[parameter.Name] = example
				continue
			}
			query[parameter.Name] = sampleValue(parameter.Schema, document.Components.Schemas)
		}
		input := &opCall{path: op.Sample.Path, query: query, body: op.Sample.Body, etag: op.Sample.Headers["If-Match"]}
		if _, err := callOperation(context.Background(), client, op.OperationID, input, nil); err != nil {
			t.Errorf("%s: %v", op.OperationID, err)
			continue
		}
		records := h.requests(t)
		if len(records) != 1 {
			t.Errorf("%s: %d requests", op.OperationID, len(records))
			continue
		}
		for _, parameter := range parameters {
			if records[0].RawQueryParams[parameter.Name] == "" {
				t.Errorf("%s: %s did not reach the wire", op.OperationID, parameter.Name)
			}
		}
		if len(records[0].Violations) != 0 {
			t.Errorf("%s: %v", op.OperationID, records[0].Violations)
		}
	}
	t.Logf("%d query parameters over %d operations", total, len(queryByOperation))
	if total == 0 {
		t.Fatal("no query parameters found")
	}
}

// The scenario runner itself, without a harness. What it cannot run must come out as a failure that /verify hears of:
// a panic ends the test binary first, and leaves the harness process running.

func TestAnOperationOutsideTheDispatchTableIsAnError(t *testing.T) {
	run := &scenarioRun{
		client:     newTestClient(t, "http://127.0.0.1:1"),
		operations: []operationInfo{{OperationID: "noSuchOperation", HasJSONResponse: true}},
	}
	for name, call := range map[string]scenarioCall{
		"operation":        {Op: "noSuchOperation"},
		"first_page":       {Helper: "first_page", Op: "noSuchOperation"},
		"iterate":          {Helper: "iterate", Op: "noSuchOperation"},
		"sweep_operations": {Helper: "sweep_operations"},
	} {
		t.Run(name, func(t *testing.T) {
			outcome := run.runCall(context.Background(), call)
			if outcome.err == nil || !strings.Contains(outcome.err.Error(), "noSuchOperation") {
				t.Errorf("got %v", outcome.err)
			}
		})
	}
}

func TestNextPageKeepsThePathTheFiltersAndTheNewCursor(t *testing.T) {
	server := newRecorder(t, func(w http.ResponseWriter, req *http.Request) {
		next := map[string]string{"c0": "c1", "c1": "c2", "c2": "c3"}[req.URL.Query().Get("cursor")]
		writeJSON(w, 200, fmt.Sprintf(`{"object":"list","data":[],"next_cursor":%q,"has_more":true}`, next))
	})
	run := &scenarioRun{client: newTestClient(t, server.URL)}
	for _, call := range []scenarioCall{
		{Helper: "first_page", Op: "listSegments", Path: map[string]any{"project_id": "prj-1"}, Query: map[string]any{"cursor": "c0", "limit": 2, "q": "hola"}},
		{Helper: "next_page", AfterCall: 0},
		{Helper: "next_page", AfterCall: 1},
	} {
		outcome := run.runCall(context.Background(), call)
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		run.calls = append(run.calls, outcome)
	}
	var got []string
	for _, request := range server.seen() {
		query, err := url.ParseQuery(request.RawQuery)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, request.Path+"?"+query.Encode()) // Encode sorts the names.
	}
	want := []string{
		"/projects/prj-1/segments?cursor=c0&limit=2&q=hola",
		"/projects/prj-1/segments?cursor=c1&limit=2&q=hola",
		"/projects/prj-1/segments?cursor=c2&limit=2&q=hola",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requests %q, want %q", got, want)
	}
}

func TestAnExpectationOfACallThatNeverRanIsAFailure(t *testing.T) {
	run := &scenarioRun{} // As after a client that could not be built: no calls.
	for _, kind := range []string{
		"returned", "raised", "field", "header_exposed", "idempotency_key_exposed", "items", "pages", "progress",
		"injected_values_preserved",
	} {
		t.Run(kind, func(t *testing.T) {
			if failures := run.check(expectation{Kind: kind}, nil, nil); len(failures) == 0 {
				t.Error("expected a failure")
			}
		})
	}
}
