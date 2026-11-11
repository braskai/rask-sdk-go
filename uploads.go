package rask

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/braskai/rask-sdk-go/v3/api"
)

const (
	uploadMaxRetries  = 3 // Whole-file retries after the first PUT (S7).
	uploadMaxResigns  = 3
	uploadDetailBytes = 4096
)

// UploadFileParams says what UploadFile sends. Give Path or Reader.
type UploadFileParams struct {
	// Path is a file to upload.
	Path string
	// Reader is a stream to upload. An io.ReadSeeker is re-read from its current offset on a retry; any other reader,
	// and one whose Seek fails (os.Stdin as a pipe), is first copied to a temporary file, because storage takes the
	// file whole on every attempt.
	Reader io.Reader
	// Size is optional: the SDK measures the stream. When set, a stream of another length is refused before anything
	// is sent.
	Size int64
	// Filename defaults to the base name of Path; it is required with Reader.
	Filename string
	// ContentType is one of the accepted types (api.UploadContentType). It defaults by Path's or Filename's extension.
	ContentType string
	// Purpose defaults to project_source on the server.
	Purpose api.MediaPurpose
	// OnProgress reports the bytes sent so far. It starts over on a whole-file retry and ends at the file's size.
	OnProgress func(sentBytes, totalBytes int64)
}

// UploadFile uploads one file: createUpload, one presigned PUT without Authorization, then completeUpload. It returns
// the media_import job that completeUpload starts; wait for it with Jobs.Wait. A failed PUT is retried with the whole
// file 3 times, and a URL that expired is re-signed through getUpload. Errors never contain the presigned URL.
func (s *UploadsService) UploadFile(ctx context.Context, in UploadFileParams) (*Response[api.Job], error) {
	source, err := newUploadSource(in)
	if err != nil {
		return nil, err
	}
	defer source.close()

	created, err := s.CreateUpload(ctx, api.CreateUploadRequest{
		Filename:    source.filename,
		ContentType: api.UploadContentType(source.contentType),
		SizeBytes:   source.size,
		Purpose:     purposeOf(in.Purpose),
	})
	if err != nil {
		return nil, err
	}
	if err := s.put(ctx, created, source, in.OnProgress); err != nil {
		return nil, err
	}
	return s.CompleteUpload(ctx, created.Data.Id, nil)
}

func purposeOf(purpose api.MediaPurpose) *api.MediaPurpose {
	if purpose == "" {
		return nil
	}
	return &purpose
}

// transfer is UploadTransfer's single variant, read from the raw body so that every signed header survives.
type transfer struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

func transferOf(id string, meta *Meta) (*transfer, error) {
	raw, ok := meta.Lookup("/transfer")
	if !ok || raw == nil {
		return nil, fmt.Errorf("rask: upload %s has no transfer: it is not pending, or the key lacks media:write", id)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("rask: upload %s: %w", id, err)
	}
	var t transfer
	if err := json.Unmarshal(encoded, &t); err != nil {
		return nil, fmt.Errorf("rask: upload %s: the transfer does not parse", id)
	}
	if t.Method != string(api.SingleUploadTransferMethodSingle) {
		return nil, fmt.Errorf("rask: upload %s: transfer method %q is not supported by this SDK version", id, t.Method)
	}
	return &t, nil
}

// put sends the file until storage accepts it, or the retries run out.
func (s *UploadsService) put(ctx context.Context, created *Response[api.Upload], source *uploadSource, progress func(int64, int64)) error {
	c := s.c
	id := created.Data.Id
	t, err := transferOf(id, &created.Meta)
	if err != nil {
		return err
	}
	failures, resigns := 0, 0
	resign := func() error {
		resigns++
		fresh, err := s.GetUpload(ctx, id)
		if err != nil {
			return err
		}
		t, err = transferOf(id, &fresh.Meta)
		return err
	}
	if !t.ExpiresAt.IsZero() && !c.now().Before(t.ExpiresAt) {
		if err := resign(); err != nil {
			return err
		}
	}
	for {
		outcome, err := s.putOnce(ctx, id, t, source, progress)
		if err != nil {
			return err
		}
		if outcome.ok {
			if progress != nil {
				progress(source.size, source.size)
			}
			return nil
		}
		if outcome.expired && resigns < uploadMaxResigns {
			if err := resign(); err != nil {
				return err
			}
			continue
		}
		failures++
		if !outcome.retryable || failures > uploadMaxRetries {
			return &UploadError{UploadID: id, Attempts: failures, Status: outcome.status, Err: outcome.err, Detail: outcome.detail}
		}
		wait := max(c.backoff(failures-1), outcome.retryAfter)
		if err := c.sleep(ctx, wait); err != nil {
			return &UploadError{UploadID: id, Attempts: failures, Status: outcome.status, Err: err}
		}
	}
}

type putOutcome struct {
	ok, expired, retryable bool
	status                 int
	retryAfter             time.Duration
	detail                 string
	err                    error
}

// putOnce makes one PUT. It returns an error only for what no retry can fix. The request is built here, not by the API
// path: it carries the signed headers and no Authorization, and a stalled transfer is cut by the timeout, while a
// large file may take as long as it keeps moving.
func (s *UploadsService) putOnce(ctx context.Context, id string, t *transfer, source *uploadSource, progress func(int64, int64)) (putOutcome, error) {
	c := s.c
	target, err := url.Parse(t.URL)
	if err != nil || !target.IsAbs() {
		return putOutcome{}, fmt.Errorf("rask: upload %s: the presigned URL does not parse", id)
	}
	if err := ctx.Err(); err != nil {
		return putOutcome{}, err
	}
	if signed, ok := t.Headers["Content-Length"]; ok && signed != strconv.FormatInt(source.size, 10) {
		return putOutcome{}, fmt.Errorf("rask: upload %s: storage signed %s bytes, the file has %d", id, signed, source.size)
	}
	body, err := source.open()
	if err != nil {
		return putOutcome{}, fmt.Errorf("rask: upload %s: %w", id, err)
	}
	defer func() { _ = body.Close() }()

	putCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var stall *time.Timer
	if c.timeout > 0 {
		stall = time.AfterFunc(c.timeout, func() { cancel(errors.New("the transfer stalled")) })
		defer stall.Stop()
	}
	counted := &progressReader{reader: body, total: source.size, report: progress, stall: stall, stallAfter: c.timeout}
	req, err := http.NewRequestWithContext(putCtx, http.MethodPut, t.URL, counted)
	if err != nil {
		return putOutcome{}, fmt.Errorf("rask: upload %s: the presigned URL does not parse", id)
	}
	req.ContentLength = source.size
	req.GetBody = func() (io.ReadCloser, error) { return source.open() } // For a storage redirect.
	for name, value := range t.Headers {
		if strings.EqualFold(name, "Authorization") {
			continue // The contract forbids it, and the key is never added here.
		}
		if strings.EqualFold(name, "Content-Length") {
			continue // Set above; net/http writes it from ContentLength.
		}
		if strings.EqualFold(name, "Host") {
			req.Host = value
			continue
		}
		req.Header.Set(name, value)
	}
	req.Header.Set("User-Agent", c.userAgent)

	started := c.now()
	c.logRequest(ctx, req, 0)
	response, err := c.http.Do(req)
	if err != nil {
		c.logFailure(ctx, req, err)
		if ctx.Err() != nil {
			return putOutcome{}, ctx.Err()
		}
		cause := redactError(err)
		if stalled := context.Cause(putCtx); stalled != nil {
			cause = stalled
		}
		return putOutcome{retryable: true, err: cause}, nil
	}
	defer func() { _ = response.Body.Close() }()
	if stall != nil {
		stall.Reset(c.timeout)
	}
	detail, readErr := io.ReadAll(io.LimitReader(response.Body, uploadDetailBytes))
	c.logResponse(ctx, req, response, c.now().Sub(started))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if readErr != nil {
			return putOutcome{retryable: true, err: redactError(readErr)}, nil
		}
		return putOutcome{ok: true}, nil
	}
	outcome := putOutcome{status: response.StatusCode, detail: string(detail)}
	outcome.expired = storageSaysExpired(response.StatusCode, detail)
	outcome.retryable = response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout
	outcome.retryAfter, _ = parseRetryAfter(response.Header, c.now())
	return outcome, nil
}

// storageSaysExpired recognises S3's refusal of a URL past its expiry: 403 AccessDenied "Request has expired", or the
// token form.
func storageSaysExpired(status int, body []byte) bool {
	if status != http.StatusForbidden && status != http.StatusBadRequest {
		return false
	}
	return bytes.Contains(body, []byte("Request has expired")) || bytes.Contains(body, []byte("ExpiredToken"))
}

type progressReader struct {
	reader     io.Reader
	sent       int64
	total      int64
	report     func(int64, int64)
	stall      *time.Timer
	stallAfter time.Duration
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.sent += int64(n)
		if r.stall != nil {
			r.stall.Reset(r.stallAfter)
		}
		if r.report != nil {
			r.report(r.sent, r.total)
		}
	}
	return n, err
}

// uploadSource re-opens the same bytes for each attempt.
type uploadSource struct {
	filename    string
	contentType string
	size        int64
	open        func() (io.ReadCloser, error)
	close       func()
}

func newUploadSource(in UploadFileParams) (*uploadSource, error) {
	if (in.Path == "") == (in.Reader == nil) {
		return nil, errors.New("rask: UploadFile needs Path or Reader, not both")
	}
	source := &uploadSource{filename: in.Filename, contentType: in.ContentType, close: func() {}}
	var err error
	if in.Path != "" {
		err = source.fromPath(in.Path)
	} else {
		err = source.fromReader(in.Reader, in.Size)
	}
	if err != nil {
		return nil, err
	}
	if source.filename == "" {
		source.close()
		return nil, errors.New("rask: UploadFile needs Filename with Reader")
	}
	if source.contentType == "" {
		source.contentType = contentTypeByExtension(source.filename)
	}
	if source.contentType == "" {
		source.close()
		return nil, fmt.Errorf("rask: UploadFile cannot tell the content type of %q: set ContentType", source.filename)
	}
	if source.size <= 0 {
		source.close()
		return nil, errors.New("rask: UploadFile: the file is empty")
	}
	return source, nil
}

func (s *uploadSource) fromPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("rask: UploadFile: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("rask: UploadFile: %s is not a regular file", path)
	}
	s.size = info.Size()
	if s.filename == "" {
		s.filename = filepath.Base(path)
	}
	s.open = func() (io.ReadCloser, error) { return os.Open(path) } //nolint:gosec // The caller's own file.
	return nil
}

// seekable returns the reader as a seeker, with its current offset. A reader may have a Seek method that fails: a pipe
// is an *os.File, and os.Stdin is often a pipe. Such a reader does not count, and nothing has been read from it yet.
func seekable(reader io.Reader) (io.ReadSeeker, int64, bool) {
	seeker, ok := reader.(io.ReadSeeker)
	if !ok {
		return nil, 0, false
	}
	start, err := seeker.Seek(0, io.SeekCurrent)
	return seeker, start, err == nil
}

func (s *uploadSource) fromReader(reader io.Reader, size int64) error {
	if seeker, start, ok := seekable(reader); ok {
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("rask: UploadFile: %w", err)
		}
		s.size = end - start
		if size != 0 && size != s.size {
			return fmt.Errorf("rask: UploadFile: Size is %d, the stream has %d bytes", size, s.size)
		}
		s.open = func() (io.ReadCloser, error) {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				return nil, err
			}
			return io.NopCloser(io.LimitReader(seeker, s.size)), nil
		}
		return nil
	}
	spool, err := os.CreateTemp("", "rask-upload-*")
	if err != nil {
		return fmt.Errorf("rask: UploadFile: %w", err)
	}
	s.close = func() { _ = os.Remove(spool.Name()) }
	written, err := io.Copy(spool, reader)
	if closeErr := spool.Close(); err == nil {
		err = closeErr
	}
	if err == nil && size != 0 && size != written {
		err = fmt.Errorf("the stream has %d bytes, Size says %d", written, size)
	}
	if err != nil {
		s.close()
		return fmt.Errorf("rask: UploadFile: %w", err)
	}
	s.size = written
	s.open = func() (io.ReadCloser, error) { return os.Open(spool.Name()) }
	return nil
}

// contentTypeByExtension covers the contract's UploadContentType values.
func contentTypeByExtension(name string) string {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mp4":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".mkv":
		return "video/x-matroska"
	case ".webm":
		return "video/webm"
	case ".avi":
		return "video/x-msvideo"
	case ".m4v":
		return "video/x-m4v"
	case ".3gp":
		return "video/3gpp"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".flac":
		return "audio/flac"
	case ".m4a":
		return "audio/x-m4a"
	}
	return ""
}
