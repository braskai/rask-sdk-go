package rask

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/braskai/rask-sdk-go/v3/api"
)

const (
	waitLongPollSeconds = 20.0
	waitDefaultPause    = time.Second
)

// WaitOption adjusts Jobs.Wait.
type WaitOption func(*waitConfig)

type waitConfig struct{ timeout time.Duration }

// WithWaitTimeout sets how long Jobs.Wait waits (default DefaultWaitTimeout). The job is left running when it expires.
func WithWaitTimeout(d time.Duration) WaitOption { return func(w *waitConfig) { w.timeout = d } }

func newWaitConfig(opts []WaitOption) waitConfig {
	cfg := waitConfig{timeout: DefaultWaitTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// DefaultWaitTimeout returns the timeout Wait uses without WithWaitTimeout.
func (s *JobsService) DefaultWaitTimeout() time.Duration { return newWaitConfig(nil).timeout }

// Wait long-polls a job (wait_seconds=20, honouring Retry-After) until it finishes. It returns the succeeded job,
// *JobFailedError with the job's problem, *JobCanceledError, or *WaitTimeoutError. A status this SDK does not know
// counts as still running, and a passing outage does not end the wait: it lasts until the timeout. Wait never cancels
// the job.
func (s *JobsService) Wait(ctx context.Context, jobID string, opts ...WaitOption) (*Response[api.Job], error) {
	cfg := newWaitConfig(opts)
	deadline := time.Now().Add(cfg.timeout)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	lastStatus := ""
	timedOut := func() error { return &WaitTimeoutError{JobID: jobID, Timeout: cfg.timeout, LastStatus: lastStatus} }
	var etag *string
	outages := 0
	for {
		remaining := math.Ceil(time.Until(deadline).Seconds())
		params := api.GetJobParams{
			WaitSeconds: Ptr(api.WaitSeconds(max(min(waitLongPollSeconds, remaining), 0))),
			IfNoneMatch: etag,
		}
		response, err := s.GetJob(waitCtx, jobID, &params)

		pause := waitDefaultPause
		var decodeError *DecodeError
		if err != nil && !errors.As(err, &decodeError) {
			switch {
			case ctx.Err() != nil:
				return nil, err
			case waitCtx.Err() != nil:
				return nil, timedOut()
			case !isTransient(err):
				return nil, err
			}
			pause = max(s.c.backoff(outages), retryAfterOf(err))
			outages++
		} else {
			outages = 0
			if value := response.ETag(); value != "" {
				etag = &value
			}
			if wait, ok := response.RetryAfter(); ok {
				pause = wait
			}
			if !response.NotModified() {
				// Read from the raw body: a job whose other fields stopped fitting their types can still finish.
				status, _ := response.Lookup("/status")
				lastStatus, _ = status.(string)
				switch api.JobStatus(lastStatus) {
				case api.JobStatusSucceeded:
					return response, nil
				case api.JobStatusFailed:
					return nil, &JobFailedError{RaskError: jobProblem(s.c, response), JobID: jobID, Job: response.Data}
				case api.JobStatusCanceled:
					return nil, &JobCanceledError{RaskError: jobProblem(s.c, response), JobID: jobID, Job: response.Data}
				}
			}
		}

		if time.Now().Add(pause).After(deadline) {
			return nil, timedOut()
		}
		if err := s.c.sleep(waitCtx, pause); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, timedOut()
		}
	}
}

func retryAfterOf(err error) time.Duration {
	var problem *RaskError
	if errors.As(err, &problem) {
		return problem.RetryAfter
	}
	return 0
}

// jobProblem turns Job.error into a RaskError. A canceled job may have none. Status is what the same failure would
// have answered synchronously, as the contract defines it.
func jobProblem(c *Client, response *Response[api.Job]) *RaskError {
	var body []byte
	status := 0
	if raw, ok := response.Lookup("/error"); ok && raw != nil {
		if encoded, err := json.Marshal(raw); err == nil {
			body = encoded
		}
		if code, ok := response.Lookup("/error/status"); ok {
			if number, isNumber := code.(json.Number); isNumber {
				parsed, _ := number.Int64()
				status = int(parsed)
			}
		}
	}
	e := baseError("getJob", status, response.Header, body, "", c.now())
	e.RetryAfter = 0 // The header on this response is the poll hint, not a statement about the failure.
	return e
}
