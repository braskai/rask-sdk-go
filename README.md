# Rask Go SDK

The Go client for the [Rask API v3](https://docs.api.rask.ai). Version 3.0.0-alpha.0.

```shell
go get github.com/braskai/rask-sdk-go/v3
```

Go 1.25 or later. The types are in the `api` subpackage, generated from the contract in `api/openapi.json`.

## Key and base URL

```go
client, err := rask.New() // reads RASK_API_KEY, and RASK_BASE_URL if set
client, err = rask.New(
	rask.WithAPIKey(key),
	rask.WithBaseURL("https://gw.example.com/rask/v3"), // path prefix kept, trailing slash ignored
)
```

The base URL defaults to `https://api.rask.ai/v3`; an argument wins over the environment. It applies to every operation and
helper. The key goes to this URL and nowhere else: presigned uploads go without `Authorization`, and a redirect that leaves
it (another host, port or scheme, or out of the path prefix) drops the key for the rest of that request. The API's
`Location` and `links.*` always say `/v3/...`, whatever the prefix, so use the ids instead of following them.

Other options: `WithTimeout` (60 s per attempt), `WithMaxRetries`, `WithHTTPClient`, `WithLogger`, `WithDebug`,
`WithDeprecationHandler`.

## Worked example

Upload, `createProject`, `jobs.wait`, download:

```go
// Command quickstart uploads a video, dubs it into Spanish and downloads the result.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/braskai/rask-sdk-go/v3"
	"github.com/braskai/rask-sdk-go/v3/api"
)

func main() {
	ctx := context.Background()
	client, err := rask.New() // RASK_API_KEY, and RASK_BASE_URL if set
	if err != nil {
		log.Fatal(err)
	}

	// Upload: createUpload, the presigned PUT, completeUpload. Then wait for the import.
	imported, err := client.Uploads.UploadFile(ctx, rask.UploadFileParams{Path: "teaser.mp4"})
	if err != nil {
		log.Fatal(err)
	}
	media, err := client.Jobs.Wait(ctx, imported.Data.Id)
	if err != nil {
		log.Fatal(err)
	}
	mediaID, err := media.Data.MediaId.Get()
	if err != nil {
		log.Fatal(err)
	}

	// Create the project. It dubs itself after translation, up to this many minutes.
	created, err := client.Projects.CreateProject(ctx, api.CreateProjectRequest{
		MediaId:             mediaID,
		TargetLanguage:      "es-es",
		ExpectedCostMinutes: rask.Ptr(int32(5)),
	})
	if err != nil {
		log.Fatal(err)
	}
	processed, err := client.Jobs.Wait(ctx, created.Data.Id)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the dub that followed.
	result, err := processed.Data.Result.Get()
	if err != nil {
		log.Fatal(err)
	}
	processing, err := result.AsProjectProcessingJobResult()
	if err != nil {
		log.Fatal(err)
	}
	dubJobID, err := processing.AutoDub.DubJobId.Get()
	if err != nil {
		log.Fatalf("no dub: %s", processing.AutoDub.State)
	}
	if _, err := client.Jobs.Wait(ctx, dubJobID); err != nil {
		log.Fatal(err)
	}

	// Download. Output URLs are presigned, so plain HTTP without the API key does it.
	outputs, err := client.Exports.ListProjectOutputs(ctx, processing.ProjectId)
	if err != nil {
		log.Fatal(err)
	}
	for _, output := range outputs.Data.Data {
		if output.Kind != api.OutputKindDubbedVideo {
			continue
		}
		url, err := output.Url.Get()
		if err != nil {
			log.Fatal("the dubbed video has no URL yet")
		}
		if err := download(ctx, url, output.Filename); err != nil {
			log.Fatal(err)
		}
		fmt.Println("saved", output.Filename)
	}
}

func download(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download: status %d", response.StatusCode)
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	_, err = io.Copy(file, response.Body)
	return err
}
```

## How calls behave

Operations are grouped by resource and named after their `operationId`: `client.Folders.CreateFolder`,
`client.Jobs.GetJob`. Every call returns a `*rask.Response[T]` with `Data`, `StatusCode`, `Header`, the raw `Body`,
`IdempotencyKey`, and `ETag()`, `Location()`, `RequestID()`, `IdempotentReplayed()`, `RetryAfter()`.

- **Retries.** 429, 502, 503, 504, lost connections and timeouts, from 0.5 s growing by 1.5 up to 30 s with jitter, within
  120 s, honouring `Retry-After`. 500 and 409 are not retried, except `idempotency_key_in_use` (3 times).
- **Idempotency.** A UUIDv4 `Idempotency-Key` per call on POST, PATCH, PUT and `DeleteSegment`, the same across retries.
  `rask.WithIdempotencyKey` overrides it; it is on results and on errors.
- **Paging.** `ListProjects` returns one page. `ListProjectsAll` yields items and `ListProjectsPages` yields pages, as
  `iter.Seq2`.
- **Jobs.** `client.Jobs.Wait(ctx, id)` waits up to 4 h (`rask.WithWaitTimeout`). It returns the succeeded job,
  `*JobFailedError`, `*JobCanceledError` or `*WaitTimeoutError`, and never cancels the job.
- **Uploads.** `client.Uploads.UploadFile` takes a path or a reader and `OnProgress`. A failed PUT is retried with the whole
  file 3 times, and an expired URL is re-signed. A reader that cannot seek is first copied to a temporary file.
- **ETags.** Writes that require `If-Match` take `ifMatch` as an argument and send it verbatim, quotes included; an empty one
  fails before any request. `UpdateTranslationSettings` takes `rask.WithIfMatch`. A 412 has `CurrentETag`.
- **Errors.** `errors.As(err, &e)` with `*rask.RaskError` catches every API error, with `Status`, `Code`, `Detail`,
  `RequestID`, `Retry`, `Hint`, `Errors` and the whole `Problem`. Per-status types narrow it: `*rask.NotFoundError`,
  `*rask.InsufficientBalanceError`, `*rask.RateLimitError` and so on. Branch on `Code`. `*rask.ConnectionError` means no
  answer came, `*rask.UploadError` that storage refused the file.
- **Unknown values.** They never fail a call. An enum keeps a value this SDK does not know, a union keeps an unknown variant
  as raw JSON, and any field without a Go type reads through `resp.Lookup("/json/pointer")`. A body that breaks a type
  returns `*rask.DecodeError` beside the response: the call took effect, so do not retry it.
- **Clearing.** `api.UpdateProjectRequest{FolderId: rask.Ptr("")}` sends `""`, which clears the field. Unset fields are
  omitted, and `null` is never sent.
- **Deprecation.** The first `Deprecation` header of an operation warns once, on the logger or through
  `WithDeprecationHandler`.
- **Debug logs.** `RASK_LOG=debug` or `WithDebug(true)` logs traffic at `slog.LevelDebug`, without bodies and with
  `Authorization` and presigned query strings redacted.

## Development

```shell
make generate        # regenerate api/api.gen.go and resources.gen.go from api/openapi.json
make build lint test # test includes the acceptance suite when it finds the harness
make acceptance      # fails when the harness is missing; RASK_SDK_ACCEPTANCE_BIN points at it
make smoke           # go run ./examples/smoke; needs RASK_API_KEY and RASK_BASE_URL
```

Nothing here publishes.

## License

MIT. See [LICENSE](LICENSE).
