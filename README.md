# Rask Go SDK

The official Go client for the [Rask AI API](https://docs.api.rask.ai): upload media, create dubbing projects, edit transcriptions, manage glossaries and run lip-sync.

The client is generated from the API's OpenAPI description, so every API operation is available as a typed method.

## Installation

```shell
go get github.com/braskai/rask-sdk-go
```

Requires Go 1.24 or later.

## Authentication

The API uses OAuth 2.0 client credentials. Pass your client ID and secret to `NewClient`. The client fetches an access token on the first request, caches it until shortly before it expires, and refreshes it once if the API answers `401`.

```go
client, err := rask.NewClient(os.Getenv("RASK_CLIENT_ID"), os.Getenv("RASK_CLIENT_SECRET"))
```

To use a token you obtained yourself, pass empty credentials and `rask.WithAccessToken(token)`.

Other options: `WithBaseURL`, `WithTokenURL`, `WithScopes`, `WithHTTPClient` (timeouts, proxies, custom transports) and `WithUserAgent`.

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	rask "github.com/braskai/rask-sdk-go"
)

func main() {
	ctx := context.Background()
	client, err := rask.NewClient(os.Getenv("RASK_CLIENT_ID"), os.Getenv("RASK_CLIENT_SECRET"))
	if err != nil {
		log.Fatal(err)
	}

	// 1. Upload a video by link (or with client.CreateMediaFile).
	kind := rask.MediaKindVideo
	media, err := client.CreateMediaLink(ctx, rask.MediaCreateFromLink{
		Link: "https://example.com/video.mp4",
		Kind: &kind,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 2. Create a project that dubs it into Spanish.
	project, err := client.CreateProject(ctx, rask.ProjectCreate{VideoId: media.Id, DstLang: "es"})
	if err != nil {
		log.Fatal(err)
	}

	// 3. Wait until dubbing is finished.
	for project.Status == nil || *project.Status != rask.ProjectStatusMergingDone {
		time.Sleep(10 * time.Second)
		if project, err = client.GetProject(ctx, project.Id); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("translated video:", *project.TranslatedVideo)
}
```

A complete example that also handles failed projects is in [examples/quickstart](examples/quickstart/main.go).

### Upload a file

`CreateMediaFile` streams the file, so large videos are not loaded into memory:

```go
f, err := os.Open("video.mp4")
if err != nil {
	log.Fatal(err)
}
defer f.Close()

kind := rask.MediaKindVideo
media, err := client.CreateMediaFile(ctx, "video.mp4", f, &rask.CreateMediaFileParams{Kind: &kind})
```

Uploads can take a while; bound them with a context deadline rather than a short `http.Client.Timeout`.

### List projects

```go
name := "demo"
page, err := client.GetProjects(ctx, &rask.GetProjectsParams{Offset: 0, Limit: 10, Name: &name})
```

## Errors

Methods return the decoded response body. If the API answers with an error status, the error is an `*rask.APIError`:

```go
project, err := client.GetProject(ctx, id)
var apiErr *rask.APIError
if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
	// apiErr.Status is the machine-readable status ("not_found"), apiErr.Detail the message.
}
```

Failures to obtain an access token are returned as `*rask.AuthError`.

## Methods

| Method | API operation |
|---|---|
| `GetCredits` | Get remaining minutes and credits |
| `CreateMediaFile` | Upload media by file |
| `CreateMediaLink` | Upload media by link |
| `GetMedia` | Get media |
| `CreateProject` | Create project |
| `GetProject` | Get project |
| `GetProjects` | List projects |
| `PatchProject` | Update project name, speakers or voices |
| `GenerateProject` | Re-translate a project after transcription changes |
| `GetProjectVoices` | List voices available for a project |
| `CreateTranscription` | Create a transcription from segments |
| `CreateTranscriptionSrt` | Create a transcription from .srt files |
| `GetProjectTranscription` | Get project transcription |
| `AddProjectTranscriptionSegments` | Add transcription segments |
| `PatchProjectTranscriptionSegments` | Update transcription segments |
| `DeleteProjectTranscriptionSegment` | Delete a transcription segment |
| `CreateGlossary` / `GetGlossary` / `UpdateGlossary` / `DeleteGlossary` | Manage glossaries |
| `RunCheckFaceTask` | Check a video for faces before lip-sync |
| `RunLipsyncTask` | Run lip-sync |
| `GetLipsyncInfo` | Get lip-sync status and result |

Method names match the [Python SDK](https://github.com/braskai/rask-sdk) and the operation IDs in the [API reference](https://docs.api.rask.ai). The low-level generated client is available from `client.API()` and in the `api` package.

## Development

The SDK is generated from [`api/openapi.yml`](api/openapi.yml), a copy of the spec published at [docs.api.rask.ai](https://docs.api.rask.ai). To update it, copy the new spec over `api/openapi.yml` and run:

```shell
make generate   # oapi-codegen for api/, then internal/cmd/genwrappers for the typed methods
make lint test
```

Never edit `*.gen.go` files by hand.
