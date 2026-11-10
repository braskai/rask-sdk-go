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
