// Command quickstart dubs a video from a public link and prints the result.
//
//	RASK_CLIENT_ID=... RASK_CLIENT_SECRET=... go run ./examples/quickstart https://example.com/video.mp4 es
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	rask "github.com/braskai/rask-sdk-go"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: quickstart <video-url> <destination-language>")
	}
	videoURL, dstLang := os.Args[1], os.Args[2]
	ctx := context.Background()

	client, err := rask.NewClient(os.Getenv("RASK_CLIENT_ID"), os.Getenv("RASK_CLIENT_SECRET"))
	if err != nil {
		log.Fatal(err)
	}

	credits, err := client.GetCredits(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("credits: %+v\n", credits)

	media, err := client.CreateMediaLink(ctx, rask.MediaCreateFromLink{Link: videoURL, Kind: rask.Ptr(rask.MediaKindVideo)})
	if err != nil {
		log.Fatal(err)
	}

	project, err := client.CreateProject(ctx, rask.ProjectCreate{VideoId: media.Id, DstLang: dstLang})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("project:", project.Id)

	for {
		project, err = client.GetProject(ctx, project.Id)
		if err != nil {
			log.Fatal(err)
		}
		status := rask.ProjectStatus("")
		if project.Status != nil {
			status = *project.Status
		}
		fmt.Println("status:", status)
		if status == rask.ProjectStatusMergingDone {
			break
		}
		if failed(status) {
			log.Fatalf("project failed with status %s", status)
		}
		time.Sleep(10 * time.Second)
	}
	if project.TranslatedVideo != nil {
		fmt.Println("translated video:", *project.TranslatedVideo)
	}
}

func failed(s rask.ProjectStatus) bool {
	switch s {
	case rask.ProjectStatusForbiddenLink, rask.ProjectStatusNoAudio, rask.ProjectStatusNoWords:
		return true
	}
	return strings.HasSuffix(string(s), "failed")
}
