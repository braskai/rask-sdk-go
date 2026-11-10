// Command smoke checks a deployed API with real calls, one line per call, without printing a secret. It needs
// RASK_API_KEY and RASK_BASE_URL; it refuses to fall back to production, because it creates and deletes a folder.
//
//	RASK_API_KEY=... RASK_BASE_URL=https://api.example.com/v3 go run ./examples/smoke
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/braskai/rask-sdk-go/v3"
	"github.com/braskai/rask-sdk-go/v3/api"
)

func main() {
	if os.Getenv("RASK_BASE_URL") == "" || os.Getenv("RASK_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "smoke: set RASK_API_KEY and RASK_BASE_URL")
		os.Exit(2)
	}
	client, err := rask.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if !run(client) {
		os.Exit(1)
	}
}

func run(client *rask.Client) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := smoke{ok: true}

	me, err := client.Account.GetMe(ctx)
	s.report("getMe", metaOf(me), err)

	languages, err := client.Catalogs.ListLanguages(ctx, nil)
	s.report("listLanguages", metaOf(languages), err)

	voices, err := client.Voices.ListVoices(ctx, api.ListVoicesParams{TargetLanguage: "ru-ru"})
	s.report("listVoices target_language=ru-ru", metaOf(voices), err)

	s.projects(ctx, client)

	balance, err := client.Account.GetBalance(ctx)
	s.report("getBalance", metaOf(balance), err)

	plan, err := client.Account.GetPlan(ctx)
	s.report("getPlan", metaOf(plan), err)

	s.folder(ctx, client)
	return s.ok
}

func (s *smoke) projects(ctx context.Context, client *rask.Client) {
	params := api.ListProjectsParams{Limit: rask.Ptr(api.Limit(2))}
	page := 0
	for response, err := range client.Projects.ListProjectsPages(ctx, &params) {
		page++
		s.report(fmt.Sprintf("listProjects page=%d", page), metaOf(response), err)
		if err != nil || page == 2 {
			return
		}
	}
	if page < 2 {
		fmt.Printf("listProjects page=2 skipped: page 1 was the last\n")
	}
}

func (s *smoke) folder(ctx context.Context, client *rask.Client) {
	name := fmt.Sprintf("sdk-smoke-go-%d", time.Now().Unix())
	created, err := client.Folders.CreateFolder(ctx, api.CreateFolderRequest{Name: name})
	s.report("createFolder", metaOf(created), err)
	if err != nil {
		return
	}
	folder, err := client.Folders.GetFolder(ctx, created.Data.Id)
	s.report("getFolder", metaOf(folder), err)
	deleted, err := client.Folders.DeleteFolder(ctx, created.Data.Id)
	s.report("deleteFolder", metaOf(deleted), err)
}

type smoke struct{ ok bool }

func metaOf[T any](response *rask.Response[T]) *rask.Meta {
	if response == nil {
		return nil
	}
	return &response.Meta
}

// report prints the status, the request id and, on failure, the problem code. Those carry no secret, unlike a raw body
// or URL might.
func (s *smoke) report(call string, meta *rask.Meta, err error) {
	var problem *rask.RaskError
	switch {
	case errors.As(err, &problem):
		s.ok = false
		fmt.Printf("%s status=%d code=%s request_id=%s FAILED\n", call, problem.Status, problem.Code, problem.RequestID)
	case err != nil:
		s.ok = false
		fmt.Printf("%s status=0 error=%q FAILED\n", call, err.Error())
	default:
		fmt.Printf("%s status=%d request_id=%s\n", call, meta.StatusCode, meta.RequestID())
	}
}
