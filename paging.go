package rask

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
)

// pages walks a list over next_cursor. A short or empty page does not end it: only a null next_cursor does.
func pages[L any](ctx context.Context, fetch func(context.Context, *string) (*Response[L], error)) iter.Seq2[*Response[L], error] {
	return func(yield func(*Response[L], error) bool) {
		var cursor *string
		for {
			page, err := fetch(ctx, cursor)
			if err != nil {
				yield(page, err)
				return
			}
			if !yield(page, nil) {
				return
			}
			var envelope struct {
				NextCursor *string `json:"next_cursor"`
			}
			if err := json.Unmarshal(page.Body, &envelope); err != nil {
				yield(nil, fmt.Errorf("rask: a page without next_cursor: %w", err))
				return
			}
			if envelope.NextCursor == nil || *envelope.NextCursor == "" {
				return
			}
			if cursor != nil && *cursor == *envelope.NextCursor {
				yield(nil, fmt.Errorf("rask: the server repeated cursor %q", *cursor))
				return
			}
			cursor = envelope.NextCursor
		}
	}
}

// items flattens pages into their items. An item whose raw form matters (unknown fields) is reachable through the
// Pages iterator, whose Response keeps the body.
func items[L, T any](source iter.Seq2[*Response[L], error]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for page, err := range source {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			var envelope struct {
				Data []T `json:"data"`
			}
			if err := json.Unmarshal(page.Body, &envelope); err != nil {
				var zero T
				yield(zero, &DecodeError{Operation: "list", Err: err})
				return
			}
			for _, item := range envelope.Data {
				if !yield(item, nil) {
					return
				}
			}
		}
	}
}
