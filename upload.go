package rask

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"path/filepath"
)

// CreateMediaFile uploads a video, audio or image file and returns the created
// media. The file is streamed, so large files are not buffered in memory.
// filename is sent as the uploaded file name and determines its content type.
//
// Corresponds with POST /api/library/v1/media (the `createMediaFile` operationId).
func (c *Client) CreateMediaFile(ctx context.Context, filename string, file io.Reader, params *CreateMediaFileParams, reqEditors ...RequestEditorFn) (*MediaGet, error) {
	if file == nil {
		return nil, fmt.Errorf("rask: file is required")
	}
	body, contentType := streamMultipart(func(w *multipart.Writer) error {
		return writeFile(w, "data", filename, file)
	})
	defer body.Close()
	return unwrap(c.api.CreateMediaFileWithBodyWithResponse(ctx, params, contentType, body, reqEditors...))
}

// CreateTranscriptionSrt creates a transcription from .srt subtitles. At least
// one of src (source language subtitles) or dst (translated subtitles) must be
// non-nil; set params.SrcLang / params.DstLang for the files provided.
//
// Corresponds with POST /v2/transcriptions/srt (the `createTranscriptionSrt` operationId).
func (c *Client) CreateTranscriptionSrt(ctx context.Context, params *CreateTranscriptionSrtParams, src, dst io.Reader, reqEditors ...RequestEditorFn) (*TranscriptionId, error) {
	if src == nil && dst == nil {
		return nil, fmt.Errorf("rask: at least one of src or dst .srt files is required")
	}
	body, contentType := streamMultipart(func(w *multipart.Writer) error {
		if src != nil {
			if err := writeFile(w, "src", "src.srt", src); err != nil {
				return err
			}
		}
		if dst != nil {
			return writeFile(w, "dst", "dst.srt", dst)
		}
		return nil
	})
	defer body.Close()
	return unwrap(c.api.CreateTranscriptionSrtWithBodyWithResponse(ctx, params, contentType, body, reqEditors...))
}

// streamMultipart writes a multipart body through a pipe so it is produced
// while the request is being sent. Callers must close the returned reader:
// if the request fails before the body is read, closing it stops the writer.
func streamMultipart(write func(*multipart.Writer) error) (io.ReadCloser, string) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		err := write(mw)
		if err == nil {
			err = mw.Close()
		}
		pw.CloseWithError(err)
	}()
	return pr, mw.FormDataContentType()
}

func writeFile(w *multipart.Writer, field, filename string, r io.Reader) error {
	contentType := mime.TypeByExtension(filepath.Ext(filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, filepath.Base(filename)))
	h.Set("Content-Type", contentType)
	part, err := w.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = io.Copy(part, r)
	return err
}
