// Package httpreaderat implements io.ReaderAt that makes HTTP Range Requests.
//
// It can be used for example with "archive/zip" package in Go standard
// library. Together they can be used to access remote (HTTP accessible)
// ZIP archives without needing to download the whole archive file.
//
// HTTP Range Requests (see RFC 7233) are used to retrieve the requested
// byte range.
package httprange

// copy from https://raw.githubusercontent.com/snabb/httpreaderat/master/httpreaderat.go

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// HTTPReaderAt is io.ReaderAt implementation that makes HTTP Range Requests.
// New instances must be created with the New() function.
// It is safe for concurrent use.
type HTTPReaderAt struct {
	client Requester
	req    *http.Request
	meta   Meta
}

var _ io.ReaderAt = (*HTTPReaderAt)(nil)

// ErrValidationFailed error is returned if the file changed under
// our feet.
var ErrValidationFailed = errors.New("validation failed")

// ErrNoRange error is returned if the server does not support range
// requests and there is no Store defined for buffering the file.
var ErrNoRange = errors.New("server does not support range requests")

// New creates a new HTTPReaderAt. If nil is passed as http.Client, then
// http.DefaultClient is used. The supplied http.Request is used as a
// prototype for requests. It is copied before making the actual request.
// It is an error to specify any other HTTP method than "GET".
func New(client Requester, req *http.Request) (ra *HTTPReaderAt, err error) {
	if (client == nil) || (req == nil) {
		return nil, errors.New("invalid args")
	}
	if req.Method != http.MethodGet {
		return nil, errors.New("invalid HTTP method, must be GET")
	}
	ra = &HTTPReaderAt{
		client: client,
		req:    req,
	}
	// Make 1 byte Range Request to see if they are supported or not.
	// Also stores the file metadata for later use.
	if err = ra.init(); err != nil {
		return nil, err
	}
	return ra, nil
}

// ContentType returns "Content-Type" header contents.
func (ra *HTTPReaderAt) ContentType() string {
	return ra.meta.contentType
}

// LastModified returns "Last-Modified" header contents.
func (ra *HTTPReaderAt) LastModified() string {
	return ra.meta.lastModified
}

// Size returns the size of the file.
func (ra *HTTPReaderAt) Size() int64 {
	return ra.meta.size
}

func (ra *HTTPReaderAt) init() error {
	var req = ra.cloneRequest()
	// Warning: not reset the http method to head, req.Method = http.MethodHead
	// if reset, the signature maybe invalid
	req.Header.Set("Range", "bytes=0-0")
	var resp, err = ra.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request error %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpect http request : %s, expect %v", resp.Status, http.StatusPartialContent)
	}
	if ra.meta, err = getMeta(resp); err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

// ReadAt reads len(b) bytes from the remote file starting at byte offset
// off. It returns the number of bytes read and the error, if any. ReadAt
// always returns a non-nil error when n < len(b). At end of file, that
// error is io.EOF. It is safe for concurrent use.
//
// It tries to notice if the file changes by tracking the size as well as
// Content-Type, Last-Modified and ETag headers between consecutive ReadAt
// calls. In case any change is detected, ErrValidationFailed is returned.
func (ra *HTTPReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var req = ra.cloneRequest()

	var reqFirst = off
	var reqLast = off + int64(len(p)) - 1

	var returnErr error
	if ra.meta.size != -1 && reqLast > ra.meta.size-1 {
		// Clamp down the requested range because some servers return
		// "416 Range Not Satisfiable" if trying to read past the end of the file.
		reqLast = ra.meta.size - 1
		returnErr = io.EOF
		if reqLast < reqFirst {
			return 0, io.EOF
		}
		p = p[:reqLast-reqFirst+1]
	}

	var reqRange = fmt.Sprintf(HttpHeaderRangeFormat, reqFirst, reqLast)
	req.Header.Set("Range", reqRange)

	var resp, err = ra.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http request error %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("unexpect http request : %s, expect %v %w", resp.Status, http.StatusPartialContent, ErrNoRange)
	}

	var meta Meta
	if meta, err = getMeta(resp); err != nil {
		return 0, err
	}
	// check
	if ra.meta.size != meta.size ||
		ra.meta.lastModified != meta.lastModified ||
		ra.meta.etag != meta.etag {
		return 0, ErrValidationFailed
	}
	if meta.start != reqFirst || meta.end > reqLast {
		return 0, fmt.Errorf(
			"received different range than requested (req=%d-%d, resp=%d-%d)",
			reqFirst, reqLast, meta.start, meta.end)
	}
	if resp.ContentLength != meta.end-meta.start+1 {
		return 0, errors.New("content-length mismatch in http response")
	}
	var n int
	n, err = io.ReadFull(resp.Body, p)

	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if (err == nil || err == io.EOF) && int64(n) != resp.ContentLength {
		// XXX body size was different from the ContentLength
		// header? should we do something about it? return error?
		fmt.Printf("bodySize %v != header ContentLength %v", n, resp.ContentLength)
	}
	if err == nil && returnErr != nil {
		err = returnErr
	}

	// you can debug print how many bytes download
	// fmt.Printf("read contentRange %v length %v\n", contentRange, n)
	return n, err
}

func (ra *HTTPReaderAt) cloneRequest() *http.Request {
	out := *ra.req
	out.Body = nil
	out.ContentLength = 0
	out.Header = cloneHeader(ra.req.Header)
	return &out
}
