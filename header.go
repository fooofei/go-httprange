package httprange

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

var errParse = errors.New("content-range parse error")

// parseContentRange will parse http header Content-Range
// Content-Range: bytes 42-1233/1234
// Content-Range: bytes 42-1233/*
// Content-Range: bytes */1234
// simple parse is better than regex:
// regexp.MustCompile(`bytes ([0-9]+)-([0-9]+)/([0-9]+|\\*)`)
// regex not supprt format of bytes */1234
func parseContentRange(str string) (first, last, length int64, err error) {
	first, last, length = -1, -1, -1

	var strList = strings.Split(str, " ")
	if len(strList) != 2 || strList[0] != "bytes" {
		return -1, -1, -1, errParse
	}
	strList = strings.Split(strList[1], "/")
	if len(strList) != 2 {
		return -1, -1, -1, errParse
	}
	if strList[1] != "*" {
		length, err = strconv.ParseInt(strList[1], 10, 64)
		if err != nil {
			return -1, -1, -1, errParse
		}
	}
	if strList[0] != "*" {
		strList = strings.Split(strList[0], "-")
		if len(strList) != 2 {
			return -1, -1, -1, errParse
		}
		first, err = strconv.ParseInt(strList[0], 10, 64)
		if err != nil {
			return -1, -1, -1, errParse
		}
		last, err = strconv.ParseInt(strList[1], 10, 64)
		if err != nil {
			return -1, -1, -1, errParse
		}
	}
	if first == -1 && last == -1 && length == -1 {
		return -1, -1, -1, errParse
	}
	return first, last, length, nil
}

func cloneHeader(h http.Header) http.Header {
	h2 := make(http.Header, len(h))
	for k, vv := range h {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2
	}
	return h2
}

type Meta struct {
	start        int64
	end          int64
	size         int64
	lastModified string
	etag         string
	contentType  string
}

func getMeta(resp *http.Response) (Meta, error) {
	var meta = Meta{
		start:        -1,
		end:          -1,
		size:         0,
		lastModified: resp.Header.Get("Last-Modified"),
		etag:         resp.Header.Get("ETag"),
		contentType:  resp.Header.Get(HttpHeaderContentType),
	}
	switch resp.StatusCode {
	case http.StatusOK:
		meta.size = resp.ContentLength
	case http.StatusPartialContent:
		contentRange := resp.Header.Get(HttpHeaderContentRange)
		if contentRange != "" {
			var err error
			if meta.start, meta.end, meta.size, err = parseContentRange(contentRange); err != nil {
				return Meta{}, err
			}
		}
	}
	return meta, nil
}
