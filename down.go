package httprange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"golang.org/x/sync/errgroup"
)

// Do 下载支持 Range 下载的文件
func Do(ctx context.Context, clt Requester, url string) ([]byte, error) {
	var req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	var preRead *HTTPReaderAt
	if preRead, err = New(clt, req); err != nil {
		return nil, err
	}
	var totalSize = preRead.Size()

	var concurrentCount = 48
	var buf = make([]byte, totalSize, totalSize)
	var taskList = makeTask(totalSize, buf)
	var taskCh = make(chan Task, len(taskList))
	for _, task := range taskList {
		taskCh <- task
	}
	close(taskCh)

	var group, _ = errgroup.WithContext(ctx)

	for i := 0; i < concurrentCount; i++ {
		group.Go(func() error {
			for task := range taskCh {
				if err := readChunk(clt, req, task); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err = group.Wait(); err != nil {
		return nil, err
	}
	return buf, nil
}

func DoWithCheck(ctx context.Context, clt Requester, url, sha256Sum string) ([]byte, error) {
	var result, err = Do(ctx, clt, url)
	if err != nil {
		return nil, err
	}
	if b, _ := equal(result, sha256Sum); !b {
		return nil, fmt.Errorf("sha256 checksum not equal with %v", sha256Sum)
	}
	return result, nil
}

func equal(content []byte, checksum string) (bool, error) {
	expect, err := hex.DecodeString(checksum)
	if err != nil {
		return false, err
	}
	v1 := sha256.Sum256(content)
	return hmac.Equal(v1[:], expect), nil
}

func readChunk(clt Requester, req *http.Request, task Task) error {
	var chunkReader, err = New(clt, req)
	if err != nil {
		return err
	}
	var n int
	if n, err = chunkReader.ReadAt(task.Content, task.Offset); err != nil {
		return err
	}
	if n != len(task.Content) {
		return fmt.Errorf("download size %v not equal with expect size %v, for task(offset %v size %v)",
			n, len(task.Content), task.Offset, len(task.Content))
	}
	return nil
}

func makeTask(totalSize int64, buf []byte) []Task {
	var chunkSize int64 = 64 * 1024
	var taskList []Task

	var taskCount = totalSize / chunkSize
	taskList = make([]Task, taskCount, taskCount+1)
	var offset int64 = 0
	for i := int64(0); i < taskCount; i++ {
		taskList[i].Offset = offset
		taskList[i].Content = buf[offset : offset+chunkSize]
		offset += chunkSize
	}
	if offset < totalSize {
		taskList = append(taskList, Task{
			Offset:  offset,
			Content: buf[offset:totalSize],
		})
	}
	return taskList
}

type Task struct {
	Offset  int64
	Content []byte
}
