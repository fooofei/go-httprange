package httprange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"time"

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
	var taskList = makeMemoryTask(totalSize, buf)
	var taskCh = make(chan memoryTaskType, len(taskList))
	for _, task := range taskList {
		taskCh <- task
	}
	close(taskCh)

	var group, errCtx = errgroup.WithContext(ctx)

	for i := 0; i < concurrentCount; i++ {
		group.Go(func() error {
			for task := range taskCh {
				select {
				case <-errCtx.Done():
					return nil
				default:
				}
				if err := readChunk(ctx, preRead, task); err != nil {
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

func DoToFile(ctx context.Context, clt Requester, url, filePath string) error {
	var req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	var preRead *HTTPReaderAt
	if preRead, err = New(clt, req); err != nil {
		return err
	}
	var totalSize = preRead.Size()
	var taskCh = makeFileTask(totalSize)
	var chunkResultCh = make(chan memoryTaskType, len(taskCh))

	var file *os.File
	if file, err = os.Create(filePath); err != nil {
		return err
	}
	var concurrentCount = 48

	var group, errCtx = errgroup.WithContext(ctx)

	for i := 0; i < concurrentCount; i++ {
		group.Go(func() error {
			for task := range taskCh {
				select {
				case <-errCtx.Done():
					return nil
				default:
				}
				var mt = memoryTaskType{
					Offset:  task.Offset,
					Content: make([]byte, task.Size),
				}

				if err := readChunk(ctx, preRead, mt); err != nil {
					return err
				}
				select {
				case <-errCtx.Done():
					return nil
				case chunkResultCh <- mt:
				}
			}
			return nil
		})
	}

	// single routine for write file
	group.Go(func() error {
		var totalWrite int64
		for {
			select {
			case <-errCtx.Done():
				return nil
			case chunk := <-chunkResultCh:
				if _, err := file.WriteAt(chunk.Content, chunk.Offset); err != nil {
					return err
				}
				totalWrite += int64(len(chunk.Content))
				if totalWrite == totalSize {
					return nil
				}
			}
		}
	})
	return group.Wait()
}

func makeFileTask(totalSize int64) <-chan fileTaskType {
	const chunkSize int64 = 64 * 1024
	var taskCount = totalSize / chunkSize
	var taskList = make([]fileTaskType, taskCount)
	var offset int64 = 0
	for i := int64(0); i < taskCount; i++ {
		taskList[i].Offset = offset
		taskList[i].Size = chunkSize
		offset += chunkSize
	}
	if offset < totalSize {
		taskList = append(taskList, fileTaskType{
			Offset: offset,
			Size:   totalSize - offset,
		})
	}
	var taskCh = make(chan fileTaskType, len(taskList))
	for _, e := range taskList {
		taskCh <- e
	}
	close(taskCh)
	return taskCh
}

func equal(content []byte, checksum string) (bool, error) {
	expect, err := hex.DecodeString(checksum)
	if err != nil {
		return false, err
	}
	v1 := sha256.Sum256(content)
	return hmac.Equal(v1[:], expect), nil
}

func readChunk(ctx context.Context, preReader *HTTPReaderAt, task memoryTaskType) error {
	// a chunk should done in 1 minutes
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var chunkReader = preReader.Clone(ctx)
	var n, err = chunkReader.ReadAt(task.Content, task.Offset)
	if err != nil {
		return err
	}
	if n != len(task.Content) {
		return fmt.Errorf("download size %v not equal with expect size %v, for task(offset %v size %v)",
			n, len(task.Content), task.Offset, len(task.Content))
	}
	return nil
}

func makeMemoryTask(totalSize int64, buf []byte) []memoryTaskType {
	var chunkSize int64 = 64 * 1024
	var taskList []memoryTaskType

	var taskCount = totalSize / chunkSize
	taskList = make([]memoryTaskType, taskCount, taskCount+1)
	var offset int64 = 0
	for i := int64(0); i < taskCount; i++ {
		taskList[i].Offset = offset
		taskList[i].Content = buf[offset : offset+chunkSize]
		offset += chunkSize
	}
	if offset < totalSize {
		taskList = append(taskList, memoryTaskType{
			Offset:  offset,
			Content: buf[offset:totalSize],
		})
	}
	return taskList
}

type memoryTaskType struct {
	Offset  int64
	Content []byte
}

type fileTaskType struct {
	Offset int64
	Size   int64
}
