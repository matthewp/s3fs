package s3fs

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	_ fs.File     = (*file)(nil)
	_ fs.FileInfo = (*fileInfo)(nil)
)

type file struct {
	s3   *S3FS
	name string
	pos  int
	size int
	stat func() (fs.FileInfo, error)
}

func (f file) Close() error {
	return nil
}

func (f *file) Read(p []byte) (int, error) {
	maxRead := f.size - f.pos

	if len(p) < maxRead {
		maxRead = len(p)
	}

	if maxRead <= 0 {
		return 0, io.EOF
	}

	end := f.pos + maxRead - 1

	out, err := f.s3.cl.GetObject(&s3.GetObjectInput{
		Key:    &f.name,
		Bucket: &f.s3.bucket,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", f.pos, end)),
	})
	if err != nil {
		return -1, err
	}

	defer out.Body.Close()

	var b bytes.Buffer

	len, err := io.CopyN(&b, out.Body, *out.ContentLength)
	if err != nil {
		return -1, err
	}
	copy(p, b.Bytes())

	f.pos = f.pos + int(len)

	return int(len), nil
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	switch offset {
	case io.SeekStart:
		f.pos = 0

	case io.SeekCurrent:
		f.pos = f.pos + whence

	case io.SeekEnd:
		f.pos = f.size - whence - 1
	}

	return int64(f.pos), nil
}

func (f file) Stat() (fs.FileInfo, error) { return f.stat() }

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

func (fi fileInfo) Name() string       { return path.Base(fi.name) }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return fi.modTime }
func (fi fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi fileInfo) Sys() interface{}   { return nil }
