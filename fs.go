// Package s3fs provides a S3 implementation for Go1.16 filesystem interface.
package s3fs

import (
	"bytes"
	"errors"
	"io/fs"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	_ fs.FS        = (*S3FS)(nil)
	_ fs.StatFS    = (*S3FS)(nil)
	_ fs.ReadDirFS = (*S3FS)(nil)
)

var errNotDir = errors.New("not a dir")

// S3FS is a S3 filesystem implementation.
//
// S3 has a flat structure instead of a hierarchy. S3FS simulates directories
// by using prefixes and delims ("/"). Because directories are simulated, ModTime
// is always a default Time value (IsZero returns true).
type S3FS struct {
	cl     s3iface.S3API
	bucket string
}

// New returns a new filesystem that works on the specified bucket.
func New(cl s3iface.S3API, bucket string) *S3FS {
	return &S3FS{
		cl:     cl,
		bucket: bucket,
	}
}

// Open implements fs.FS.
func (f *S3FS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	if name == "." {
		return openDir(f.cl, f.bucket, name)
	}

	out, err := f.cl.GetObject(&s3.GetObjectInput{
		Key:    &name,
		Bucket: &f.bucket,
		Range:  aws.String("bytes=0-1"),
	})
	if err != nil {
		if isNotFoundErr(err) {
			switch d, err := openDir(f.cl, f.bucket, name); {
			case err == nil:
				return d, nil
			case !isNotFoundErr(err) && !errors.Is(err, errNotDir) && !errors.Is(err, fs.ErrNotExist):
				return nil, err
			}

			return nil, &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}

		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  err,
		}
	}
	defer out.Body.Close()

	statFunc := func() (fs.FileInfo, error) {
		return stat(f.cl, f.bucket, name)
	}

	info, err := statFunc()
	if err != nil {
		return nil, err
	}

	return &file{
		s3:   f,
		name: name,
		stat: statFunc,
		pos:  0,
		size: int(info.Size()),
	}, nil
}

// Stat implements fs.StatFS.
func (f *S3FS) Stat(name string) (fs.FileInfo, error) {
	fi, err := stat(f.cl, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	return fi, nil
}

// ReadDir implements fs.ReadDirFS.
func (f *S3FS) ReadDir(name string) ([]fs.DirEntry, error) {
	d, err := openDir(f.cl, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  err,
		}
	}
	return d.ReadDir(-1)
}

func (f *S3FS) WriteFile(filename string, data []byte, perm fs.FileMode) error {
	mimeType := http.DetectContentType(data)

	_, err := f.cl.PutObject(&s3.PutObjectInput{
		Key:    &filename,
		Bucket: &f.bucket,
		Body:   bytes.NewReader(data),
		Metadata: map[string]*string{
			"Content-Type": &mimeType,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (f *S3FS) Rename(oldpath, newpath string) error {
	if _, err := f.cl.CopyObject(&s3.CopyObjectInput{
		Bucket:     &f.bucket,
		Key:        aws.String(newpath),
		CopySource: aws.String(oldpath),
	}); err != nil {
		return err
	}

	if _, err := f.cl.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &f.bucket,
		Key:    aws.String(oldpath),
	}); err != nil {
		return err
	}

	return nil
}

func (f *S3FS) MkdirAll(path string, perm os.FileMode) error {
	return nil
}

func stat(s3cl s3iface.S3API, bucket, name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}

	if name == "." {
		return &dir{
			s3cl:   s3cl,
			bucket: bucket,
			fileInfo: fileInfo{
				name: ".",
				mode: fs.ModeDir,
			},
		}, nil
	}

	out, err := s3cl.ListObjects(&s3.ListObjectsInput{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
		Prefix:    &name,
		MaxKeys:   aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}

	if len(out.CommonPrefixes) > 0 &&
		out.CommonPrefixes[0] != nil &&
		out.CommonPrefixes[0].Prefix != nil &&
		*out.CommonPrefixes[0].Prefix == name+"/" {
		return &dir{
			s3cl:   s3cl,
			bucket: bucket,
			fileInfo: fileInfo{
				name: name,
				mode: fs.ModeDir,
			},
		}, nil
	}

	if len(out.Contents) != 0 &&
		out.Contents[0] != nil &&
		out.Contents[0].Key != nil &&
		*out.Contents[0].Key == name {
		return &fileInfo{
			name:    name,
			size:    derefInt64(out.Contents[0].Size),
			mode:    0,
			modTime: derefTime(out.Contents[0].LastModified),
		}, nil
	}

	return nil, fs.ErrNotExist
}

func openDir(s3cl s3iface.S3API, bucket, name string) (fs.ReadDirFile, error) {
	fi, err := stat(s3cl, bucket, name)
	if err != nil {
		return nil, err
	}

	if d, ok := fi.(fs.ReadDirFile); ok {
		return d, nil
	}
	return nil, errNotDir
}

var notFoundCodes = map[string]struct{}{
	s3.ErrCodeNoSuchKey: {},
	"NotFound":          {}, // localstack
}

func isNotFoundErr(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		_, ok := notFoundCodes[aerr.Code()]
		return ok
	}
	return false
}
