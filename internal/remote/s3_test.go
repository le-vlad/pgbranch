package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockS3Client struct {
	putObjectFn     func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	getObjectFn     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	headObjectFn    func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	deleteObjectFn  func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	listObjectsV2Fn func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return m.putObjectFn(ctx, params, optFns...)
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getObjectFn(ctx, params, optFns...)
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m.headObjectFn(ctx, params, optFns...)
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return m.deleteObjectFn(ctx, params, optFns...)
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.listObjectsV2Fn(ctx, params, optFns...)
}

func newTestS3Remote(mock s3API, bucket, prefix string) *S3Remote {
	return &S3Remote{name: "test", remoteType: "s3", bucket: bucket, prefix: prefix, client: mock}
}

func TestS3Remote_ObjectKey(t *testing.T) {
	t.Run("with prefix", func(t *testing.T) {
		r := newTestS3Remote(nil, "bucket", "backups")
		got := r.objectKey("main")
		want := "backups/main.pgbranch"
		if got != want {
			t.Errorf("objectKey() = %q, want %q", got, want)
		}
	})

	t.Run("without prefix", func(t *testing.T) {
		r := newTestS3Remote(nil, "bucket", "")
		got := r.objectKey("main")
		want := "main.pgbranch"
		if got != want {
			t.Errorf("objectKey() = %q, want %q", got, want)
		}
	})
}

func TestS3Remote_Push_Success(t *testing.T) {
	var capturedInput *s3.PutObjectInput
	mock := &mockS3Client{
		putObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			capturedInput = params
			return &s3.PutObjectOutput{}, nil
		},
	}
	r := newTestS3Remote(mock, "my-bucket", "pfx")

	data := []byte("snapshot-bytes")
	err := r.Push(context.Background(), "dev", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Push() unexpected error: %v", err)
	}

	if aws.ToString(capturedInput.Bucket) != "my-bucket" {
		t.Errorf("Bucket = %q, want %q", aws.ToString(capturedInput.Bucket), "my-bucket")
	}
	if aws.ToString(capturedInput.Key) != "pfx/dev.pgbranch" {
		t.Errorf("Key = %q, want %q", aws.ToString(capturedInput.Key), "pfx/dev.pgbranch")
	}
	if aws.ToString(capturedInput.ContentType) != "application/x-pgbranch" {
		t.Errorf("ContentType = %q, want %q", aws.ToString(capturedInput.ContentType), "application/x-pgbranch")
	}
}

func TestS3Remote_Push_Error(t *testing.T) {
	mock := &mockS3Client{
		putObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, fmt.Errorf("access denied")
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	err := r.Push(context.Background(), "dev", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatalf("Push() expected error, got nil")
	}
}

func TestS3Remote_Pull_Success(t *testing.T) {
	payload := []byte("restored-data-here")
	mock := &mockS3Client{
		getObjectFn: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader(payload)),
				ContentLength: aws.Int64(int64(len(payload))),
			}, nil
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	rc, size, err := r.Pull(context.Background(), "dev")
	if err != nil {
		t.Fatalf("Pull() unexpected error: %v", err)
	}
	defer rc.Close()

	if size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", size, len(payload))
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

func TestS3Remote_Pull_Error(t *testing.T) {
	mock := &mockS3Client{
		getObjectFn: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, fmt.Errorf("no such key")
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	_, _, err := r.Pull(context.Background(), "missing")
	if err == nil {
		t.Fatalf("Pull() expected error, got nil")
	}
}

func TestS3Remote_List_SinglePage(t *testing.T) {
	mock := &mockS3Client{
		listObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("dev.pgbranch"), Size: aws.Int64(100)},
					{Key: aws.String("readme.txt")},
					{Key: aws.String("main.pgbranch"), Size: aws.Int64(200)},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	branches, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List() unexpected error: %v", err)
	}
	if len(branches) != 2 {
		t.Fatalf("List() returned %d branches, want 2", len(branches))
	}
	if branches[0].Name != "dev" {
		t.Errorf("branches[0].Name = %q, want %q", branches[0].Name, "dev")
	}
	if branches[0].Size != 100 {
		t.Errorf("branches[0].Size = %d, want 100", branches[0].Size)
	}
	if branches[1].Name != "main" {
		t.Errorf("branches[1].Name = %q, want %q", branches[1].Name, "main")
	}
}

func TestS3Remote_List_Pagination(t *testing.T) {
	callCount := 0
	mock := &mockS3Client{
		listObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			callCount++
			if callCount == 1 {
				return &s3.ListObjectsV2Output{
					Contents: []s3types.Object{
						{Key: aws.String("page1.pgbranch"), Size: aws.Int64(10)},
					},
					IsTruncated:           aws.Bool(true),
					NextContinuationToken: aws.String("token-abc"),
				}, nil
			}
			if aws.ToString(params.ContinuationToken) != "token-abc" {
				t.Errorf("second call ContinuationToken = %q, want %q", aws.ToString(params.ContinuationToken), "token-abc")
			}
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: aws.String("page2.pgbranch"), Size: aws.Int64(20)},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	branches, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List() unexpected error: %v", err)
	}
	if callCount != 2 {
		t.Errorf("ListObjectsV2 called %d times, want 2", callCount)
	}
	if len(branches) != 2 {
		t.Fatalf("List() returned %d branches, want 2", len(branches))
	}
	if branches[0].Name != "page1" {
		t.Errorf("branches[0].Name = %q, want %q", branches[0].Name, "page1")
	}
	if branches[1].Name != "page2" {
		t.Errorf("branches[1].Name = %q, want %q", branches[1].Name, "page2")
	}
}

func TestS3Remote_List_Empty(t *testing.T) {
	mock := &mockS3Client{
		listObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents:    []s3types.Object{},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	branches, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List() unexpected error: %v", err)
	}
	if len(branches) != 0 {
		t.Errorf("List() returned %d branches, want 0", len(branches))
	}
}

func TestS3Remote_List_Error(t *testing.T) {
	mock := &mockS3Client{
		listObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, fmt.Errorf("network error")
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	_, err := r.List(context.Background())
	if err == nil {
		t.Fatalf("List() expected error, got nil")
	}
}

func TestS3Remote_Delete_Success(t *testing.T) {
	var capturedInput *s3.DeleteObjectInput
	mock := &mockS3Client{
		deleteObjectFn: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			capturedInput = params
			return &s3.DeleteObjectOutput{}, nil
		},
	}
	r := newTestS3Remote(mock, "my-bucket", "pfx")

	err := r.Delete(context.Background(), "feature")
	if err != nil {
		t.Fatalf("Delete() unexpected error: %v", err)
	}
	if aws.ToString(capturedInput.Bucket) != "my-bucket" {
		t.Errorf("Bucket = %q, want %q", aws.ToString(capturedInput.Bucket), "my-bucket")
	}
	if aws.ToString(capturedInput.Key) != "pfx/feature.pgbranch" {
		t.Errorf("Key = %q, want %q", aws.ToString(capturedInput.Key), "pfx/feature.pgbranch")
	}
}

func TestS3Remote_Delete_Error(t *testing.T) {
	mock := &mockS3Client{
		deleteObjectFn: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return nil, fmt.Errorf("forbidden")
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	err := r.Delete(context.Background(), "branch")
	if err == nil {
		t.Fatalf("Delete() expected error, got nil")
	}
}

func TestS3Remote_Exists_True(t *testing.T) {
	mock := &mockS3Client{
		headObjectFn: func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, nil
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	exists, err := r.Exists(context.Background(), "dev")
	if err != nil {
		t.Fatalf("Exists() unexpected error: %v", err)
	}
	if !exists {
		t.Errorf("Exists() = false, want true")
	}
}

func TestS3Remote_Exists_False(t *testing.T) {
	mock := &mockS3Client{
		headObjectFn: func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, fmt.Errorf("not found")
		},
	}
	r := newTestS3Remote(mock, "bucket", "")

	exists, err := r.Exists(context.Background(), "nope")
	if err != nil {
		t.Fatalf("Exists() unexpected error: %v", err)
	}
	if exists {
		t.Errorf("Exists() = true, want false")
	}
}

func TestIsArchiveFile(t *testing.T) {
	tests := []struct {
		filename string
		want     bool
	}{
		{"main.pgbranch", true},
		{"feature_foo.pgbranch", true},
		{".pgbranch", false},
		{"readme.txt", false},
		{"", false},
		{"pgbranch", false},
	}
	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			if got := isArchiveFile(tt.filename); got != tt.want {
				t.Errorf("isArchiveFile(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

func TestArchiveNameToBranch(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		{"main.pgbranch", "main"},
		{"feature_bar.pgbranch", "feature_bar"},
		{".pgbranch", ""},
		{"short", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			if got := archiveNameToBranch(tt.filename); got != tt.want {
				t.Errorf("archiveNameToBranch(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}
