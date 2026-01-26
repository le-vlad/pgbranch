package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	Register("gcs", NewGCSRemote)
}

type GCSRemote struct {
	name   string
	bucket string
	prefix string
	client *storage.Client
}

func NewGCSRemote(cfg *Config) (Remote, error) {
	bucket := cfg.Options["bucket"]
	if bucket == "" {
		return nil, fmt.Errorf("GCS bucket is required")
	}

	prefix := cfg.Options["prefix"]

	ctx := context.Background()
	client, err := createGCSClient(ctx, cfg.Options)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSRemote{
		name:   cfg.Name,
		bucket: bucket,
		prefix: prefix,
		client: client,
	}, nil
}

func createGCSClient(ctx context.Context, options map[string]string) (*storage.Client, error) {
	var opts []option.ClientOption

	serviceAccountPath := options["service_account"]
	if serviceAccountPath == "" {
		serviceAccountPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}

	if serviceAccountPath != "" {
		opts = append(opts, option.WithCredentialsFile(serviceAccountPath))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (r *GCSRemote) Name() string {
	return r.name
}

func (r *GCSRemote) Type() string {
	return "gcs"
}

func (r *GCSRemote) objectKey(branchName string) string {
	filename := ArchiveFileName(branchName)
	if r.prefix != "" {
		return path.Join(r.prefix, filename)
	}
	return filename
}

func (r *GCSRemote) Push(ctx context.Context, branchName string, reader io.Reader, size int64) error {
	key := r.objectKey(branchName)

	obj := r.client.Bucket(r.bucket).Object(key)
	w := obj.NewWriter(ctx)
	w.ContentType = "application/x-pgbranch"

	if _, err := io.Copy(w, reader); err != nil {
		w.Close()
		return fmt.Errorf("failed to upload to GCS: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finalize GCS upload: %w", err)
	}

	return nil
}

func (r *GCSRemote) Pull(ctx context.Context, branchName string) (io.ReadCloser, int64, error) {
	key := r.objectKey(branchName)

	obj := r.client.Bucket(r.bucket).Object(key)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get GCS object attributes: %w", err)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to download from GCS: %w", err)
	}

	return reader, attrs.Size, nil
}

func (r *GCSRemote) List(ctx context.Context) ([]RemoteBranch, error) {
	prefix := r.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var branches []RemoteBranch

	it := r.client.Bucket(r.bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects: %w", err)
		}

		filename := path.Base(attrs.Name)
		if !isArchiveFile(filename) {
			continue
		}

		branchName := archiveNameToBranch(filename)

		branches = append(branches, RemoteBranch{
			Name:    branchName,
			Size:    attrs.Size,
			ModTime: attrs.Updated,
		})
	}

	return branches, nil
}

func (r *GCSRemote) Delete(ctx context.Context, branchName string) error {
	key := r.objectKey(branchName)

	obj := r.client.Bucket(r.bucket).Object(key)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete from GCS: %w", err)
	}

	return nil
}

func (r *GCSRemote) Exists(ctx context.Context, branchName string) (bool, error) {
	key := r.objectKey(branchName)

	obj := r.client.Bucket(r.bucket).Object(key)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check GCS object existence: %w", err)
	}

	return true, nil
}
