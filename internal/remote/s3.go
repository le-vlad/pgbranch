package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/le-vlad/pgbranch/internal/credentials"
)

func init() {
	Register("s3", NewS3Remote)
	Register("r2", NewS3Remote) // R2 is S3-compatible
}

type S3Remote struct {
	name       string
	remoteType string // "s3" or "r2"
	bucket     string
	prefix     string
	client     *s3.Client
}

func NewS3Remote(cfg *Config) (Remote, error) {
	bucket := cfg.Options["bucket"]
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}

	prefix := cfg.Options["prefix"]
	remoteType := cfg.Type
	if remoteType == "" {
		remoteType = "s3"
	}

	ctx := context.Background()
	awsCfg, err := loadAWSConfig(ctx, cfg.Options, remoteType)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	return &S3Remote{
		name:       cfg.Name,
		remoteType: remoteType,
		bucket:     bucket,
		prefix:     prefix,
		client:     client,
	}, nil
}

func loadAWSConfig(ctx context.Context, options map[string]string, remoteType string) (aws.Config, error) {
	var optFns []func(*config.LoadOptions) error

	creds, err := credentials.GetCredentials(options, remoteType)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to get credentials: %w", err)
	}

	if creds.AccessKey != "" && creds.SecretKey != "" {
		optFns = append(optFns, config.WithCredentialsProvider(
			awscreds.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
		))
	}

	region := options["region"]
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		if remoteType == "r2" {
			region = "auto"
		} else {
			region = "us-east-1"
		}
	}
	optFns = append(optFns, config.WithRegion(region))

	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return aws.Config{}, err
	}

	endpoint := options["endpoint"]
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	return cfg, nil
}

func (r *S3Remote) Name() string {
	return r.name
}

func (r *S3Remote) Type() string {
	return r.remoteType
}

func (r *S3Remote) objectKey(branchName string) string {
	filename := ArchiveFileName(branchName)
	if r.prefix != "" {
		return path.Join(r.prefix, filename)
	}
	return filename
}

func (r *S3Remote) Push(ctx context.Context, branchName string, reader io.Reader, size int64) error {
	key := r.objectKey(branchName)

	// TODO: For large files, use multipart upload
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read archive data: %w", err)
	}

	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(r.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/x-pgbranch"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

func (r *S3Remote) Pull(ctx context.Context, branchName string) (io.ReadCloser, int64, error) {
	key := r.objectKey(branchName)

	result, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to download from S3: %w", err)
	}

	size := int64(0)
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	return result.Body, size, nil
}

func (r *S3Remote) List(ctx context.Context) ([]RemoteBranch, error) {
	prefix := r.prefix
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var branches []RemoteBranch

	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			filename := path.Base(*obj.Key)
			if !isArchiveFile(filename) {
				continue
			}

			branchName := archiveNameToBranch(filename)
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}

			var modTime = obj.LastModified

			branch := RemoteBranch{
				Name: branchName,
				Size: size,
			}
			if modTime != nil {
				branch.ModTime = *modTime
			}

			branches = append(branches, branch)
		}
	}

	return branches, nil
}

func (r *S3Remote) Delete(ctx context.Context, branchName string) error {
	key := r.objectKey(branchName)

	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}

	return nil
}

func (r *S3Remote) Exists(ctx context.Context, branchName string) (bool, error) {
	key := r.objectKey(branchName)

	_, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a "not found" error
		// The AWS SDK v2 doesn't have a nice way to check this
		return false, nil
	}

	return true, nil
}

func isArchiveFile(filename string) bool {
	return len(filename) > 9 && filename[len(filename)-9:] == ".pgbranch"
}

func archiveNameToBranch(filename string) string {
	if len(filename) <= 9 {
		return ""
	}
	return filename[:len(filename)-9]
}
