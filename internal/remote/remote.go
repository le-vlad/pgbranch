package remote

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// RemoteBranch represents a branch stored on a remote
type RemoteBranch struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// Remote defines the interface for remote storage backends
type Remote interface {
	// Name returns the name of this remote
	Name() string

	// Type returns the type of this remote (fs, s3, gcs, etc.)
	Type() string

	// Push uploads a snapshot archive to the remote
	// The reader should contain the archive data
	Push(ctx context.Context, branchName string, r io.Reader, size int64) error

	// Pull downloads a snapshot archive from the remote
	// Returns a reader for the archive data
	Pull(ctx context.Context, branchName string) (io.ReadCloser, int64, error)

	// List returns all branches available on the remote
	List(ctx context.Context) ([]RemoteBranch, error)

	// Delete removes a branch from the remote
	Delete(ctx context.Context, branchName string) error

	// Exists checks if a branch exists on the remote
	Exists(ctx context.Context, branchName string) (bool, error)
}

type Config struct {
	Name string `json:"name"`

	Type string `json:"type"`

	URL string `json:"url"`

	Options map[string]string `json:"options,omitempty"`
}

func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("remote name is required")
	}
	if c.Type == "" {
		return fmt.Errorf("remote type is required")
	}
	if c.URL == "" {
		return fmt.Errorf("remote URL is required")
	}

	switch c.Type {
	case "fs", "s3", "r2", "gcs":
	default:
		return fmt.Errorf("unsupported remote type: %s", c.Type)
	}

	return nil
}

// ParseURL parses a remote URL and returns a Config
// Supported URL formats:
//   - /path/to/dir or file:///path/to/dir -> filesystem
//   - s3://bucket/prefix -> S3/MinIO
//   - r2://account-id/bucket/prefix -> Cloudflare R2
//   - gs://bucket/prefix -> Google Cloud Storage
func ParseURL(name, rawURL string) (*Config, error) {
	if strings.HasPrefix(rawURL, "/") {
		return &Config{
			Name: name,
			Type: "fs",
			URL:  rawURL,
		}, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	cfg := &Config{
		Name:    name,
		URL:     rawURL,
		Options: make(map[string]string),
	}

	switch u.Scheme {
	case "file", "":
		cfg.Type = "fs"
		cfg.URL = u.Path
	case "s3":
		cfg.Type = "s3"
		cfg.Options["bucket"] = u.Host
		cfg.Options["prefix"] = strings.TrimPrefix(u.Path, "/")
	case "r2":
		// r2://account-id/bucket/prefix
		// Host is the account ID, path contains bucket and optional prefix
		cfg.Type = "r2"
		accountID := u.Host
		if accountID == "" {
			return nil, fmt.Errorf("R2 URL requires account ID: r2://<account-id>/<bucket>[/<prefix>]")
		}
		cfg.Options["account_id"] = accountID
		cfg.Options["endpoint"] = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)

		// Parse path: /bucket/prefix or /bucket
		path := strings.TrimPrefix(u.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) == 0 || parts[0] == "" {
			return nil, fmt.Errorf("R2 URL requires bucket name: r2://<account-id>/<bucket>[/<prefix>]")
		}
		cfg.Options["bucket"] = parts[0]
		if len(parts) > 1 {
			cfg.Options["prefix"] = parts[1]
		}
	case "gs":
		cfg.Type = "gcs"
		cfg.Options["bucket"] = u.Host
		cfg.Options["prefix"] = strings.TrimPrefix(u.Path, "/")
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	return cfg, nil
}

var registry = make(map[string]func(*Config) (Remote, error))

func Register(remoteType string, factory func(*Config) (Remote, error)) {
	registry[remoteType] = factory
}

func New(cfg *Config) (Remote, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	factory, ok := registry[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("no factory registered for remote type: %s", cfg.Type)
	}

	return factory(cfg)
}

func ArchiveFileName(branchName string) string {
	safe := strings.ReplaceAll(branchName, "/", "_")
	safe = strings.ReplaceAll(safe, "\\", "_")
	safe = strings.ReplaceAll(safe, ":", "_")
	return fmt.Sprintf("%s.pgbranch", safe)
}
