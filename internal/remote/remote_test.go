package remote

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestParseURL_R2(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		wantType    string
		wantBucket  string
		wantPrefix  string
		wantAccount string
		wantErr     bool
	}{
		{
			name:        "full R2 URL with prefix",
			url:         "r2://abc123def/my-bucket/pgbranch/snapshots",
			wantType:    "r2",
			wantBucket:  "my-bucket",
			wantPrefix:  "pgbranch/snapshots",
			wantAccount: "abc123def",
		},
		{
			name:        "R2 URL without prefix",
			url:         "r2://abc123def/my-bucket",
			wantType:    "r2",
			wantBucket:  "my-bucket",
			wantPrefix:  "",
			wantAccount: "abc123def",
		},
		{
			name:    "R2 URL without bucket",
			url:     "r2://abc123def/",
			wantErr: true,
		},
		{
			name:    "R2 URL without account",
			url:     "r2:///my-bucket",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseURL("test", tt.url)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseURL() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ParseURL() unexpected error: %v", err)
				return
			}
			if cfg.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", cfg.Type, tt.wantType)
			}
			if cfg.Options["bucket"] != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", cfg.Options["bucket"], tt.wantBucket)
			}
			if cfg.Options["prefix"] != tt.wantPrefix {
				t.Errorf("prefix = %q, want %q", cfg.Options["prefix"], tt.wantPrefix)
			}
			if cfg.Options["account_id"] != tt.wantAccount {
				t.Errorf("account_id = %q, want %q", cfg.Options["account_id"], tt.wantAccount)
			}
			expectedEndpoint := "https://" + tt.wantAccount + ".r2.cloudflarestorage.com"
			if cfg.Options["endpoint"] != expectedEndpoint {
				t.Errorf("endpoint = %q, want %q", cfg.Options["endpoint"], expectedEndpoint)
			}
		})
	}
}

func TestParseURL_S3(t *testing.T) {
	cfg, err := ParseURL("origin", "s3://my-bucket/prefix")
	if err != nil {
		t.Fatalf("ParseURL() error: %v", err)
	}
	if cfg.Type != "s3" {
		t.Errorf("Type = %q, want %q", cfg.Type, "s3")
	}
	if cfg.Options["bucket"] != "my-bucket" {
		t.Errorf("bucket = %q, want %q", cfg.Options["bucket"], "my-bucket")
	}
	if cfg.Options["prefix"] != "prefix" {
		t.Errorf("prefix = %q, want %q", cfg.Options["prefix"], "prefix")
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:   "valid fs",
			config: Config{Name: "local", Type: "fs", URL: "/tmp/backups"},
		},
		{
			name:   "valid s3",
			config: Config{Name: "origin", Type: "s3", URL: "s3://bucket/prefix"},
		},
		{
			name:   "valid r2",
			config: Config{Name: "r2remote", Type: "r2", URL: "r2://acct/bucket"},
		},
		{
			name:   "valid gcs",
			config: Config{Name: "gcsremote", Type: "gcs", URL: "gs://bucket/prefix"},
		},
		{
			name:    "missing name",
			config:  Config{Name: "", Type: "fs", URL: "/tmp"},
			wantErr: true,
		},
		{
			name:    "missing type",
			config:  Config{Name: "local", Type: "", URL: "/tmp"},
			wantErr: true,
		},
		{
			name:    "missing url",
			config:  Config{Name: "local", Type: "fs", URL: ""},
			wantErr: true,
		},
		{
			name:    "unsupported type",
			config:  Config{Name: "bad", Type: "azure", URL: "az://container"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("Validate() expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

func TestArchiveFileName(t *testing.T) {
	tests := []struct {
		name     string
		branch   string
		expected string
	}{
		{name: "simple name", branch: "main", expected: "main.pgbranch"},
		{name: "with slashes", branch: "feature/foo/bar", expected: "feature_foo_bar.pgbranch"},
		{name: "with backslashes", branch: "feature\\foo", expected: "feature_foo.pgbranch"},
		{name: "with colons", branch: "ref:heads:main", expected: "ref_heads_main.pgbranch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ArchiveFileName(tt.branch)
			if got != tt.expected {
				t.Errorf("ArchiveFileName(%q) = %q, want %q", tt.branch, got, tt.expected)
			}
		})
	}
}

func TestParseURL_Filesystem(t *testing.T) {
	cfg, err := ParseURL("local", "/tmp/backups")
	if err != nil {
		t.Fatalf("ParseURL() error: %v", err)
	}
	if cfg.Type != "fs" {
		t.Errorf("Type = %q, want %q", cfg.Type, "fs")
	}
	if cfg.URL != "/tmp/backups" {
		t.Errorf("URL = %q, want %q", cfg.URL, "/tmp/backups")
	}
}

func TestParseURL_FileScheme(t *testing.T) {
	cfg, err := ParseURL("local", "file:///tmp/backups")
	if err != nil {
		t.Fatalf("ParseURL() error: %v", err)
	}
	if cfg.Type != "fs" {
		t.Errorf("Type = %q, want %q", cfg.Type, "fs")
	}
	if cfg.URL != "/tmp/backups" {
		t.Errorf("URL = %q, want %q", cfg.URL, "/tmp/backups")
	}
}

func TestParseURL_GCS(t *testing.T) {
	cfg, err := ParseURL("gcs", "gs://my-bucket/some/prefix")
	if err != nil {
		t.Fatalf("ParseURL() error: %v", err)
	}
	if cfg.Type != "gcs" {
		t.Errorf("Type = %q, want %q", cfg.Type, "gcs")
	}
	if cfg.Options["bucket"] != "my-bucket" {
		t.Errorf("bucket = %q, want %q", cfg.Options["bucket"], "my-bucket")
	}
	if cfg.Options["prefix"] != "some/prefix" {
		t.Errorf("prefix = %q, want %q", cfg.Options["prefix"], "some/prefix")
	}
}

func TestParseURL_UnsupportedScheme(t *testing.T) {
	_, err := ParseURL("bad", "ftp://server/path")
	if err == nil {
		t.Errorf("ParseURL() expected error for unsupported scheme, got nil")
	}
}

func TestRegistryNew_Filesystem(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{Name: "local", Type: "fs", URL: dir}

	rem, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if rem.Name() != "local" {
		t.Errorf("Name() = %q, want %q", rem.Name(), "local")
	}
	if rem.Type() != "fs" {
		t.Errorf("Type() = %q, want %q", rem.Type(), "fs")
	}
}

func TestRegistryNew_InvalidConfig(t *testing.T) {
	cfg := &Config{Name: "", Type: "fs", URL: "/tmp"}
	_, err := New(cfg)
	if err == nil {
		t.Errorf("New() expected error for invalid config, got nil")
	}
}

func TestRegistryNew_UnregisteredType(t *testing.T) {
	cfg := &Config{Name: "x", Type: "magiccloud", URL: "magiccloud://bucket"}

	err := cfg.Validate()
	if err == nil {
		t.Fatalf("expected Validate to reject unknown type")
	}
}

func TestFilesystemRemote_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	cfg := &Config{Name: "testremote", Type: "fs", URL: dir}
	rem, err := NewFilesystemRemote(cfg)
	if err != nil {
		t.Fatalf("NewFilesystemRemote() error: %v", err)
	}

	if rem.Name() != "testremote" {
		t.Errorf("Name() = %q, want %q", rem.Name(), "testremote")
	}
	if rem.Type() != "fs" {
		t.Errorf("Type() = %q, want %q", rem.Type(), "fs")
	}

	data := []byte("snapshot-data-here")
	err = rem.Push(ctx, "dev", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	archivePath := filepath.Join(dir, "dev.pgbranch")
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("expected archive file at %s, got error: %v", archivePath, err)
	}

	exists, err := rem.Exists(ctx, "dev")
	if err != nil {
		t.Fatalf("Exists() error: %v", err)
	}
	if !exists {
		t.Errorf("Exists(dev) = false, want true")
	}

	exists, err = rem.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Exists() error: %v", err)
	}
	if exists {
		t.Errorf("Exists(nonexistent) = true, want false")
	}

	rc, size, err := rem.Pull(ctx, "dev")
	if err != nil {
		t.Fatalf("Pull() error: %v", err)
	}
	pulled, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll() error: %v", err)
	}
	if int64(len(data)) != size {
		t.Errorf("Pull size = %d, want %d", size, len(data))
	}
	if !bytes.Equal(pulled, data) {
		t.Errorf("Pull data = %q, want %q", pulled, data)
	}

	branches, err := rem.List(ctx)
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(branches) != 1 {
		t.Fatalf("List() returned %d branches, want 1", len(branches))
	}
	if branches[0].Name != "dev" {
		t.Errorf("List()[0].Name = %q, want %q", branches[0].Name, "dev")
	}

	err = rem.Delete(ctx, "dev")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	exists, err = rem.Exists(ctx, "dev")
	if err != nil {
		t.Fatalf("Exists() after delete error: %v", err)
	}
	if exists {
		t.Errorf("Exists(dev) after delete = true, want false")
	}

	_, _, err = rem.Pull(ctx, "dev")
	if err == nil {
		t.Errorf("Pull() after delete expected error, got nil")
	}
}

func TestFilesystemRemote_ListNonExistentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	ctx := context.Background()

	r := &FilesystemRemote{name: "test", path: dir}
	branches, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(branches) != 0 {
		t.Errorf("List() returned %d branches, want 0", len(branches))
	}
}
