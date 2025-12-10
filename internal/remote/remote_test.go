package remote

import (
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
