package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 5432, cfg.Port)
	assert.Equal(t, "postgres", cfg.User)
	assert.Empty(t, cfg.Database)
	assert.Empty(t, cfg.Password)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
			},
			wantErr: false,
		},
		{
			name: "valid config with password",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "secret",
			},
			wantErr: false,
		},
		{
			name: "missing database",
			config: &Config{
				Host: "localhost",
				Port: 5432,
				User: "postgres",
			},
			wantErr: true,
			errMsg:  "database name is required",
		},
		{
			name: "missing host",
			config: &Config{
				Database: "testdb",
				Port:     5432,
				User:     "postgres",
			},
			wantErr: true,
			errMsg:  "host is required",
		},
		{
			name: "missing port",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				User:     "postgres",
			},
			wantErr: true,
			errMsg:  "port is required",
		},
		{
			name: "missing user",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				Port:     5432,
			},
			wantErr: true,
			errMsg:  "user is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected string
	}{
		{
			name: "without password",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
			},
			expected: "host=localhost port=5432 user=postgres dbname=testdb sslmode=disable",
		},
		{
			name: "with password",
			config: &Config{
				Database: "testdb",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "secret",
			},
			expected: "host=localhost port=5432 user=postgres dbname=testdb sslmode=disable password=secret",
		},
		{
			name: "custom port",
			config: &Config{
				Database: "mydb",
				Host:     "db.example.com",
				Port:     5433,
				User:     "admin",
			},
			expected: "host=db.example.com port=5433 user=admin dbname=mydb sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.ConnectionString()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSaveAndLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pgbranch-config-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	pgbranchDir := filepath.Join(tmpDir, DirName)
	err = os.MkdirAll(pgbranchDir, 0755)
	require.NoError(t, err)

	cfg := &Config{
		Database: "testdb",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "secret",
	}

	err = cfg.Save()
	require.NoError(t, err)

	configPath := filepath.Join(pgbranchDir, ConfigFileName)
	_, err = os.Stat(configPath)
	require.NoError(t, err)

	loadedCfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, cfg.Database, loadedCfg.Database)
	assert.Equal(t, cfg.Host, loadedCfg.Host)
	assert.Equal(t, cfg.Port, loadedCfg.Port)
	assert.Equal(t, cfg.User, loadedCfg.User)
	assert.Equal(t, cfg.Password, loadedCfg.Password)
}

func TestIsInitialized(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pgbranch-init-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	assert.False(t, IsInitialized())

	err = os.MkdirAll(filepath.Join(tmpDir, DirName), 0755)
	require.NoError(t, err)

	assert.True(t, IsInitialized())
}

func TestGetRootDir(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	rootDir, err := GetRootDir()
	require.NoError(t, err)

	expected := filepath.Join(cwd, DirName)
	assert.Equal(t, expected, rootDir)
}

func TestGetConfigPath(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	configPath, err := GetConfigPath()
	require.NoError(t, err)

	expected := filepath.Join(cwd, DirName, ConfigFileName)
	assert.Equal(t, expected, configPath)
}

func TestGetSnapshotsDir(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	snapshotsDir, err := GetSnapshotsDir()
	require.NoError(t, err)

	expected := filepath.Join(cwd, DirName, SnapshotsDir)
	assert.Equal(t, expected, snapshotsDir)
}
