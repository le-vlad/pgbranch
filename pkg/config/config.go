package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DirName        = ".pgbranch"
	ConfigFileName = "config.json"
	SnapshotsDir   = "snapshots"
)

// RemoteConfig holds configuration for a remote storage backend
type RemoteConfig struct {
	// Name is the name of this remote (e.g., "origin")
	Name string `json:"name"`

	// Type is the remote type (fs, s3, gcs)
	Type string `json:"type"`

	// URL is the remote URL
	URL string `json:"url"`

	// Options contains type-specific options
	Options map[string]string `json:"options,omitempty"`
}

type Config struct {
	Database string `json:"database"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password,omitempty"`

	Remotes map[string]*RemoteConfig `json:"remotes,omitempty"`

	DefaultRemote string `json:"default_remote,omitempty"`
}

func DefaultConfig() *Config {
	return &Config{
		Host: "localhost",
		Port: 5432,
		User: "postgres",
	}
}

func GetRootDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current directory: %w", err)
	}
	return filepath.Join(cwd, DirName), nil
}

func GetConfigPath() (string, error) {
	rootDir, err := GetRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rootDir, ConfigFileName), nil
}

func GetSnapshotsDir() (string, error) {
	rootDir, err := GetRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(rootDir, SnapshotsDir), nil
}

func IsInitialized() bool {
	rootDir, err := GetRootDir()
	if err != nil {
		return false
	}
	_, err = os.Stat(rootDir)
	return err == nil
}

func Load() (*Config, error) {
	configPath, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Save() error {
	configPath, err := GetConfigPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func (c *Config) ConnectionString() string {
	connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Database)
	if c.Password != "" {
		connStr += fmt.Sprintf(" password=%s", c.Password)
	}
	return connStr
}

func (c *Config) ConnectionURLForDB(dbName string) string {
	if c.Password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			c.User, c.Password, c.Host, c.Port, dbName)
	}
	return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable",
		c.User, c.Host, c.Port, dbName)
}

func (c *Config) Validate() error {
	if c.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Port == 0 {
		return fmt.Errorf("port is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	return nil
}

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

func (c *Config) AddRemote(remote *RemoteConfig) error {
	if remote.Name == "" {
		return fmt.Errorf("remote name is required")
	}
	if c.Remotes == nil {
		c.Remotes = make(map[string]*RemoteConfig)
	}
	if _, exists := c.Remotes[remote.Name]; exists {
		return fmt.Errorf("remote '%s' already exists", remote.Name)
	}
	c.Remotes[remote.Name] = remote

	if c.DefaultRemote == "" {
		c.DefaultRemote = remote.Name
	}

	return nil
}

func (c *Config) RemoveRemote(name string) error {
	if c.Remotes == nil {
		return fmt.Errorf("remote '%s' not found", name)
	}
	if _, exists := c.Remotes[name]; !exists {
		return fmt.Errorf("remote '%s' not found", name)
	}
	delete(c.Remotes, name)

	if c.DefaultRemote == name {
		c.DefaultRemote = ""
		for remoteName := range c.Remotes {
			c.DefaultRemote = remoteName
			break
		}
	}

	return nil
}

func (c *Config) GetRemote(name string) (*RemoteConfig, error) {
	if name == "" {
		name = c.DefaultRemote
	}
	if name == "" {
		return nil, fmt.Errorf("no remote specified and no default remote configured")
	}
	if c.Remotes == nil {
		return nil, fmt.Errorf("remote '%s' not found", name)
	}
	remote, exists := c.Remotes[name]
	if !exists {
		return nil, fmt.Errorf("remote '%s' not found", name)
	}
	return remote, nil
}

func (c *Config) ListRemotes() []*RemoteConfig {
	if c.Remotes == nil {
		return nil
	}
	remotes := make([]*RemoteConfig, 0, len(c.Remotes))
	for _, remote := range c.Remotes {
		remotes = append(remotes, remote)
	}
	return remotes
}

func (c *Config) SetDefaultRemote(name string) error {
	if c.Remotes == nil || c.Remotes[name] == nil {
		return fmt.Errorf("remote '%s' not found", name)
	}
	c.DefaultRemote = name
	return nil
}
