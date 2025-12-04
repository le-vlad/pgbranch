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

type Config struct {
	Database string `json:"database"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password,omitempty"`
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
