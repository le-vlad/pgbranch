package grace

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	defaultSlotName        = "grace_slot"
	defaultPublicationName = "grace_pub"
	defaultBatchSize       = 10000
)

// Config represents the YAML configuration for a grace migration.
type Config struct {
	Source          DBConfig `yaml:"source"`
	Target          DBConfig `yaml:"target"`
	Tables          []string `yaml:"tables"`
	SlotName        string   `yaml:"slot_name"`
	PublicationName string   `yaml:"publication_name"`
	BatchSize       int      `yaml:"batch_size"`

	// configPath is the directory where the config file lives (for checkpoint storage).
	configDir string
}

// DBConfig holds connection parameters for a PostgreSQL instance.
type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config path: %w", err)
	}
	cfg.configDir = filepath.Dir(absPath)

	cfg.setDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) setDefaults() {
	if c.SlotName == "" {
		c.SlotName = defaultSlotName
	}
	if c.PublicationName == "" {
		c.PublicationName = defaultPublicationName
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.Source.Port == 0 {
		c.Source.Port = 5432
	}
	if c.Target.Port == 0 {
		c.Target.Port = 5432
	}
	if c.Source.SSLMode == "" {
		c.Source.SSLMode = "prefer"
	}
	if c.Target.SSLMode == "" {
		c.Target.SSLMode = "prefer"
	}
}

// Validate checks that all required fields are present.
func (c *Config) Validate() error {
	if err := c.Source.validate("source"); err != nil {
		return err
	}
	if err := c.Target.validate("target"); err != nil {
		return err
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("at least one table must be specified (use [\"*\"] for all tables)")
	}
	return nil
}

func (d *DBConfig) validate(label string) error {
	if d.Host == "" {
		return fmt.Errorf("%s.host is required", label)
	}
	if d.Database == "" {
		return fmt.Errorf("%s.database is required", label)
	}
	if d.User == "" {
		return fmt.Errorf("%s.user is required", label)
	}
	return nil
}

// ConnectionURL returns a PostgreSQL connection URL.
func (d *DBConfig) ConnectionURL() string {
	if d.Password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			d.User, d.Password, d.Host, d.Port, d.Database, d.SSLMode)
	}
	return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s",
		d.User, d.Host, d.Port, d.Database, d.SSLMode)
}

// ReplicationURL returns a connection URL with the replication=database parameter.
func (d *DBConfig) ReplicationURL() string {
	if d.Password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&replication=database",
			d.User, d.Password, d.Host, d.Port, d.Database, d.SSLMode)
	}
	return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&replication=database",
		d.User, d.Host, d.Port, d.Database, d.SSLMode)
}

// CheckpointPath returns the path for the checkpoint file.
func (c *Config) CheckpointPath() string {
	return filepath.Join(c.configDir, c.SlotName+".checkpoint.json")
}
