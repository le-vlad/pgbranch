package credentials

import (
	"fmt"
	"os"
)

type RemoteCredentials struct {
	AccessKey string
	SecretKey string
}

type Store struct {
	key []byte
}

func NewStore() (*Store, error) {
	key, err := LoadKey()
	if err != nil {
		return nil, fmt.Errorf("failed to load encryption key: %w", err)
	}
	return &Store{key: key}, nil
}

func (s *Store) EncryptCredentials(creds *RemoteCredentials) (accessKey, secretKey string, err error) {
	if creds.AccessKey != "" {
		accessKey, err = Encrypt(creds.AccessKey, s.key)
		if err != nil {
			return "", "", fmt.Errorf("failed to encrypt access key: %w", err)
		}
	}

	if creds.SecretKey != "" {
		secretKey, err = Encrypt(creds.SecretKey, s.key)
		if err != nil {
			return "", "", fmt.Errorf("failed to encrypt secret key: %w", err)
		}
	}

	return accessKey, secretKey, nil
}

func (s *Store) DecryptCredentials(encAccessKey, encSecretKey string) (*RemoteCredentials, error) {
	creds := &RemoteCredentials{}

	if encAccessKey != "" {
		accessKey, err := Decrypt(encAccessKey, s.key)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt access key: %w", err)
		}
		creds.AccessKey = accessKey
	}

	if encSecretKey != "" {
		secretKey, err := Decrypt(encSecretKey, s.key)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt secret key: %w", err)
		}
		creds.SecretKey = secretKey
	}

	return creds, nil
}

func GetCredentials(options map[string]string, remoteType string) (*RemoteCredentials, error) {
	creds := &RemoteCredentials{}

	encAccessKey := options["encrypted_access_key"]
	encSecretKey := options["encrypted_secret_key"]

	if encAccessKey != "" && encSecretKey != "" {
		store, err := NewStore()
		if err != nil {
			return nil, err
		}
		return store.DecryptCredentials(encAccessKey, encSecretKey)
	}

	creds.AccessKey = options["access_key"]
	if creds.AccessKey == "" {
		if remoteType == "r2" {
			creds.AccessKey = os.Getenv("R2_ACCESS_KEY_ID")
		}
		if creds.AccessKey == "" {
			creds.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		}
	}

	creds.SecretKey = options["secret_key"]
	if creds.SecretKey == "" {
		if remoteType == "r2" {
			creds.SecretKey = os.Getenv("R2_SECRET_ACCESS_KEY")
		}
		if creds.SecretKey == "" {
			creds.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		}
	}

	return creds, nil
}

func RequiresCredentials(remoteType string) bool {
	switch remoteType {
	case "s3", "r2", "gcs":
		return true
	default:
		return false
	}
}

func GetCredentialPrompts(remoteType string) []CredentialPrompt {
	switch remoteType {
	case "s3":
		return []CredentialPrompt{
			{Key: "access_key", Label: "AWS Access Key ID", Secret: false},
			{Key: "secret_key", Label: "AWS Secret Access Key", Secret: true},
		}
	case "r2":
		return []CredentialPrompt{
			{Key: "access_key", Label: "R2 Access Key ID", Secret: false},
			{Key: "secret_key", Label: "R2 Secret Access Key", Secret: true},
		}
	case "gcs":
		return []CredentialPrompt{
			{Key: "service_account", Label: "Service Account JSON path", Secret: false},
		}
	default:
		return nil
	}
}

type CredentialPrompt struct {
	Key    string
	Label  string
	Secret bool
}
