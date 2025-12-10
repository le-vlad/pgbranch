package credentials

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	plaintext := "my-secret-access-key"

	encrypted, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if encrypted == plaintext {
		t.Error("encrypted should not equal plaintext")
	}

	decrypted, err := Decrypt(encrypted, key)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptDecryptEmpty(t *testing.T) {
	key, _ := GenerateKey()

	encrypted, err := Encrypt("", key)
	if err != nil {
		t.Fatalf("failed to encrypt empty: %v", err)
	}

	decrypted, err := Decrypt(encrypted, key)
	if err != nil {
		t.Fatalf("failed to decrypt empty: %v", err)
	}

	if decrypted != "" {
		t.Errorf("decrypted = %q, want empty", decrypted)
	}
}

func TestDecryptWithWrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	encrypted, _ := Encrypt("secret", key1)

	_, err := Decrypt(encrypted, key2)
	if err == nil {
		t.Error("expected error when decrypting with wrong key")
	}
}

func TestSaveLoadKey(t *testing.T) {
	tmpDir := t.TempDir()
	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", oldHome)

	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	if err := SaveKey(key); err != nil {
		t.Fatalf("failed to save key: %v", err)
	}

	keyPath := filepath.Join(tmpDir, KeyFileName)
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file not created: %v", err)
	}

	if info.Mode().Perm() != 0600 {
		t.Errorf("key file permissions = %o, want 0600", info.Mode().Perm())
	}

	loaded, err := LoadKey()
	if err != nil {
		t.Fatalf("failed to load key: %v", err)
	}

	if len(loaded) != len(key) {
		t.Errorf("loaded key length = %d, want %d", len(loaded), len(key))
	}

	for i := range key {
		if loaded[i] != key[i] {
			t.Errorf("loaded key differs at byte %d", i)
			break
		}
	}
}

func TestStoreEncryptDecryptCredentials(t *testing.T) {
	tmpDir := t.TempDir()
	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", oldHome)

	_, _, err := EnsureKey()
	if err != nil {
		t.Fatalf("failed to ensure key: %v", err)
	}

	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	creds := &RemoteCredentials{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	encAccess, encSecret, err := store.EncryptCredentials(creds)
	if err != nil {
		t.Fatalf("failed to encrypt credentials: %v", err)
	}

	if encAccess == creds.AccessKey {
		t.Error("encrypted access key should not equal plaintext")
	}
	if encSecret == creds.SecretKey {
		t.Error("encrypted secret key should not equal plaintext")
	}

	decrypted, err := store.DecryptCredentials(encAccess, encSecret)
	if err != nil {
		t.Fatalf("failed to decrypt credentials: %v", err)
	}

	if decrypted.AccessKey != creds.AccessKey {
		t.Errorf("access key = %q, want %q", decrypted.AccessKey, creds.AccessKey)
	}
	if decrypted.SecretKey != creds.SecretKey {
		t.Errorf("secret key = %q, want %q", decrypted.SecretKey, creds.SecretKey)
	}
}

func TestGetCredentialsFromEnv(t *testing.T) {
	os.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")

	creds, err := GetCredentials(map[string]string{}, "s3")
	if err != nil {
		t.Fatalf("failed to get credentials: %v", err)
	}

	if creds.AccessKey != "test-access" {
		t.Errorf("access key = %q, want %q", creds.AccessKey, "test-access")
	}
	if creds.SecretKey != "test-secret" {
		t.Errorf("secret key = %q, want %q", creds.SecretKey, "test-secret")
	}
}

func TestGetCredentialsR2FromEnv(t *testing.T) {
	os.Setenv("R2_ACCESS_KEY_ID", "r2-access")
	os.Setenv("R2_SECRET_ACCESS_KEY", "r2-secret")
	defer os.Unsetenv("R2_ACCESS_KEY_ID")
	defer os.Unsetenv("R2_SECRET_ACCESS_KEY")

	creds, err := GetCredentials(map[string]string{}, "r2")
	if err != nil {
		t.Fatalf("failed to get credentials: %v", err)
	}

	if creds.AccessKey != "r2-access" {
		t.Errorf("access key = %q, want %q", creds.AccessKey, "r2-access")
	}
	if creds.SecretKey != "r2-secret" {
		t.Errorf("secret key = %q, want %q", creds.SecretKey, "r2-secret")
	}
}

func TestRequiresCredentials(t *testing.T) {
	tests := []struct {
		remoteType string
		want       bool
	}{
		{"s3", true},
		{"r2", true},
		{"gcs", true},
		{"fs", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.remoteType, func(t *testing.T) {
			got := RequiresCredentials(tt.remoteType)
			if got != tt.want {
				t.Errorf("RequiresCredentials(%q) = %v, want %v", tt.remoteType, got, tt.want)
			}
		})
	}
}
