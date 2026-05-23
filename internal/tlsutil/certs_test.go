package tlsutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIdentityRoundTrip(t *testing.T) {
	expected := SourceIdentity("backup", "run-1", "app", "files")
	actual, err := ParseIdentity(expected.URI())
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("expected %#v, got %#v", expected, actual)
	}
}

func TestLoadBundleAndTLSConfig(t *testing.T) {
	ca, err := NewRunCA("backup", "run-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	identity := TargetIdentity("backup", "run-1", "backup", "archive")
	bundle, err := ca.Mint(identity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, data := range bundle.SecretData() {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := LoadBundle(dir)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.CACertPEM) != string(bundle.CACertPEM) {
		t.Fatal("loaded CA did not match")
	}
	config, err := TLSConfig(loaded, identity, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Certificates) != 1 || config.RootCAs == nil || config.ClientCAs == nil {
		t.Fatalf("unexpected tls config: %#v", config)
	}
}

func TestMintAndVerifySourceIdentity(t *testing.T) {
	ca, err := NewRunCA("backup", "run-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	identity := SourceIdentity("backup", "run-1", "app", "files")
	bundle, err := ca.Mint(identity, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyIdentity(bundle, identity, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := VerifyIdentity(bundle, TargetIdentity("backup", "run-1", "backup", "archive"), time.Now()); err == nil {
		t.Fatal("expected wrong identity to fail")
	}
	data := bundle.SecretData()
	for _, key := range []string{"ca.crt", "tls.crt", "tls.key"} {
		if len(data[key]) == 0 {
			t.Fatalf("expected secret data key %s", key)
		}
	}
}

func TestLeafExpiryIsCappedByCA(t *testing.T) {
	ca, err := NewRunCA("backup", "run-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	identity := TargetIdentity("backup", "run-1", "backup", "archive")
	bundle, err := ca.Mint(identity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyIdentity(bundle, identity, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected expired certificate to fail")
	}
}
