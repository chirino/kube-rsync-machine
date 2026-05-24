package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

const (
	SecretCAFile        = "ca.crt"
	SecretCAKeyFile     = "ca.key"
	SecretCertFile      = "tls.crt"
	SecretKeyFile       = "tls.key"
	SecretControlCAFile = "control-ca.crt"
)

func LoadBundle(dir string) (Bundle, error) {
	if dir == "" {
		return Bundle{}, fmt.Errorf("tls directory is required")
	}
	caCert, err := os.ReadFile(filepath.Join(dir, SecretCAFile))
	if err != nil {
		return Bundle{}, fmt.Errorf("read %s: %w", SecretCAFile, err)
	}
	cert, err := os.ReadFile(filepath.Join(dir, SecretCertFile))
	if err != nil {
		return Bundle{}, fmt.Errorf("read %s: %w", SecretCertFile, err)
	}
	key, err := os.ReadFile(filepath.Join(dir, SecretKeyFile))
	if err != nil {
		return Bundle{}, fmt.Errorf("read %s: %w", SecretKeyFile, err)
	}
	return Bundle{CACertPEM: caCert, CertPEM: cert, KeyPEM: key}, nil
}

func TLSConfig(bundle Bundle, expectedPeer Identity, serverName string) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(bundle.CertPEM, bundle.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.CACertPEM) {
		return nil, fmt.Errorf("ca.crt does not contain a certificate")
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		ServerName:   serverName,
	}
	if expectedPeer != (Identity{}) {
		if serverName == "" {
			config.InsecureSkipVerify = true
		}
		config.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("peer certificate is required")
			}
			peer, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				cert, err := x509.ParseCertificate(raw)
				if err != nil {
					return err
				}
				intermediates.AddCert(cert)
			}
			if _, err := peer.Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				DNSName:       serverName,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			}); err != nil {
				return err
			}
			for _, uri := range peer.URIs {
				identity, err := ParseIdentity(uri.String())
				if err == nil && identity == expectedPeer {
					return nil
				}
			}
			return fmt.Errorf("peer certificate does not contain expected identity %s", expectedPeer.URI())
		}
	}
	return config, nil
}
