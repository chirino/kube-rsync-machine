package tlsutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"time"
)

type Bundle struct {
	CACertPEM []byte
	CertPEM   []byte
	KeyPEM    []byte
}

func (b Bundle) SecretData() map[string][]byte {
	return map[string][]byte{
		SecretCAFile:   append([]byte(nil), b.CACertPEM...),
		SecretCertFile: append([]byte(nil), b.CertPEM...),
		SecretKeyFile:  append([]byte(nil), b.KeyPEM...),
	}
}

type CA struct {
	cert    *x509.Certificate
	key     ed25519.PrivateKey
	certPEM []byte
}

func NewRunCA(runNamespace, runName string, ttl time.Duration) (*CA, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive")
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	cert := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: fmt.Sprintf("krm-run-ca:%s/%s", runNamespace, runName)},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(ttl),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, key.Public(), key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &CA{cert: cert, key: key, certPEM: certPEM}, nil
}

func (ca *CA) Mint(identity Identity, ttl time.Duration) (Bundle, error) {
	if ca == nil || ca.cert == nil || ca.key == nil {
		return Bundle{}, fmt.Errorf("ca is required")
	}
	if ttl <= 0 {
		return Bundle{}, fmt.Errorf("ttl must be positive")
	}
	uri, err := url.Parse(identity.URI())
	if err != nil {
		return Bundle{}, err
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Bundle{}, err
	}
	now := time.Now().UTC()
	notAfter := now.Add(ttl)
	if notAfter.After(ca.cert.NotAfter) {
		notAfter = ca.cert.NotAfter
	}
	cert := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: string(identity.Role)},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, key.Public(), ca.key)
	if err != nil {
		return Bundle{}, err
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Bundle{}, err
	}
	return Bundle{
		CACertPEM: append([]byte(nil), ca.certPEM...),
		CertPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:    pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}),
	}, nil
}

func VerifyIdentity(bundle Bundle, expected Identity, now time.Time) error {
	cert, err := parseCertificate(bundle.CertPEM)
	if err != nil {
		return err
	}
	caCert, err := parseCertificate(bundle.CACertPEM)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	if now.IsZero() {
		now = time.Now()
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return err
	}
	for _, uri := range cert.URIs {
		identity, err := ParseIdentity(uri.String())
		if err == nil && identity == expected {
			return nil
		}
	}
	return fmt.Errorf("certificate does not contain expected identity %s", expected.URI())
}

func parseCertificate(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("certificate PEM block is required")
	}
	return x509.ParseCertificate(block.Bytes)
}

func serial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	value, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return value
}
