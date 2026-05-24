package manager

import (
	"crypto/x509"
	"fmt"

	"github.com/chirino/kube-rsync-machine/internal/controlgrpc"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
)

func NewRunClientCertificateVerifier(controlCAPEM []byte) controlgrpc.ClientCertificateVerifier {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		identity, err := identityFromRawCertificate(rawCerts)
		if err != nil {
			return err
		}
		return controlgrpc.VerifyClientCertificateWithCA(rawCerts, controlCAPEM, identity)
	}
}

func identityFromRawCertificate(rawCerts [][]byte) (tlsutil.Identity, error) {
	if len(rawCerts) == 0 {
		return tlsutil.Identity{}, fmt.Errorf("client certificate is required")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return tlsutil.Identity{}, err
	}
	for _, uri := range cert.URIs {
		identity, err := tlsutil.ParseIdentity(uri.String())
		if err == nil {
			return identity, nil
		}
	}
	return tlsutil.Identity{}, fmt.Errorf("client certificate does not contain a kube-rsync-machine identity")
}
