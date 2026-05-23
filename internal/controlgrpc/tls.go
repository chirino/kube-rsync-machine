package controlgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type ClientCertificateVerifier func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

func ServerCredentials(server tlsutil.Bundle, clientCAPEMs ...[]byte) (credentials.TransportCredentials, error) {
	cert, err := tls.X509KeyPair(server.CertPEM, server.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	clientCAs := x509.NewCertPool()
	for _, caPEM := range clientCAPEMs {
		if len(caPEM) > 0 && !clientCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("client CA bundle does not contain a certificate")
		}
	}
	if len(clientCAPEMs) == 0 {
		if !clientCAs.AppendCertsFromPEM(server.CACertPEM) {
			return nil, fmt.Errorf("server ca.crt does not contain a certificate")
		}
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

func ServerCredentialsWithClientVerifier(server tlsutil.Bundle, verify ClientCertificateVerifier) (credentials.TransportCredentials, error) {
	if verify == nil {
		return nil, fmt.Errorf("client certificate verifier is required")
	}
	cert, err := tls.X509KeyPair(server.CertPEM, server.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates:          []tls.Certificate{cert},
		ClientAuth:            tls.RequireAnyClientCert,
		MinVersion:            tls.VersionTLS12,
		VerifyPeerCertificate: verify,
	}), nil
}

func ClientCredentials(client tlsutil.Bundle, expectedServer tlsutil.Identity) (credentials.TransportCredentials, error) {
	return ClientCredentialsWithServerCA(client, client.CACertPEM, expectedServer)
}

func ClientCredentialsWithServerCA(client tlsutil.Bundle, serverCAPEM []byte, expectedServer tlsutil.Identity) (credentials.TransportCredentials, error) {
	cert, err := tls.X509KeyPair(client.CertPEM, client.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCAPEM) {
		return nil, fmt.Errorf("server CA bundle does not contain a certificate")
	}
	config := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            roots,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyPeer(rawCerts, roots, expectedServer, x509.ExtKeyUsageServerAuth)
		},
	}
	return credentials.NewTLS(config), nil
}

func PeerIdentity(ctx context.Context) (tlsutil.Identity, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return tlsutil.Identity{}, false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return tlsutil.Identity{}, false
	}
	for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
		identity, err := tlsutil.ParseIdentity(uri.String())
		if err == nil {
			return identity, true
		}
	}
	return tlsutil.Identity{}, false
}

func VerifyClientCertificateWithCA(rawCerts [][]byte, caPEM []byte, expected tlsutil.Identity) error {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("client CA bundle does not contain a certificate")
	}
	return verifyPeer(rawCerts, roots, expected, x509.ExtKeyUsageClientAuth)
}

func verifyPeer(rawCerts [][]byte, roots *x509.CertPool, expected tlsutil.Identity, usage x509.ExtKeyUsage) error {
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
		KeyUsages:     []x509.ExtKeyUsage{usage},
	}); err != nil {
		return err
	}
	if expected == (tlsutil.Identity{}) {
		return nil
	}
	for _, uri := range peer.URIs {
		identity, err := tlsutil.ParseIdentity(uri.String())
		if err == nil && identity == expected {
			return nil
		}
	}
	return fmt.Errorf("peer certificate does not contain expected identity %s", expected.URI())
}
