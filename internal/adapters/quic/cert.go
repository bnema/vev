package quic

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"
)

// Ephemeral certificates are minted per bootstrap listener and never
// persisted: the SHA-256 pin travels through the SSH bootstrap channel,
// and the key dies with the process.

// GenerateEphemeralCert mints one self-signed Ed25519 certificate valid
// for 24 hours. The returned fingerprint is the SHA-256 of the raw DER
// certificate the dialer pins.
func GenerateEphemeralCert() (tls.Certificate, []byte, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "vev-ephemeral"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"vev-bootstrap"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustMarshalPKCS8(private)}),
	)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	sum := sha256.Sum256(der)
	return cert, sum[:], nil
}

// mustMarshalPKCS8 marshals an Ed25519 private key; Ed25519 keys always
// marshal, so a failure panics as a programming error.
func mustMarshalPKCS8(private ed25519.PrivateKey) []byte {
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		panic(err)
	}
	return der
}

// FingerprintDER returns the SHA-256 pin of one DER certificate.
func FingerprintDER(der []byte) []byte {
	sum := sha256.Sum256(der)
	return sum[:]
}
