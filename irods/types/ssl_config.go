package types

import (
	"crypto/x509"

	"github.com/hashicorp/go-rootcerts"
	"golang.org/x/xerrors"
)

// IRODSSSLConfig contains irods ssl configuration
type IRODSSSLConfig struct {
	CACertificateFile   string
	CACertificatePath   string
	EncryptionKeySize   int
	EncryptionAlgorithm string
	SaltSize            int
	HashRounds          int
}

// CreateIRODSSSLConfig creates IRODSSSLConfig
func CreateIRODSSSLConfig(caCertFile string, caCertPath string, keySize int, algorithm string, saltSize int, hashRounds int) (*IRODSSSLConfig, error) {
	return &IRODSSSLConfig{
		CACertificateFile:   caCertFile,
		CACertificatePath:   caCertPath,
		EncryptionKeySize:   keySize,
		EncryptionAlgorithm: algorithm,
		SaltSize:            saltSize,
		HashRounds:          hashRounds,
	}, nil
}

// LoadCACert loads CA Cert
func (config *IRODSSSLConfig) LoadCACert() (*x509.CertPool, error) {
	certConfig := &rootcerts.Config{
		CAFile: config.CACertificateFile,
		CAPath: config.CACertificatePath,
	}

	certPool, err := rootcerts.LoadCACerts(certConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to load CA Certificate file: %w", err)
	}

	return certPool, nil
}
