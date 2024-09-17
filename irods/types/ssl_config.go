package types

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/hashicorp/go-rootcerts"
	"golang.org/x/xerrors"
)

// SSLVerifyServer defines SSL Verify Server options
type SSLVerifyServer string

const (
	// SSLVerifyServerCert verifies server by cert
	SSLVerifyServerCert SSLVerifyServer = "cert"
	// SSLVerifyServerHostname verifies server by hostname
	SSLVerifyServerHostname SSLVerifyServer = "hostname"
	// SSLVerifyServerNone does not verify server
	SSLVerifyServerNone SSLVerifyServer = "none"
)

// GetSSLVerifyServer returns SSLVerifyServer value from string
func GetSSLVerifyServer(verifyServer string) (SSLVerifyServer, error) {
	var sslVerifyServer SSLVerifyServer
	var err error = nil
	switch strings.TrimSpace(strings.ToLower(verifyServer)) {
	case string(SSLVerifyServerCert):
		sslVerifyServer = SSLVerifyServerCert
	case string(SSLVerifyServerHostname):
		sslVerifyServer = SSLVerifyServerHostname
	case string(SSLVerifyServerNone), "":
		sslVerifyServer = SSLVerifyServerNone
	default:
		sslVerifyServer = SSLVerifyServerNone
		err = fmt.Errorf("cannot parse string %q", verifyServer)
	}

	return sslVerifyServer, err
}

// IsVerificationRequired checks if verification is required
func (verify SSLVerifyServer) IsVerificationRequired() bool {
	return verify == SSLVerifyServerHostname
}

// IRODSSSLConfig contains irods ssl configuration
type IRODSSSLConfig struct {
	CACertificateFile       string
	CACertificatePath       string
	EncryptionKeySize       int
	EncryptionAlgorithm     string
	EncryptionSaltSize      int
	EncryptionNumHashRounds int
	VerifyServer            SSLVerifyServer
	DHParamsFile            string
	ServerName              string // optional server name for verification
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

// GetTLSConfig returns TLS Config
func (config *IRODSSSLConfig) GetTLSConfig(serverName string) (*tls.Config, error) {
	caCertPool, err := config.LoadCACert()
	if err != nil {
		return nil, err
	}

	if len(config.ServerName) > 0 {
		serverName = config.ServerName
	}

	return &tls.Config{
		RootCAs:            caCertPool,
		ServerName:         serverName,
		InsecureSkipVerify: !config.VerifyServer.IsVerificationRequired(),
	}, nil
}
