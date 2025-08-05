package types

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-rootcerts"
	log "github.com/sirupsen/logrus"
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
func (config *IRODSSSLConfig) LoadCACert(ignoreWrongFile bool) (*x509.CertPool, error) {
	logger := log.WithFields(log.Fields{
		"ignore_wrong_file": ignoreWrongFile,
	})

	if len(config.CACertificateFile) > 0 {
		// check file exists
		_, err := os.Stat(config.CACertificateFile)
		if err != nil {
			if os.IsNotExist(err) {
				if ignoreWrongFile {
					logger.Debugf("CA Certificate File %q does not exist, ignoring.", config.CACertificateFile)
				} else {
					return nil, xerrors.Errorf("CA Certificate File %q error: %w", config.CACertificateFile, NewFileNotFoundError(config.CACertificateFile))
				}
			} else {
				return nil, xerrors.Errorf("CA Certificate File %q error: %w", config.CACertificateFile, err)
			}
		}
	}

	if len(config.CACertificatePath) > 0 {
		// check file exists
		_, err := os.Stat(config.CACertificatePath)
		if err != nil {
			if os.IsNotExist(err) {
				if ignoreWrongFile {
					return nil, xerrors.Errorf("CA Certificate Path %q error: %w", config.CACertificatePath, NewFileNotFoundError(config.CACertificatePath))
				} else {
					logger.Debugf("CA Certificate Path %q does not exist, ignoring.", config.CACertificatePath)
				}
			} else {
				return nil, xerrors.Errorf("CA Certificate Path %q error: %w", config.CACertificatePath, err)
			}
		}
	}

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
func (config *IRODSSSLConfig) GetTLSConfig(serverName string, ignoreCertFileError bool) (*tls.Config, error) {
	caCertPool, err := config.LoadCACert(ignoreCertFileError)
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
		CipherSuites: []uint16{
			//cipherSuitesPreferenceOrder
			// AEADs w/ ECDHE
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,

			// CBC w/ ECDHE
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,

			// AEADs w/o ECDHE
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,

			// CBC w/o ECDHE
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,

			// 3DES
			tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
			tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,

			// CBC_SHA256
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA256,

			// RC4
			tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA, tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
			tls.TLS_RSA_WITH_RC4_128_SHA,

			//rsaKexCiphers
			tls.TLS_RSA_WITH_RC4_128_SHA,
			tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,

			//disabledCipherSuites
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA256,

			tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
			tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
			tls.TLS_RSA_WITH_RC4_128_SHA,
		},
	}, nil
}

func (config *IRODSSSLConfig) Validate() error {
	// icommands ignores non-existing ca cert files and paths
	//if len(config.CACertificateFile) > 0 {
	//	// check file exists
	//	_, err := os.Stat(config.CACertificateFile)
	//	if err != nil {
	//		return xerrors.Errorf("CA Certificate File %q does not exist: %w", config.CACertificateFile, err)
	//	}
	//}

	//if len(config.CACertificatePath) > 0 {
	//	// check file exists
	//	_, err := os.Stat(config.CACertificatePath)
	//	if err != nil {
	//		return xerrors.Errorf("CA Certificate Path %q does not exist: %w", config.CACertificatePath, err)
	//	}
	//}

	if config.EncryptionKeySize <= 0 {
		return xerrors.Errorf("invalid encryption key size")
	}

	if len(config.EncryptionAlgorithm) == 0 {
		return xerrors.Errorf("empty encryption algorithm")
	}

	if config.EncryptionSaltSize <= 0 {
		return xerrors.Errorf("invalid encryption salt size")
	}

	if config.EncryptionNumHashRounds <= 0 {
		return xerrors.Errorf("invalid encryption number of hash rounds")
	}

	_, err := GetSSLVerifyServer(string(config.VerifyServer))
	if err != nil {
		return xerrors.Errorf("failed to validate SSL verify server: %w", err)
	}

	return nil
}
