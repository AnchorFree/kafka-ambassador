package kafka

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CertExpirationTime(path string) (time.Time, error) {
	pemData, err := ioutil.ReadFile(filepath.Clean(path))
	if err != nil {
		return time.Unix(0, 0), err
	}
	block, _ := pem.Decode(pemData)
	if block == nil {
		return time.Unix(0, 0), err
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return cert.NotAfter, nil
}

func ParamsCertExpirationTime(kafkaParams *kafka.ConfigMap, certConfigPath string) (time.Time, error) {
	certPath, err := kafkaParams.Get(certConfigPath, "")
	if err != nil {
		return time.Unix(0, 0), err
	}
	if certPath != nil && len(certPath.(string)) > 0 {
		expirationTime, err := CertExpirationTime(certPath.(string))
		if err != nil {
			return time.Unix(0, 0), err
		}
		return expirationTime, nil
	}
	return time.Unix(0, 0), errors.New("Could not fetch path from configuration")
}
