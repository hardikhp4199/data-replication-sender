package common

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"datareplication_sender/config"
	"datareplication_sender/storage/logging"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	rootCAPath          = config.GetString("Receiver.SSL.RootCA")
	maxIdleConns        = config.GetString("Receiver.Transport.MaxIdleConns")
	maxIdleConnsPerHost = config.GetString("Receiver.Transport.MaxIdleConnsPerHost")
	maxConnsPerHost     = config.GetString("Receiver.Transport.MaxConnsPerHost")
	idleConnTimeout     = config.GetString("Receiver.Transport.IdleConnTimeout")
)

func GetTranportObject() (tr *http.Transport, errOut error) {

	maxIdleConns, _ := strconv.Atoi(maxIdleConns)
	maxIdleConnsPerHost, _ := strconv.Atoi(maxIdleConnsPerHost)
	maxConnsPerHost, _ := strconv.Atoi(maxConnsPerHost)
	idleConnTimeout, _ := strconv.Atoi(idleConnTimeout)

	if strings.TrimSpace(rootCAPath) != "" {
		rootCA, readFileErr := os.ReadFile(rootCAPath)
		if readFileErr != nil {
			errOut = logging.EnrichErrorWithStackTrace(readFileErr)
		} else {
			rootCACert := x509.NewCertPool()
			ok := rootCACert.AppendCertsFromPEM(rootCA)

			if !ok {
				errOut = logging.EnrichErrorWithStackTrace(errors.New("fails to parse root to cert"))
			} else {
				tr = &http.Transport{
					MaxIdleConns:        maxIdleConns,
					MaxIdleConnsPerHost: maxIdleConnsPerHost,
					MaxConnsPerHost:     maxConnsPerHost,
					IdleConnTimeout:     time.Duration(idleConnTimeout) * time.Second,
					DisableCompression:  false,
					TLSClientConfig: &tls.Config{
						RootCAs: rootCACert,
					},
					ForceAttemptHTTP2: true,
				}
			}
		}
	} else {
		tr = &http.Transport{
			MaxIdleConns:        maxIdleConns,
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			MaxConnsPerHost:     maxConnsPerHost,
			IdleConnTimeout:     time.Duration(idleConnTimeout) * time.Second,
			DisableCompression:  false,
			ForceAttemptHTTP2:   true,
		}
	}
	return
}
func CompressDataTOGzip(str []byte) (buf bytes.Buffer, errOut error) {

	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(str)
	if err != nil {
		errOut = logging.EnrichErrorWithStackTrace(err)
	} else {
		if err := zw.Close(); err != nil {
			errOut = logging.EnrichErrorWithStackTrace(err)
		}
	}
	return buf, errOut
}
