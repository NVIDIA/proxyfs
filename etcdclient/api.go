package etcdclient

import (
	"log"
	"os"
	"time"

	etcd "go.etcd.io/etcd/clientv3"
	etcdtransport "go.etcd.io/etcd/pkg/transport"
)

const (
	certPath      = "/etc/ssl/etcd/ssl/"
	trustedCAFile = certPath + "ca.pem"
)

// New initializes etcd config structures and returns an etcd client
func New(endPoints []string, autoSyncInterval time.Duration, dialTimeout time.Duration) (etcdClient *etcd.Client, err error) {
	var caFile string

	// If we have a local CA then use it.  Otherwise, use the system wide CA
	_, statErr := os.Stat(trustedCAFile)
	if os.IsExist(statErr) {
		caFile = trustedCAFile
	}

	// Certs are named based on the host name.
	h, _ := os.Hostname()
	certFile := certPath + "node-" + h + ".pem"
	keyFile := certPath + "node-" + h + "-key.pem"

	tlsInfo := etcdtransport.TLSInfo{
		CertFile:      certFile,
		KeyFile:       keyFile,
		TrustedCAFile: caFile,
	}
	tlsConfig, etcdErr := tlsInfo.ClientConfig()
	if etcdErr != nil {
		log.Fatal(etcdErr)
	}

	etcdClient, err = etcd.New(etcd.Config{
		Endpoints:        endPoints,
		AutoSyncInterval: autoSyncInterval,
		DialTimeout:      dialTimeout,
		TLS:              tlsConfig,
	})
	return
}
