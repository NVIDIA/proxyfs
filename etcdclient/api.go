// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package etcdclient

import (
	"log"
	"os"
	"time"

	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

const (
	trustedCAFile = "/ca.pem"
)

// New initializes etcd config structures and returns an etcd client
func New(tlsInfo *transport.TLSInfo, endPoints []string, autoSyncInterval time.Duration, dialTimeout time.Duration) (etcdClient *etcd.Client, err error) {
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

// GetCertFile returns the name of the cert file for the local node
func GetCertFile(certPath string) string {
	h, _ := os.Hostname()
	return certPath + "/node-" + h + ".pem"
}

// GetKeyFile returns the name of the key file for the local node
func GetKeyFile(certPath string) string {
	h, _ := os.Hostname()
	return certPath + "/node-" + h + "-key.pem"
}

// GetCA returns the name of the certificate authority for the local node
func GetCA(certPath string) string {
	var (
		caFile string
	)

	_, statErr := os.Stat(certPath + trustedCAFile)
	if os.IsExist(statErr) {
		caFile = trustedCAFile
	}

	return caFile
}
