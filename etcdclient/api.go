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

// GetCertFilePath returns the name of the cert file for the local node
func GetCertFilePath(certDir string) string {
	h, _ := os.Hostname()
	return certDir + "/node-" + h + ".pem"
}

// GetKeyFilePath returns the name of the key file for the local node
func GetKeyFilePath(certDir string) string {
	h, _ := os.Hostname()
	return certDir + "/node-" + h + "-key.pem"
}

// GetCA returns the name of the certificate authority for the local node
func GetCA(certDir string) string {
	var (
		caFile string
	)

	trustedCAFilePath := certDir + trustedCAFile
	_, statErr := os.Stat(trustedCAFilePath)
	if os.IsExist(statErr) {
		caFile = trustedCAFilePath
	}

	return caFile
}
