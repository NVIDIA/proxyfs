#!/bin/bash -x

source /etc/etcd/etcd.conf

etcd --data-dir=data.etcd \
  --name ${ETCD_NAME} \
  --initial-advertise-peer-urls ${ETCD_INITIAL_ADVERTISE_PEER_URLS} \
  --listen-peer-urls ${ETCD_LISTEN_PEER_URLS} \
  --listen-client-urls ${ETCD_LISTEN_CLIENT_URLS} \
  --advertise-client-urls ${ETCD_ADVERTISE_CLIENT_URLS} \
  --initial-cluster-token ${ETCD_INITIAL_CLUSTER_TOKEN} \
  --initial-cluster ${ETCD_INITIAL_CLUSTER} \
  --initial-cluster-state new 
