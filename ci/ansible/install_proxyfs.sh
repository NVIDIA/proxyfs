#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

ENV_NAME=$1
GOLANG_VERSION=$2

if [ -z "$ENV_NAME" ]; then
  echo "usage: $0 <env-name>"
  exit 1
fi

if [ -z "$GOLANG_VERSION" ]; then
  GOLANG_VERSION="current"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ansible-playbook -i "localhost," -c local -e env=$ENV_NAME -e env_arg="$1" -e golang_version="$GOLANG_VERSION" "$SCRIPT_DIR"/tasks/main.yml
chef-solo -c "$SCRIPT_DIR"/chef_files/$ENV_NAME.cfg
