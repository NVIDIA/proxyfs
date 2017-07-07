#!/bin/bash

set -e

ENV_NAME=$1
if [ -z "$ENV_NAME" ]; then
  echo "usage: $0 <env-name>"
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ansible-playbook -i "localhost," -c local -e env=$ENV_NAME $SCRIPT_DIR/tasks/main.yml
chef-solo -c $SCRIPT_DIR/chef_files/$ENV_NAME.cfg
