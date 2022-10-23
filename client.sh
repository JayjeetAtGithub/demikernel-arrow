#!/bin/bash
set -ex

export CONFIG_PATH=/users/noobjc/demikernel/client.yaml
export MTU=1500
export MSS=1500
export LIBOS=catnip

./code --client 10.10.1.2 3000
