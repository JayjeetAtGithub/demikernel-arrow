#!/bin/bash
set -ex

export CONFIG_PATH=/users/noobjc/demikernel/server.yaml
export MTU=1500
export MSS=1500
export LIBOS=catnip
export LD_LIBRARY_PATH=/root/lib/x86_64-linux-gnu

./main --server 10.10.1.2 3000
