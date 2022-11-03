#!/bin/bash
set -ex

export CONFIG_PATH=/users/noobjc/demikernel/$1.yaml
export MTU=1500
export MSS=1500
export LIBOS=$3
export LD_LIBRARY_PATH=/root/lib:/root/lib/x86_64-linux-gnu

make
./$2 --$1 10.10.1.2 3000
