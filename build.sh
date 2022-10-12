#!/bin/bash
set -ex

cp -r /root/include/demi /usr/local/include
g++ -O3 -larrow -ldemikernel code.cc -o code
