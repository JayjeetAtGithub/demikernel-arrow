default:
	cp -r /root/include/demi /usr/local/include
	cp -r /root/lib/x86_64-linux-gnu/* /usr/local/lib
	g++ -O3 code.cc -larrow -ldemikernel -o code
