default:
	cp -r /root/include/demi /usr/local/include
	g++ -O3 code.cc -larrow -ldemikernel -o code
