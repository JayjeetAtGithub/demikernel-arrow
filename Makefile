make init:
	cp -r /root/include/demi /usr/local/include

all:
	g++ -O3 -larrow -ldemikernel code.cc -o code
