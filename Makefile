default:
	cp -r /root/include/demi /usr/local/include
	cp -r /root/lib/x86_64-linux-gnu/* /usr/local/lib
	cp -r /root/lib/* /usr/local/lib
	
	g++ -std=c++17 -O3 src/server.cc -larrow -larrow_dataset -ldemikernel -o main
