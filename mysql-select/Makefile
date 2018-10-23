CXX=g++
CXXFLAGS=-g3 -std=c++11 -O2
INCLUDE=-I. -I/usr/include/mysql
#LIB=-L/usr/lib/x86_64-linux-gnu -lmysqlclient -lpthread -lz -lm -lrt -ldl
LIB=/usr/local/mysql/lib/libmysqlclient.a -lpthread -lz -lm -lrt -ldl

all:
	${CXX} ${CXXFLAGS} ${INCLUDE} -o select main.c ${LIB}

clean:
	rm select
