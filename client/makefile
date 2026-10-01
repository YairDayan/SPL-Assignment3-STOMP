# client/makefile
CFLAGS := -c -Wall -Weffc++ -g -std=c++11 -Iinclude
LDFLAGS := -lboost_system -lpthread

all: StompEMIClient

StompEMIClient: bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o
	g++ -o bin/StompEMIClient bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o $(LDFLAGS)

bin/ConnectionHandler.o: src/ConnectionHandler.cpp include/ConnectionHandler.h | bin
	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

bin/event.o: src/event.cpp include/event.h | bin
	g++ $(CFLAGS) -o bin/event.o src/event.cpp

bin/StompProtocol.o: src/StompProtocol.cpp include/StompProtocol.h include/event.h | bin
	g++ $(CFLAGS) -o bin/StompProtocol.o src/StompProtocol.cpp

bin/StompClient.o: src/StompClient.cpp include/StompProtocol.h include/ConnectionHandler.h include/event.h | bin
	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

bin:
	mkdir -p bin

.PHONY: clean
clean:
	rm -f bin/*
