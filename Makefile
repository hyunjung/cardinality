CC = g++
FLAGS = -fPIC
LIBS = -ldl -pthread -lboost_serialization-mt
OBJS = objs/Tools.o objs/clientCom.o objs/clientIndex.o objs/clientHelper.o objs/index.so

FLAGS := $(FLAGS) -g -O2
MYOBJS = objs/Client.o objs/Operator.o objs/Scan.o objs/Join.o objs/SeqScan.o objs/NLJoin.o

all: objs/mainClient objs/mainSlaveClient

objs/Client.so: $(MYOBJS)
	$(CC) $(FLAGS) -shared $(MYOBJS) -o $@

$(MYOBJS): objs/%.o: client/%.cpp $(patsubst objs/%, client/%, $(MYOBJS:.o=.h))
	$(CC) $(FLAGS) -Wall -Weffc++ -Wold-style-cast -c $< -o $@

buildLib:objs/SimpleClient.so objs/TrivialClient.so $(OBJS) 
buildBench: objs/mainSimpleClient objs/mainSlaveSimpleClient objs/mainTrivialClient objs/mainSlaveTrivialClient
 
test:objs/TestBench objs/TestTools runUnitTest

objs/index.so:
	$(CC) $(FLAGS) -shared lib/index/index.cpp -o objs/index.so

objs/%.so: client/%.c include/client.h
	$(CC) $(FLAGS) -shared $< -o $@

objs/%.so: client/%.cpp include/client.h
	$(CC) $(FLAGS) -shared $< -o $@

objs/mainSlave%: objs/%.so bench/mainClient.cpp $(OBJS) bench/clientHelper.h 
	$(CC) $(FLAGS) bench/mainClient.cpp $(OBJS) $< $(LIBS) -o $@

objs/main%: objs/%.so  bench/mainMaster.cpp $(OBJS) bench/clientHelper.h
	$(CC) $(FLAGS) bench/mainMaster.cpp $(OBJS) $< $(LIBS) -o $@

objs/TestObj%.o: unitTest/Test%.cpp
	$(CC) $(FLAGS) -c $< -o $@

objs/Test%: objs/TestObj%.o bench/clientHelper.o objs/Tools.o objs/index.so
	$(CC) $(FLAGS) unitTest/Runner.cpp $< bench/clientHelper.o objs/index.so objs/Tools.o $(LIBS) -lcppunit -o $@ 

objs/%.o: bench/%.cpp bench/%.h
	$(CC) $(FLAGS) -c $< -o $@

objs/%.o: lib/%.cpp lib/%.h
	$(CC) $(FLAGS) -c $< -o $@

runUnitTest:
	./objs/TestBench
	./objs/TestTools

clean:
	rm -f objs/*
	rm -f logs/*
