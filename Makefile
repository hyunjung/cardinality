CC = g++
FLAGS = -fPIC -O2 -g
LIBS = -ldl -pthread
OBJS = objs/Tools.o objs/clientCom.o objs/clientIndex.o objs/clientHelper.o objs/index.so

MYFLAGS = -Wall -I/u/hyunjung/boost_1_42_0
MYOBJS = objs/Operator.o objs/Scan.o objs/Join.o objs/SeqScan.o objs/IndexScan.o objs/NLJoin.o objs/NBJoin.o objs/Remote.o objs/Union.o objs/PartitionStats.o objs/client.o objs/Server.o objs/optimizer.o
MYLIBS = lib/thirdparty/libboost_serialization.a lib/thirdparty/libboost_system.a lib/thirdparty/libboost_iostreams.a lib/thirdparty/libboost_thread.a lib/thirdparty/libboost_filesystem.a lib/thirdparty/libtcmalloc_minimal.a lib/thirdparty/rawmemchr.os

all: objs/mainClient objs/mainSlaveClient

objs/Client.so: $(MYOBJS)
	$(CC) $(FLAGS) -shared $(MYOBJS) $(MYLIBS) -o $@

-include $(MYOBJS:.o=.d)

objs/%.d: client/%.cpp
	@set -e; \
	 rm -f $@; \
	 $(CC) $(MYFLAGS) -MM -MT $(@:.d=.o) $< > $@.$$$$; \
	 sed 's,\($*\)\.o[ :]*,\1.o $@: ,g' $@.$$$$ > $@; \
	 rm -f $@.$$$$

$(MYOBJS): objs/%.o:
	$(CC) $(FLAGS) $(MYFLAGS) -c $< -o $@

zip: objs/Client.so
	strip objs/Client.so -o submit/`date -u +%F-%H%M`-`git log -1 --pretty=format:%h`.so
	zip submit/`date -u +%F-%H%M`-`git log -1 --pretty=format:%h`.zip Makefile client/*

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
