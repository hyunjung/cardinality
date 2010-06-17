CC = g++
FLAGS = -fPIC -O2
LIBS = -ldl -lboost_system-mt -lboost_iostreams-mt -lboost_thread-mt -lboost_filesystem-mt -lprotobuf-lite -pthread
OBJS = objs/Tools.o objs/clientCom.o objs/clientIndex.o objs/clientHelper.o objs/index.so

MYFLAGS = -fno-strict-aliasing -Wall -Wno-sign-compare -I.
MYOBJS = \
	objs/Operator.o \
	objs/Project.o \
	objs/Scan.o \
	objs/Join.o \
	objs/SeqScan.o \
	objs/IndexScan.o \
	objs/NLJoin.o \
	objs/NBJoin.o \
	objs/Remote.o \
	objs/Union.o \
	objs/Dummy.o \
	objs/PartStats.o \
	objs/IOManager.o \
	objs/Connection.o \
	objs/client.o

objs/Client.so: $(MYOBJS)
	$(CC) $(FLAGS) -shared $(MYOBJS) -o $@

-include $(MYOBJS:.o=.d)

objs/%.d: client/%.cpp
	@set -e; \
	 rm -f $@; \
	 $(CC) $(MYFLAGS) -MM -MT $(@:.d=.o) $< > $@.$$$$; \
	 sed 's,\($*\)\.o[ :]*,\1.o $@: ,g' $@.$$$$ > $@; \
	 rm -f $@.$$$$

$(MYOBJS): objs/%.o:
	$(CC) $(FLAGS) $(MYFLAGS) -c $< -o $@

buildLib: objs/Client.so $(OBJS)
buildBench: objs/mainClient objs/mainSlaveClient
 
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
