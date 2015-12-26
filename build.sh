#!/bin/bash

# replica.cpp rfollower.cpp rfollower.h rleader.cpp rleader.h SimpleConf.cpp SimpleConf.h SimpleConnPool.cpp SimpleConnPool.h SimpleHashtab.cpp SimpleHashtab.h thr_pool.c thr_pool.h utils.cpp utils.h
#g++ -g -W -Wall -o ./replica replica.cpp rfollower.cpp rleader.cpp SimpleConf.cpp SimpleConnPool.cpp SimpleHashtab.cpp thr_pool.c utils.cpp HSearchHelper.cpp -I/usr/local/include -L/usr/local/lib -lzookeeper_mt -lpthread -lrt
g++ -g -W -Wall -o ./replica thr_pool.o replica.cpp rfollower.cpp rleader.cpp SimpleConf.cpp SimpleConnPool.cpp SimpleHashtab.cpp utils.cpp HSearchHelper.cpp -I/usr/local/include -L/usr/local/lib -lzookeeper_mt -lpthread -lrt
