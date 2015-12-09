benchmark=word2vec word2vec_dbg word2vec_timed

CXX=icc
CXXFLAGS+=-O3 -std=c++11 -I. -DASAP
CXXFLAGS += -I../cilkpub_v105/include -I..
CXXFLAGS+= -O3 -march=native -Wall -funroll-loops
CXXFLAGSDEBUG+=-g $(CXXFLAGS)
LDFLAGS += -lrt

benchmark=word2vec

all: $(benchmark)

word2vec: word2vec.c
	$(CXX) $(CXXFLAGS) -DASAP $^ -o $@ $(LDFLAGS)

word2vec_dbg: word2vec.c
	$(CXX) $(CXXFLAGSDEBUG) -DASAP $^ -o $@ $(LDFLAGS)

word2vec_timed: word2vec.cpp
	$(CXX) $(CXXFLAGS) -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)
