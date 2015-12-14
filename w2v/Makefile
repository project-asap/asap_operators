# benchmark=word2vec word2vec_dbg word2vec_timed
benchmark=word2vec word2vec_dbg origw2v

CXX=icc
OPTFLAGS+=-O3
CXXFLAGS+=-std=c++11 -I. -DASAP
CXXFLAGS+= -I../cilkpub_v105/include -I..
CXXFLAGS+= -march=native -Wall -funroll-loops
CXXFLAGSDEBUG+=-g $(CXXFLAGS)
LDFLAGS += -lrt -lpthread


all: $(benchmark)

word2vec: word2vec.cpp
	$(CXX) $(OPTFLAGS) $(CXXFLAGS) -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

word2vec_dbg: word2vec.cpp
	$(CXX) -O0 $(CXXFLAGSDEBUG) -DASAP $^ -o $@ $(LDFLAGS)

origw2v: origw2v.cpp
	$(CXX) $(OPTFLAGS) $(CXXFLAGS) -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

origw2v_dbg: origw2v.cpp
	$(CXX) -O0 $(CXXFLAGSDEBUG) -DASAP $^ -o $@ $(LDFLAGS)

word2vec_timed: word2vec.cpp
	$(CXX) $(OPTFLAGS) $(CXXFLAGS) -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)
