include ../Config.mk

# benchmark=wc wc_rec wc_seq wc_trace wc_tls wc_p2 wc_std
benchmark=tfidf tfidf_dbg

CXX=icc
#CXXFLAGS+=-O3 -std=c++11 -I. -I../../include -I../../../../phoenix++-1.0/include -gcc-name=/usr/local/bin/gcc
#CXXFLAGS+=-O3 -std=c++11 -I. -I../../include -I../../../../phoenix++-1.0/include 
CXXFLAGS+=-O3 -std=c++11 -I. -I../../include -I../../../../phoenix++-1.0/include
CXXFLAGSDEBUG+=-g -std=c++11 -I. -I../../include -I../../../../phoenix++-1.0/include
# CXXFLAGS+=-O3 -I. -I../../include -I../../../../phoenix++-1.0/include 
# #CXXFLAGS+=-O0 -g -std=c++11 -I. -I../../include -I../../../../phoenix++-1.0/include
#
CXXFLAGS += -I../cilkpub_v105/include -I..
CXXFLAGSDEBUG += -I../cilkpub_v105/include -I..

all: $(benchmark)

tfidf_seq_likwid: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DSEQUENTIAL=1 -DPMC=1 -DLIKWID_PERFMON=1 $^ -o $@ $(LDFLAGS) -llikwid

tfidf_seq: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DSEQUENTIAL=1 $^ -o $@ $(LDFLAGS)           #-DSTD_UNORDERED_MAP=1 added by Mahwish

tfidf_trace: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DTRACING=1 $^ -o $@ $(LDFLAGS)

tfidf_tls: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DTLSREDUCE=1 $^ -o $@ $(LDFLAGS)

tfidf_master: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DMASTER=1 $^ -o $@ $(LDFLAGS)

tfidf_master_ph: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DMASTER=1 -DPHOENIX_MAP=1 $^ -o $@ $(LDFLAGS)

tfidf_ph: tfidf.cpp
        $(CXX) $(CXXFLAGS) -DPHOENIX_MAP=1 $^ -o $@ $(LDFLAGS)
	
tfidf_p2: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DP2_UNORDERED_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)

tfidf_std: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DSTD_UNORDERED_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)

tfidf_dbg: tfidf.cpp
	$(CXX) $(CXXFLAGSDEBUG) -DASAP $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)
