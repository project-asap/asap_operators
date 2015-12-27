benchmark=tfidf tfidf_dbg tfidf_timed tfidf_std tfidf_kmeans tfidf_kmeans_dbg

CXX=icc
CXXFLAGS+=-O3 -std=c++11 -I. -DASAP -DTIMING
CXXFLAGS += -I../cilkpub_v105/include -I.. 
CXXFLAGSDEBUG+=-g $(CXXFLAGS) 
DEBUG_FLAGS=-O0 -g -std=c++11 -I. -DASAP -DTIMING -I../cilkpub_v105/include -I..
LDFLAGS += -lrt

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
	$(CXX) $(CXXFLAGS) -DPHOENIX_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)
	
tfidf_p2: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DP2_UNORDERED_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)

tfidf_std: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DSTD_UNORDERED_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)

tfidf_map: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DSTD_MAP=1 -DTIMING=1 $^ -o $@ $(LDFLAGS)

tfidf_ph_kmeans: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DPHOENIX_MAP=1 -DTIMING=1 -DKMEANS $^ -o $@ $(LDFLAGS)

tfidf_std_kmeans: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DSTD_UNORDERED_MAP=1 -DTIMING=1 -DKMEANS $^ -o $@ $(LDFLAGS)

tfidf_map_kmeans: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DASAP -DSTD_MAP=1 -DTIMING=1 -DKMEANS $^ -o $@ $(LDFLAGS)

tfidf_dbg: tfidf.cpp
	$(CXX) $(CXXFLAGSDEBUG) -DASAP -O0 $^ -o $@ $(LDFLAGS)

tfidf_timed: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

tfidf_kmeans: tfidf.cpp
	$(CXX) $(CXXFLAGS) -DKMEANS -DTIMING=1 -DASAP $^ -o $@ $(LDFLAGS)

tfidf_kmeans_dbg: tfidf.cpp
	$(CXX) $(DEBUG_FLAGS) -DKMEANS -DASAP -O0 $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)

