benchmark=tfidf tfidf_dbg tfidf_timed tfidf_std tfidf_kmeans tfidf_kmeans_dbg

#CXX=icc
# The following block of defines is concerned with setting up the swan compiler.
# Define SWAN_PATH SWAN_RT and SWAN_LLVM on the command line to customize
SWAN_PATH ?= $(HOME)/research/asap
SWAN_RT ?= $(SWAN_PATH)/intel-cilk-runtime-hpdc02
SWAN_LLVM ?= $(SWAN_PATH)/swan-cilkplus-llvm/build_hpdc02/Debug+Asserts/bin
CXX=$(SWAN_LLVM)/clang++
CC=$(SWAN_LLVM)/clang
CXXFLAGS += -fcilkplus
CXXFLAGS += -I$(SWAN_RT)/include
CXXFLAGS += -L$(SWAN_RT)/.libs
LDFLAGS += -lnuma -ldl

CXXFLAGS+=-O3 -std=c++11 -I. -DASAP -DTIMING
CXXFLAGS += -I../cilkpub_v105/include -I.. 
CXXFLAGSDEBUG+=-g $(CXXFLAGS) 
DEBUG_FLAGS=-O0 -g -std=c++11 -I. -DASAP -DTIMING -I../cilkpub_v105/include -I..
LDFLAGS += -lrt

# all: $(benchmark)
all: tfidf_std

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

test: tfidf_std FORCE
	./tfidf_std -d test -o tfidf_results.txt
	@if cmp tfidf_results.txt tfidf_results.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi


FORCE:

clean:
	rm -fr $(benchmark)

