benchmark=kmeans_arff

OPT+=

#CXX=icpc
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

CXXFLAGS+=-O3 $(OPT) -I.. -std=c++11
# CXXFLAGS+=-std=c++11
LDFLAGS += -lrt $(OPT)

all: $(benchmark)

kmeans_arff: kmeans_arff.cpp 
	$(CXX) $(CXXFLAGS) -DTIMING $^ -o $@ $(LDFLAGS)

test: kmeans_arff FORCE
	./kmeans_arff -c 2 -i test.arff -o kmeans_results.txt
	@if [[ `../utils/checkSimilar.py -i kmeans_results.txt -j kmeans_results.good` -eq 0 ]] ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

FORCE:

clean:
	rm -fr $(benchmark)

all: kmeans_arff
