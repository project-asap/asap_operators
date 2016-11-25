targets=kmeans tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc tfidf_1gram tfidf_2gram tfidf_3gram tfidf_best
tfidf_tests=tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc tfidf_mix_malloc tfidf_mix_prealloc tfidf_mix_managed
tests=$(patsubst %, test_%, $(targets))

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h hashtable.h
INCLUDE=$(patsubst %, ../include/asap/%, $(INCLUDE_FILES))

OBJ=$(patsubst %, %.o, $(targets))

OPT += 

#CXX=icpc
#CC=icpc
# The following block of defines is concerned with setting up the swan compiler.
# Define SWAN_PATH SWAN_RT and SWAN_LLVM on the command line to customize
SWAN_PATH ?= $(HOME)/research/asap
SWAN_RT ?= $(SWAN_PATH)/intel-cilk-runtime-hpdc02
SWAN_LLVM ?= $(SWAN_PATH)/swan-cilkplus-llvm/build_hpdc02/Debug+Asserts/bin
#CXX=$(SWAN_LLVM)/clang++
#CC=$(SWAN_LLVM)/clang
CXX=g++-mp-7
CC=gcc-mp-7
CXXFLAGS += -fcilkplus
#CXXFLAGS += -I$(SWAN_RT)/include
#CXXFLAGS += -L$(SWAN_RT)/.libs
#LDFLAGS += -lnuma -ldl -lrt

CXXFLAGS+=-O3 $(OPT) -g -std=c++11 -I. -I.. -DTIMING
CXXFLAGS += -I../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11 $(OPT)
LDFLAGS+=-fcilkplus

all: $(targets)

%.o: %.cpp $(INCLUDE)

tfidf_1gram: tfidf_1gram.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_1gram.o: tfidf_ngram.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DN_IN_NGRAM=1 -c $< -o $@

tfidf_2gram: tfidf_2gram.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_2gram.o: tfidf_ngram.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DN_IN_NGRAM=2 -c $< -o $@

tfidf_3gram: tfidf_3gram.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_3gram.o: tfidf_ngram.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DN_IN_NGRAM=3 -c $< -o $@



tfidf_mix_managed: tfidf_mix_managed.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_mix_malloc: tfidf_mix_malloc.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_mix_prealloc: tfidf_mix_prealloc.o
	$(CXX) $(LDFLAGS) $< -o $@

tfidf_mix_managed.o: tfidf_mix.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DMEM=2 -c $< -o $@

tfidf_mix_malloc.o: tfidf_mix.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DMEM=1 -c $< -o $@

tfidf_mix_prealloc.o: tfidf_mix.cpp $(INCLUDE)
	$(CXX) $(CXXFLAGS) -DMEM=0 -c $< -o $@

%: %.o

#$(tests): %.o

test: $(targets) $(tests) FORCE

test_tfidf_list_umap: tfidf_list_umap FORCE
	./$< -i testdir -o $@.txt
	@if cmp $@.txt $@.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_wc: wc FORCE
	./wc -i testdir/file3 -o $@.txt
	@if cmp $@.txt $@.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_kmeans: kmeans FORCE
	@ ./$< -c 2 -i test.arff -o $@.txt
	@if [[ `../utils/checkSimilar.py -i $@.txt -j $@.good` -eq 0 ]] ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_tfidf_kmeans: tfidf_kmeans FORCE
	@ ./$< -c 2 -i testdir -o $@.txt
	@if [[ `../utils/checkSimilar.py -i $@.txt -j $@.good` -eq 0 ]] ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi
	@if cmp $@.txt $@.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_%: % FORCE
	./$< -i testdir -o $@.txt
	@if cmp $@.txt $@.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

FORCE:

.PHONY: clean
clean:
	rm -f $(targets) $(OBJ) test_*.txt

