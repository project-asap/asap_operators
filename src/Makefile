targets=kmeans tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc tfidf_1gram tfidf_2gram tfidf_3gram tfidf_sfxa
tfidf_tests=tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc tfidf_mix_malloc tfidf_mix_prealloc tfidf_mix_managed
tests=$(patsubst %, test_%, $(targets))

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h hashtable.h
INCLUDE=$(patsubst %, ../include/asap/%, $(INCLUDE_FILES))

OBJ=$(patsubst %, %.o, $(targets))

OPT += 

CXX=icpc
CC=icpc
CXXFLAGS+=-O3 $(OPT) -g -std=c++11 -I. -I.. -DTIMING
CXXFLAGS += -I../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11 -lrt $(OPT)

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
	./$< -i testdir -o tfidf_results.txt -s

test_tfidf_list_inplace: tfidf_list_inplace FORCE
	./$< -i testdir -o tfidf_results.txt

test_wc: wc FORCE
	./wc -i testdir -o wc_results.txt
	@if cmp wc_results.txt testdir_wc.txt ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_kmeans: kmeans FORCE
	@ ./$< -i test.arff -o kmeans_results.txt

test_tfidf_kmeans: tfidf_kmeans FORCE
	@ ./$< -i testdir -o kmeans_results.txt

test_%: % FORCE
	./$< -i testdir -o tfidf_results.txt
	@if cmp tfidf_results.txt testdir_tfidf.txt ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

FORCE:

.PHONY: clean
clean:
	rm -f $(targets) $(OBJ) wc_results.txt kmeans_results.txt tfidf_results.txt

