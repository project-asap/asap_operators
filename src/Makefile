targets=kmeans tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc
tfidf_tests=tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc
tests=$(patsubst %, test_%, $(targets))

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h
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

