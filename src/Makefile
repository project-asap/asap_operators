tests=kmeans tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc
tfidf_tests=tfidf_list tfidf_map tfidf_list_inplace tfidf_list_list tfidf_list_umap tfidf_kmeans wc

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h
INCLUDE=$(patsubst %, ../include/asap/%, $(INCLUDE_FILES))

OBJ=$(patsubst %, %.o, $(tests))

OPT += 

CXX=icpc
CC=icpc
CXXFLAGS+=-O3 $(OPT) -g -std=c++11 -I. -I.. -DTIMING
CXXFLAGS += -I../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11 -lrt $(OPT)

all: $(tests)

%.o: %.cpp $(INCLUDE)

%: %.o

test: $(tfidf_tests)
	@ $< -i testdir -o tfidf_results.txt

test_kmeans:	kmeans
	@ $< -i test.arff -o kmeans_results.txt

.PHONY: clean
clean:
	rm -f $(tests) $(OBJ)

