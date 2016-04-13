tests=t_dense_vector t_fatal t_arff_read

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h
INCLUDE=$(patsubst %, ../include/asap/%, $(INCLUDE_FILES))

CXX=icpc
CC=icpc
CXXFLAGS+=-O0 -g -std=c++11 -I.
CXXFLAGS += -I../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11

all: $(tests)

t_dense_vector: t_dense_vector.o

t_dense_vector.o: t_dense_vector.cpp $(INCLUDE) 

t_fatal: t_fatal.o
t_fatal.o: t_fatal.cpp $(INCLUDE)

t_arff_read: t_arff_read.o
t_arff_read.o: t_arff_read.cpp $(INCLUDE)

clean:
	rm -fr $(tests)

