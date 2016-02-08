##
## Python compilation/code generation
##

tests=tfidf kmeans tfidf_then_kmeans tfidf_and_kmeans
py_objects=tfidf kmeans tfidf_and_kmeans
PEXES=$(patsubst %, %_gen.cpp, $(tests))

PY=python
COMPILER=jsontocpp.py
OPLIB=SwanMaterialised.json

$(py_objects): %: %.json
	$(PY) $(COMPILER) -i $< -l $(OPLIB) 
	@ make $@_gen
	@ echo " #####   Running $@   #####"
	@ $@_gen 


tfidf_then_kmeans: %: %.json
	@ echo $@
	$(PY) $(COMPILER) -i $< -l $(OPLIB) 
	@ make tfidf_gen
	@ make kmeans_gen
	@ echo " #####   Running TF/IDF   #####"
	@ tfidf_gen 
	@ echo " #####   Running K-means   #####"
	@ kmeans_gen 

FORCE:

.PHONY:	clean


##
## Swan/C++ compilation
##

swantests=tfidf_gen kmeans_gen tfidf_and_kmeans_gen

INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h
INCLUDE=$(patsubst %, ../../include/asap/%, $(INCLUDE_FILES))

OBJ=$(patsubst %, %.o, $(tests))
OPT += 

CXX=icpc
CC=icpc
CXXFLAGS+=-O3 $(OPT) -g -std=c++11 -I. -I.. -DTIMING
CXXFLAGS += -I../../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11 -lrt $(OPT)

# all: $(tests) $(swantests)

%.o: %.cpp $(INCLUDE)

%: %.o

.PHONY: clean

clean:
	rm -f $(swantests) $(OBJ)
	rm -f $(PEXES)

