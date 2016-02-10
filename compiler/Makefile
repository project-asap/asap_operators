##
## Python compilation/code generation Rules
##

imtests=tfidf kmeans tfidf_and_kmeans
omtests=tfidf_then_kmeans
exes=$(omtests) $(imtests)
cpps=$(patsubst %, %.cpp, $(imtests))
jsons=$(patsubst %, %.json, $(exes))

PY=python
COMPILER=jsontocpp.py
OPLIB=SwanMaterialised.json

.SILENT:

$(cpps): %.cpp: %.json
	@ echo " #####   Compiling Workflow $<   #####"
	$(PY) $(COMPILER) -i $< -l $(OPLIB) 
	@ echo " #####   Compiling Generated Swan $@ #####"
	@ make $*

tfidf_then_kmeans.cpp: tfidf.cpp kmeans.cpp
	@ echo "11111"


## Test Rules

test_%: %.cpp
	@ echo " #####   Executing Workflow $*   #####"
	@ $*

test_tfidf_then_kmeans: tfidf kmeans
	@ echo " #####   Executing Workflow $@   #####"
	@ tfidf
	@ kmeans
	@ echo " #####   Execution of Workflow $@ complete  #####"



##
## Swan/C++ compilation Rules
##


INCLUDE_FILES=traits.h dense_vector.h sparse_vector.h vector_ops.h kmeans.h attributes.h memory.h utils.h data_set.h arff.h normalize.h word_bank.h word_count.h io.h
INCLUDE=$(patsubst %, ../../include/asap/%, $(INCLUDE_FILES))

# OBJ=$(patsubst %, %.o, $(tests))
OPT += 

CXX=icpc
CC=icpc
CXXFLAGS+=-O3 $(OPT) -g -std=c++11 -I. -I.. -DTIMING
CXXFLAGS += -I../../cilkpub_v105/include -I../include
LDFLAGS+=-g -std=c++11 -lrt $(OPT)

%.o: %.cpp $(INCLUDE)

tfidf_then_kmeans: tfidf kmeans
	@ echo "#### tfidf and kmeans have been built separately ####"

%: %.o


# .PHONY: clean

clean:
	rm -f $(exes)
	rm -f $(cpps)


all:
	@ make clean
	@ echo "_________________________________________________________________________________"
	@ make tfidf
	@ make test_tfidf
	@ echo "_________________________________________________________________________________"
	@ make kmeans
	@ make test_kmeans
	@ echo "_________________________________________________________________________________"
	@ make tfidf_and_kmeans
	@ make test_tfidf_and_kmeans
	@ echo "_________________________________________________________________________________"
	@ make clean
	@ make tfidf_then_kmeans
	@ make test_tfidf_then_kmeans
	@ echo "_________________________________________________________________________________"

