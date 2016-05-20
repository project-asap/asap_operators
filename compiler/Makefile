##
## Python compilation/code generation Rules
##

imtests=tfidf kmeans tfidf_and_kmeans
omtests=tfidf_then_kmeans
exes=$(omtests) $(imtests)
#cpps=$(patsubst %, %.cpp, $(imtests))
#jsons=$(patsubst %, %.json, $(exes))

PY=python
COMPILER=jsontocpp.py
OPLIB=SwanMaterialised.json

.SILENT:

%.dir: %.json
	@ echo " #####   Compiling Workflow $<   #####"
	$(PY) $(COMPILER) -i $< -l $(OPLIB) 
	@ echo " #####   Compiling Generated Swan $@ #####"
	@ make `ls -1 $*.dir/*.cpp | sed -e 's/.cpp//'`

## Test Rules
test_tfidf: test_%: tfidf.dir
	@ echo " #####   Executing Workflow $*   #####"
	@ $*.dir/$*
	@if cmp tfidf_output.arff tfidf_output.arff.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_kmeans: test_%: kmeans.dir
	@ echo " #####   Executing Workflow $*   #####"
	@ $*.dir/$*
	@if cmp kmeans_output.txt kmeans_output.txt.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_tfidf_and_kmeans: test_%: tfidf_and_kmeans.dir
	@ echo " #####   Executing Workflow $*   #####"
	@ $*.dir/$*
	@if cmp kmeans_output.txt tfidf_and_kmeans_output.txt.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi

test_tfidf_then_kmeans: test_%: tfidf_then_kmeans.dir
	if [ -a $*.dir/tfidf_kmeans ] ; \
	then \
		echo " #####   Executing Optimised Version of Workflow tfidf_kmeans   #####" ; \
		$*.dir/tfidf_kmeans ; \
	else \
		echo " #####   Executing Workflow $* #####" ; \
		$*.dir/tfidf ; \
		$*.dir/kmeans ; \
	fi;
	@ echo " #####   Execution of Workflow $* complete  #####"
	@if cmp kmeans_output.txt tfidf_then_kmeans_output.txt.good ; then echo "SUCCESS -- Output compared successfully" ; else echo "FAILURE -- Output deviates from reference" ; fi
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

%: %.o

.PHONY: clean
clean:
	rm -fr $(patsubst %,%.dir,$(exes))

test:
	@ echo "_________________________________________________________________________________"
	@ make tfidf.dir
	@ make test_tfidf
	@ echo "_________________________________________________________________________________"
	@ make kmeans.dir
	@ make test_kmeans
	@ echo "_________________________________________________________________________________"
	@ make tfidf_and_kmeans.dir
	@ make test_tfidf_and_kmeans
	@ echo "_________________________________________________________________________________"
	@ make tfidf_then_kmeans.dir
	@ make test_tfidf_then_kmeans
	@ echo "_________________________________________________________________________________"
