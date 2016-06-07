benchmark=kmeans_arff

OPT+=

CXX=icpc
CXXFLAGS+=-O3 $(OPT) -I.. -std=c++11
# CXXFLAGS+=-std=c++11
LDFLAGS=-lrt $(OPT)

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
