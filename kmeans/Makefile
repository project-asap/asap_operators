benchmark=kmeans_arff

OPT+=

CXX=icpc
CXXFLAGS+=-O3 $(OPT) -I.. -std=c++11
# CXXFLAGS+=-std=c++11
LDFLAGS=-lrt $(OPT)

all: $(benchmark)

kmeans_arff: kmeans_arff.cpp 
	$(CXX) $(CXXFLAGS) -DTIMING $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)
