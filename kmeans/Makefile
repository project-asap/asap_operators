benchmark=kmeans_arff

CXX=icpc
CXXFLAGS+=-O3 -I.. -std=c++11
# CXXFLAGS+=-std=c++11
LDFLAGS=-lrt

all: $(benchmark)

kmeans_arff: kmeans_arff.cpp 
	$(CXX) $(CXXFLAGS) -DTIMING $^ -o $@ $(LDFLAGS)

clean:
	rm -fr $(benchmark)
