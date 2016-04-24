Repository Contents
==============

The repository contains:
* An implementation of the workflow compiler for WP2. 
* Implementations of the Swan operators defined within the workflows.  These include:
-	TF-IDF
-	k-means
-	word2vec

Compiler
--------------

The compiler, implemented in Python, translates a high level workflow description in json to swan codes.  For detailed instructions on how to compile workflows to executables see compiler/README.  The main component files are:
- jsontocpp.py - The compiler module
- SwanMaterialised.json - The operators library (materialised operators).
- *.json - Sample workflow descriptions.
- README - Instructions on how to build, and execute workflows.

Operators
--------------

The TF-IDF algorithm determines the significance of words within a document or corpus.  It is based on the frequency of a word within a document relative to the words occurence in other documents.  Alternative implementations are provided based on choice of associative container.  These include:
- An implementation using an unordered map for the associative container is at *tfidf/tfidf.cpp*. To build and run:
    make tfidf_std
    make test
- An Implementation using lists for the containers is at *src/tfidf_list_list*.
    make tfidf_list_list
    make test_tfidf_list_list

K-Means algorithms partitions a dataset into related clusters based on creating vector representations for data points.  An implementation accepting arff input format files is at *kmeans/kmeans.arff*. To build and run:
    cd kmeans
    make kmeans_arff
    make test

To build and run tests for all the compiler and operators,  from this directory (top level) do:
    make clean
    make -C compiler test
    make -C kmeans test
    make -C tfidf test
    make -C src test
