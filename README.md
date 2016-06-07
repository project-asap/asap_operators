# Repository Contents
==============

The repository contains:
* An implementation of the workflow compiler for WP2. 
* Implementations of the Swan operators defined within the workflows.  These include:
-	TF-IDF
-	k-means

## Compiler
--------------

The compiler, implemented in Python, translates a high level workflow description in json to swan codes.  For detailed instructions on how to compile workflows to executables see compiler/README.  The main component files are:
- jsontocpp.py - The compiler module
- SwanMaterialised.json - The operators library (materialised operators).
- *.json - Sample workflow descriptions.
- README - Instructions on how to build, and execute workflows.

## Operators
--------------

The TF-IDF algorithm determines the significance of words within a document or corpus.  It is based on the frequency of a word within a document relative to the words occurence in other documents.  Alternative implementations are provided based on choice of associative container.  These include:

- An implementation using an unordered map for the associative container is at *tfidf/tfidf.cpp*. To build and run:
    - cd tfidf
    - make tfidf_std
    - make test

- An Implementation using lists for the containers is at *src/tfidf_list_list*.
    - cd src
    - make tfidf_list_list
    - make test_tfidf_list_list

K-Means algorithms partitions a dataset into related clusters based on creating vector representations for data points.  An implementation accepting arff input format files is at *kmeans/kmeans.arff*. 

- To build and run:
    - cd kmeans
    - make kmeans_arff
    - make test

- To build and run tests for all the compiler and operators,  from this directory (top level) do:
    - make clean
    - make -C compiler test
    - make -C kmeans test
    - make -C tfidf test
    - make -C src test

## Unit Tests
----------

Makefile targets contain operations which perform regression testing for operators.  When a test is executed, a comparison is made between the output generated a 'good' version of the output currently stored.
- For example, executing:
    - cd src
    - make test_tfidf_list_umap

- will result in the following output file which contains the output from running tfidf_list_map:
    - test_tfidf_list_umap.txt

- and the Makefile compares this file against:
    - test_tfidf_list_umap.good 

to ensure there has been no regression bugs introduced in later versions of the source codes.
   
- Running make with any of the test targets will trigger unit tests. For example:

   - cd src
   - make test

- Unit tests are described in the file TESTING.md

## Direct invocation and argument options
--------------------------------------

### kmeans
------
Arguments can be supplied to a direct invocation of kmeans.  An example of a direct invocation command line is:
 - ./kmeans_arff -c 2 -i test.arff -o kmeans_results.txt

And the possible arguments are:
 - d \- to force a (slower) dense computation
 - m \- to set maximum iterations for the algorithm
 - c \- to set the number of clusters to kmeans
 - i \- to specify the input file
 - o \- to speccify the output file
 
### tfidf
-----
Arguments can be supplied to a direct invocation of tfidf.  An example of a direct invocation command line is:
 - ./tfidf_std -d test -o tfidf_results.txt

And the possible arguments are:
 - d \- to specify the input file
 - o \- to speccify the output file
