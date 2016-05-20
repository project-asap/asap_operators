Repository Contents
==============

The repository contains:
* An implementation of the workflow compiler for WP2. 
* Implementations of the Swan operators defined within the workflows.  These include:
-	TF-IDF
-	k-means

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
    cd tfidf
    make tfidf_std
    make test
- An Implementation using lists for the containers is at *src/tfidf_list_list*.
    cd src
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

Unit Tests
----------

Makefile targets contain operations which perform regression testing for operators.  When a test is executed, a comparison is made between the output generated a 'good' version of the output currently stored.
For example, executing:
    cd src
    make test_tfidf_list_umap

will result in the following output file which contains the output from running tfidf_list_map:
    test_tfidf_list_umap.txt

and the Makefile compares this file against:
    test_tfidf_list_umap.good 

to ensure there has been no regression bugs introduced in later versions of the source codes.
   
Running make with any of the test targets will trigger unit tests. For example:

   cd src
   make test

Below is descriptions for 7 key unit tests and how to run them:

1) Unit test for  "tfidf -> Compiler -> execution"
Command: cd $INSTALL_DIR/compiler ; make test_tfidf
This unit test reads a simple user workflow description (tfidf.json) describing the operators, input and output datasets for calculating tfidf (term frequency inverse document frequency) values for each word in a corpus of documents.  The workflow compiler will generate Swan code based on available operators as defined in the operators library (SwanMaterialised.json).  The code will be compiled and executed to produce ARFF output in file "tfidf_output.arff".  This output is compared against a 'good' version and any deviances are reported.

2) Unit test for  "kmeans -> Compiler -> execution"
Command: cd $INSTALL_DIR/compiler ; make test_kmeans
This unit test reads a simple user workflow description (kmeans.json) describing the operators, input and output datasets for calculating k-means clustering from an ARFF text files containing TF-IDF values for words in a corpus of documents.  The workflow compiler will generate Swan code based on available operators as defined in the operators library (SwanMaterialised.json).  The code will be compiled and executed to produce a text output file in "kmeans_output.txt".  This output is compared against a 'good' version and any deviances are reported.

3) Unit test for  "tfidf and kmeans -> Compiler -> execution"
Command: cd $INSTALL_DIR/compiler ; make test_tfidf_and_kmeans
This unit test reads a workflow description which contains a combined in-memory description of TF-IDF and K-means together.  No output file is specified to TF-IDF and no input file is specified to K-means as the intermediate data is reatained in-memory.  The code will be compiled and executed to produce a text output file in "tfidf_and_kmeans_output.txt".  This output is compared against a 'good' version and any deviances are reported.  Timings gained from this benchmark are indicative of benefits of potential in-memory workflow optimisations.

4) Unit test for  "tfidf then kmeans -> Compiler -> execution"
Command: cd $INSTALL_DIR/compiler ; make test_tfidf_then_kmeans
This unit tests reads a workflow description which contains a 2-phased transformation of a text dataset using TF-IDF 'followed' by K-means.  In contrast to "tfidf and kmeans", TF-IDF produces an output ARFF file which is specified as the input to K-means, when they produces the clustering results in text file "tfidf_then_kmeans_output.txt".  As it isn't benefiting from in-memory optimisations it is expected to take a longer time to execute.

5) Unit test for "tfidf standalone benchmark"
Command: cd $INSTALL_DIR/src ; make test_tfidf_list_umap
This unit test compiles a version of TF-IDF which uses a list and an unordered_map datastructure, for benchmark comparisons against other versions of TF-IDF which use for example ordered map data structures.  On execution it produces output in ARFF format (test_tfidf_list_umap.txt) which is compared against a 'good' version and any deviances are reported.

6) Unit test for "kmeans standalone benchmark"
Command: cd $INSTALL_DIR/src ; make test_kmeans
This unit test compiles a standalone version of K-means using Swan for benchmark comparisons.  It produces output in text format (test_kmeans.txt) which is compared against a 'good' version and any deviances are reported.

7) Unit test for "wc standalone benchmark"
Command: cd $INSTALL_DIR/src ; make test_wc
This unit test compiles a standalone version of Word Count using Swan for benchmark comparisons.  It produces output in text file listing of resulting word counts for a document (test_wc.txt) which is compared against a 'good' version and any deviances are reported.


Direct invocation and argument options
--------------------------------------

kmeans
------
Arguments can be supplied to a direct invocation of kmeans.  An example of a direct invocation command line is:
    ./kmeans_arff -c 2 -i test.arff -o kmeans_results.txt

And the possible arguments are:
 - d \- to force a (slower) dense computation
 - m \- to set maximum iterations for the algorithm
 - c \- to set the number of clusters to kmeans
 - i \- to specify the input file
 - o \- to speccify the output file
 
tfidf
-----
Arguments can be supplied to a direct invocation of tfidf.  An example of a direct invocation command line is:
    ./tfidf_std -d test -o tfidf_results.txt

And the possible arguments are:
 - d \- to specify the input file
 - o \- to speccify the output file
