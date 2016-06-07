### Descriptions and examples of individual Unit Tests

Below is descriptions for 7 unit tests and how to run them:

1. Unit test for  "tfidf -> Compiler -> execution"
  - Command: cd $INSTALL_DIR/compiler ; make test_tfidf
  - This unit test reads a simple user workflow description (tfidf.json) describing the operators, input and output datasets for calculating tfidf (term frequency inverse document frequency) values for each word in a corpus of documents.  The workflow compiler will generate Swan code based on available operators as defined in the operators library (SwanMaterialised.json).  The code will be compiled and executed to produce ARFF output in file "tfidf_output.arff".  This output is compared against a 'good' version and any deviances are reported.

2. Unit test for  "kmeans -> Compiler -> execution"
  - Command: cd $INSTALL_DIR/compiler ; make test_kmeans
  - This unit test reads a simple user workflow description (kmeans.json) describing the operators, input and output datasets for calculating k-means clustering from an ARFF text files containing TF-IDF values for words in a corpus of documents.  The workflow compiler will generate Swan code based on available operators as defined in the operators library (SwanMaterialised.json).  The code will be compiled and executed to produce a text output file in "kmeans_output.txt".  This output is compared against a 'good' version and any deviances are reported.

3. Unit test for  "tfidf and kmeans -> Compiler -> execution"
  - Command: cd $INSTALL_DIR/compiler ; make test_tfidf_and_kmeans
  - This unit test reads a workflow description which contains a combined in-memory description of TF-IDF and K-means together.  No output file is specified to TF-IDF and no input file is specified to K-means as the intermediate data is reatained in-memory.  The code will be compiled and executed to produce a text output file in "tfidf_and_kmeans_output.txt".  This output is compared against a 'good' version and any deviances are reported.  Timings gained from this benchmark are indicative of benefits of potential in-memory workflow optimisations.

4. Unit test for  "tfidf then kmeans -> Compiler -> execution"
  - Command: cd $INSTALL_DIR/compiler ; make test_tfidf_then_kmeans
  - This unit tests reads a workflow description which contains a 2-phased transformation of a text dataset using TF-IDF 'followed' by K-means.  In contrast to "tfidf and kmeans", TF-IDF produces an output ARFF file which is specified as the input to K-means, when they produces the clustering results in text file "tfidf_then_kmeans_output.txt".  As it is not benefiting from in-memory optimisations it is expected to take a longer time to execute.

5. Unit test for "tfidf standalone benchmark"
  - Command: cd $INSTALL_DIR/src ; make test_tfidf_list_umap
  - This unit test compiles a version of TF-IDF which uses a list and an unordered_map datastructure, for benchmark comparisons against other versions of TF-IDF which use for example ordered map data structures.  On execution it produces output in ARFF format (test_tfidf_list_umap.txt) which is compared against a 'good' version and any deviances are reported.

6. Unit test for "kmeans standalone benchmark"
  - Command: cd $INSTALL_DIR/src ; make test_kmeans
  - This unit test compiles a standalone version of K-means using Swan for benchmark comparisons.  It produces output in text format (test_kmeans.txt) which is compared against a 'good' version and any deviances are reported.

7. Unit test for "wc standalone benchmark"
  - Command: cd $INSTALL_DIR/src ; make test_wc
  - This unit test compiles a standalone version of Word Count using Swan for benchmark comparisons.  It produces output in text file listing of resulting word counts for a document (test_wc.txt) which is compared against a 'good' version and any deviances are reported.
