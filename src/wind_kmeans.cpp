/* -*-C++-*-
*/
/*
 * Copyright 2016 EU Project ASAP 619706.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <climits>
#include <limits>

#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/cilk_api.h>

#include "asap/utils.h"
#include "asap/imrformat.h"
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/kmeans.h"
#include "asap/normalize.h"

#include <stddefines.h>

#define DEF_NUM_MEANS 8
#define DEF_NUM_RUNS 1

size_t num_clusters;
size_t num_runs;
size_t max_iters;
bool force_dense;
char const * infile = nullptr;
char const * outfile = nullptr;

static void help(char *progname) {
    std::cout << "Usage: " << progname
	      << " [-d] -i <infile> -o <outfile> -c <numclusters> "
	      << " -m <maxiters>\n";
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    num_clusters = DEF_NUM_MEANS;
    num_runs = DEF_NUM_RUNS;
    max_iters = 20; // default
   
#ifndef NOFLAGS
       while ((c = getopt(argc, argv, "c:i:o:m:r:d")) != EOF) {
#else
       while ((c = getopt(argc, argv, "c:m:r:d")) != EOF) {
#endif
         switch (c) {
	        case 'd':
                   force_dense = true;
                   break;
                case 'm':
                   max_iters = atoi(optarg);
	           break;
                case 'c':
                   num_clusters = atoi(optarg);
                   break;
                case 'r':
                   num_runs = atoi(optarg);
                   break;
#ifndef NOFLAGS
        case 'i':
            infile = optarg;
            break;
        case 'o':
            outfile = optarg;
            break;
#endif
        case '?':
            help(argv[0]);
            exit(1);
        }
		         }


    if( num_clusters <= 0 )
        fatal( "Number of clusters must be larger than 0." );
#ifndef NOFLAGS
    if( !infile )
	        fatal( "Input file must be supplied." );
    if( !outfile )
	        fatal( "Output file must be supplied." );
#else
    infile = argv[1];
    outfile = argv[2];
#endif

    std::cerr << "Number of clusters = " << num_clusters << '\n';
    std::cerr << "Input file = " << infile << '\n';
    std::cerr << "Output file = " << outfile << '\n';
}

typedef double real;

#if !VECTORIZED
    // real sq_dist(point const& p) const {
    real sq_dist(const real * d, real * p, int num_dimensions) {
        real sum = 0;
        for (int i = 0; i < num_dimensions; i++) {
            real diff = d[i] - p[i];
            sum += diff * diff;
        }
        return sum;
    }
#endif

int main(int argc, char **argv) {
    struct timespec begin, end;
    struct timespec veryStart, veryEnd;

    srand( time(NULL) );

    get_time( begin );
    get_time( veryStart );

    //read args
    parse_args(argc,argv);

    std::cerr << "Available threads: " << __cilkrts_get_nworkers() << "\n";

#if 0
    typedef asap::dense_vector<size_t, real, true, asap::mm_ownership_policy>
	vector_type;
#else
    typedef asap::sparse_vector<size_t, real, true, asap::mm_ownership_policy>
	vector_type;
#endif
    typedef asap::word_list<std::vector<const char *>, asap::word_bank_pre_alloc> word_list;
    typedef asap::data_set<vector_type,word_list> data_set_type;

    bool is_sparse;
    data_set_type data_set
	= asap::array_read<data_set_type>( std::string( infile ), is_sparse );
    std::cout << "Relation: " << data_set.get_relation() << std::endl;
    std::cout << "Dimensions: " << data_set.get_dimensions() << std::endl;
    std::cout << "Points: " << data_set.get_num_points() << std::endl;

    // Normalize data for improved clustering results
    // std::vector<std::pair<real, real>> extrema
 	// = asap::normalize( data_set );

    get_time (end);
    print_time("input", begin, end);

    // for reproducibility
    // srand(1);

/*
    for( auto I=data_set.vector_cbegin(), E=data_set.vector_cend();
	 I != E; ++I ) {
	std::cout << *I << std::endl;
    }
*/

   // K-means
    get_time (begin);
    typedef decltype(asap::kmeans( data_set, num_clusters, max_iters )) dset_type;
    dset_type* kmeans_op = nullptr;
    get_time (end);
    int stored_sse=INT_MAX;        
    for (int j = 0 ; j < num_runs ; ++j) {
        dset_type *kmeans_op_j = new dset_type(std::move(asap::kmeans( data_set, num_clusters, max_iters, 1e-4 )));
        if (j == 0) { 
            stored_sse = kmeans_op_j->within_sse();
            kmeans_op = kmeans_op_j;
        } else {
            if (kmeans_op_j->within_sse() < stored_sse)  {
	       delete kmeans_op;
               kmeans_op = kmeans_op_j; 
               stored_sse = kmeans_op_j->within_sse();
            } else {
              delete kmeans_op_j;
            }
        }
    }
    // std::cout << "Stored_sse after clustering is " << stored_sse << std::endl;
    print_time("kmeans", begin, end);

    // Unscale data
    get_time (begin);
    // asap::denormalize( extrema, data_set );
    get_time (end);        
    print_time("denormalize", begin, end);

    // Output
    get_time (begin);
    fprintf( stdout, "sparse? %s\n",
	     ( is_sparse && !force_dense ) ? "yes" : "no" );
    fprintf( stdout, "iterations: %d\n", kmeans_op->num_iterations() );

    fprintf( stdout, "within cluster SSE: %11.4lf\n", kmeans_op->within_sse() );

    std::ofstream of( outfile, std::ios_base::out );

    // Setup datastructure to hold the training/model data
    typedef asap::dense_vector<size_t, real, true, asap::mm_ownership_policy> centres_vector_type;
    std::vector<int> ids = {0,1,2,3,4,5,5,6,7,8,9,10,11,12,13,14,15};
    std::vector<std::string> cats = {"resident","resident","resident","dynamic_resident",
		"dynamic_resident","dynamic_resident","commuter","commuter","commuter","visitor",
		"visitor","visitor","resident","resident","visitor","visitor","visitor"};
    real modelFloats[17][24] = {
        {0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5},
	{0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1},
	{0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0},
	{0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5},
	{0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1},
	{1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0},
	{0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0},
	{0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0},
	{1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.5,0.5,0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.1, 0.1, 0.1,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{1.0,1.0,1.0,1.0,1.0,1.0,0.5,0.5,0.5,0.5,0.5,0.5, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.5,0.5,0.5,0.5,0.5,0.5, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.0,0.0,0.0,0.5,0.5,0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0},
	{0.0,0.0,0.0, 0.1, 0.1, 0.1,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0}
    };

    // Struct to hold each training record together in a struct with dense vector for the centres
    struct archetipiRecord {
        // archetipiRecord(int i, std::string str, centres_vector_type cvt) 
        archetipiRecord(int i, std::string str, real * cvt) 
              : id(i), name(str), centres(cvt) {}
        int id;
        std::string name;
        // centres_vector_type centres;
        real * centres;
    };

    // Vector of training records
    std::vector<archetipiRecord> archetipi;
    std::vector<int>::iterator itid ;
    std::vector<std::string>::iterator itname = cats.begin();
    int ct=0;
    // centres_vector_type cvt(17);
    real* cvt;
    for(itid = ids.begin(); itid != ids.end(); ++itid, ++itname, ++ct) {
        real * fPtr = modelFloats[ct];

        // cvt[ct] = modelFloats[ct];
        // archetipiRecord rec(*itid, *itname, cvt);
        archetipiRecord rec(*itid, *itname, modelFloats[ct]);
        archetipi.push_back( rec );
    }

#if 1
    // For each test cluster, find the minimum euclidean distance of the provided training clusters
    auto I = kmeans_op->centres().cbegin(); 
    auto E = kmeans_op->centres().cend(); 
    int cnt=0;
    for( auto II=I; II != E; ++II ) {

        real lowestDistance = std::numeric_limits<real>::max();
        std::vector<archetipiRecord>::iterator targetCategory;
	// std::cout << "Classifications from Centres number " << cnt << ":" << std::endl;
        for(std::vector<archetipiRecord>::iterator it = archetipi.begin(); it != archetipi.end(); ++it) {
            // std::cout << "[ centres- " << (*II).get_value()<< std::endl;
            // std::cout << "[ archi- " << (*it).centres << std::endl;

	    real distance = sq_dist((*II).get_value(), (*it).centres, 17 );
            // std::cout << "II : " << *II << std::endl << "it: " << (*it).centres << std::endl;
            // std::cout << "Category : " << (*it).name << "Distance: " << distance << std::endl;
 	    if (distance < lowestDistance) { 
		lowestDistance = distance; 
		targetCategory = it; 
	    }
        }

	std::ostringstream os;
	os << "('" << (*targetCategory).name << "', "  << *II << ")" << std::endl;
  	std::string s = os.str();
        std::replace( s.begin(), s.end(), '{', '[');
        std::replace( s.begin(), s.end(), '}', ']');
  	of << s ;// << std::endl;

    }
#endif

// Check that classifies datapoints directly with training data:
#if 0
    auto Idata = data_set.vector_cbegin();
    auto Edata = data_set.vector_cend();

    for( auto II=Idata; II != Edata; ++II ) {

        real lowestDistance = std::numeric_limits<real>::max();
        std::vector<archetipiRecord>::iterator targetCategory;
	std::cout << "Classifications from Data point" << ":" << std::endl;
        for(std::vector<archetipiRecord>::iterator it = archetipi.begin(); it != archetipi.end(); ++it) {
	    real distance = sq_dist((*II).get_value(), &(*it).centres[0], 24 );
            std::cout << "II : " << *II << std::endl << "it: " << (*it).centres << std::endl;
            std::cout << "Category : " << (*it).name << "  Distance: " << distance << std::endl;
 	    if (distance < lowestDistance) { 
		lowestDistance = distance; 
		targetCategory = it; 
	    }
        }

	// os << std::endl << "Classifications from Data Points" << std::endl;
	std::ostringstream os;
	os << "('" << (*targetCategory).name << "', "  << *II << ")";
  	std::string s = os.str();
        std::replace( s.begin(), s.end(), '{', '[');
        std::replace( s.begin(), s.end(), '}', ']');
  	of << s << std::endl;


        // *II == or within mcentres[?] 
      
    }

#endif

    of.close();
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
