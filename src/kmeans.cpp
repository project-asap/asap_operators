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

#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/cilk_api.h>

#include "asap/utils.h"
#ifdef IMR
#include "asap/imrformat.h"
#else
#include "asap/arff.h"
#endif
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/kmeans.h"
#include "asap/normalize.h"

#include <stddefines.h>

#define DEF_NUM_MEANS 8

size_t num_clusters;
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
    max_iters = 0;
    
    while ((c = getopt(argc, argv, "c:i:o:m:d")) != EOF) {
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
	case 'i':
	    infile = optarg;
	    break;
	case 'o':
	    outfile = optarg;
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( num_clusters <= 0 )
	fatal( "Number of clusters must be larger than 0." );
    if( !infile )
	fatal( "Input file must be supplied." );
    if( !outfile )
	fatal( "Output file must be supplied." );
    
    std::cerr << "Number of clusters = " << num_clusters << '\n';
    std::cerr << "Input file = " << infile << '\n';
    std::cerr << "Output file = " << outfile << '\n';
}

typedef float real;

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

#ifdef IMR
    typedef asap::dense_vector<size_t, float, true, asap::mm_ownership_policy>
	vector_type;
#else
    typedef asap::sparse_vector<size_t, float, true, asap::mm_ownership_policy>
	vector_type;
#endif
    typedef asap::word_list<std::vector<const char *>, asap::word_bank_pre_alloc> word_list;
    typedef asap::data_set<vector_type,word_list> data_set_type;

    bool is_sparse;
    data_set_type data_set
#ifdef IMR
	= asap::array_read<data_set_type>( std::string( infile ), is_sparse );
#else
	= asap::arff_read<data_set_type>( std::string( infile ), is_sparse );
#endif
    std::cout << "Relation: " << data_set.get_relation() << std::endl;
    std::cout << "Dimensions: " << data_set.get_dimensions() << std::endl;
    std::cout << "Points: " << data_set.get_num_points() << std::endl;

    // Normalize data for improved clustering results
    std::vector<std::pair<float, float>> extrema
 	= asap::normalize( data_set );

    get_time (end);
    print_time("input", begin, end);

    // for reproducibility
    srand(1);

/*
    for( auto I=data_set.vector_cbegin(), E=data_set.vector_cend();
	 I != E; ++I ) {
	std::cout << *I << std::endl;
    }
*/

    // K-means
    get_time (begin);
    auto kmeans_op = asap::kmeans( data_set, num_clusters, max_iters );
    get_time (end);        
    print_time("kmeans", begin, end);

    // Unscale data
    get_time (begin);
    asap::denormalize( extrema, data_set );
    get_time (end);        
    print_time("denormalize", begin, end);

    // Output
    get_time (begin);
    fprintf( stdout, "sparse? %s\n",
	     ( is_sparse && !force_dense ) ? "yes" : "no" );
    fprintf( stdout, "iterations: %d\n", kmeans_op.num_iterations() );

    fprintf( stdout, "within cluster SSE: %11.4lf\n", kmeans_op.within_sse() );

    std::ofstream of( outfile, std::ios_base::out );
#ifndef IMR
    kmeans_op.output( of );
#else

    // Setup datastructure to hold the training/model data
    typedef asap::dense_vector<size_t, float, true, asap::mm_ownership_policy> centres_vector_type;
    std::vector<int> ids = {0,1,2,3,4,5,5,6,7,8,9,10,11,12,13,14,15};
    std::vector<std::string> cats = {"resident","resident","resident","dynamic_resident",
		"dynamic_resident","dynamic_resident","commuter","commuter","commuter","visitor",
		"visitor","visitor","resident","resident","visitor","visitor","visitor"};
    float modelFloats[17][24] = {
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
        archetipiRecord(int i, std::string str, centres_vector_type cvt) 
              : id(i), name(str), centres(cvt) {}
        int id;
        std::string name;
        centres_vector_type centres;
    };

    // Vector of training records
    std::vector<archetipiRecord> archetipi;
    std::vector<int>::iterator itid ;
    std::vector<std::string>::iterator itname = cats.begin();
    int ct=0;
    centres_vector_type cvt(24);
    for(itid = ids.begin(); itid != ids.end(); ++itid, ++itname, ++ct) {
        float * fPtr = modelFloats[ct];

        cvt[ct] = *modelFloats[ct];
        archetipiRecord rec(*itid, *itname, cvt);
        archetipi.push_back( rec );
    }

    // For each test cluster, find the minimum euclidean distance of the provided training clusters
    auto I = kmeans_op.centres().cbegin(); 
    auto E = kmeans_op.centres().cend(); 
    for( auto II=I; II != E; ++II ) {

        float lowestDistance = 99.99;
        std::vector<archetipiRecord>::iterator targetCategory;
        for(std::vector<archetipiRecord>::iterator it = archetipi.begin(); it != archetipi.end(); ++it) {
	    real distance = sq_dist((*II).get_value(), &(*it).centres[0], 24 );
 	    if (distance < lowestDistance) { 
		lowestDistance = distance; 
		targetCategory = it; 
                // printf("We have reached here\n");
	    }
        }

	std::ostringstream os;
   	os << "('" << (*targetCategory).name << "', " << "array(" << *II << "))" << std::endl;
  	std::string s = os.str();
        std::replace( s.begin(), s.end(), '{', '[');
        std::replace( s.begin(), s.end(), '}', ']');
  	of << s << std::endl;
    }
#endif

    of.close();
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
