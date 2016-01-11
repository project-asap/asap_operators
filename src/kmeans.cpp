#include <iostream>
#include <fstream>
#include <unistd.h>

#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/cilk_api.h>

#include "asap/utils.h"
#include "asap/arff.h"
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

int main(int argc, char **argv) {
    struct timespec begin, end;
    struct timespec veryStart, veryEnd;

    srand( time(NULL) );

    get_time( begin );
    get_time( veryStart );

    //read args
    parse_args(argc,argv);

    std::cerr << "Available threads: " << __cilkrts_get_nworkers() << "\n";

    typedef asap::sparse_vector<size_t, float, true, asap::mm_ownership_policy>
	vector_type;
    typedef asap::word_list<std::vector<const char *>, asap::word_bank_pre_alloc> word_list;
    typedef asap::data_set<vector_type,word_list> data_set_type;

    bool is_sparse;
    data_set_type data_set
	= asap::arff_read<data_set_type>( std::string( infile ), is_sparse );
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
    kmeans_op.output( of );
    of.close();
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
