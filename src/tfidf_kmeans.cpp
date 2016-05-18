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

#include <unistd.h>

#include <iostream>
#include <fstream>
#include <deque>
#include <unordered_map>

#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/cilk_api.h>

#include "asap/utils.h"
#include "asap/arff.h"
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/word_count.h"
#include "asap/normalize.h"
#include "asap/io.h"
#include "asap/kmeans.h"

#include <stddefines.h>

#define DEF_NUM_MEANS 8

size_t num_clusters = DEF_NUM_MEANS;
size_t max_iters = 0;
char const * indir = nullptr;
char const * outfile = nullptr;
bool by_words = false;
bool do_sort = false;
unsigned int rnd_init = 1;

static void help(char *progname) {
    std::cout << "Usage: " << progname
	      << " -i <indir> -o <outfile> -c <numclusters> [-m <maxiters>] [-w] [-s] [-r <rnd-init>]\n";
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:c:m:wsr:")) != EOF) {
        switch (c) {
	case 'i':
	    indir = optarg;
	    break;
	case 'o':
	    outfile = optarg;
	    break;
	case 'm':
	    max_iters = atoi(optarg);
	    break;
	case 'c':
	    num_clusters = atoi(optarg);
	    break;
	case 'r':
	    rnd_init = atoi(optarg);
	    break;
	case 'w':
	    by_words = true;
	    break;
	case 's':
	    do_sort = true;
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( num_clusters <= 0 )
	fatal( "Number of clusters must be larger than 0." );
    if( !indir )
	fatal( "Input directory must be supplied." );
    
    std::cerr << "Input directory = " << indir << '\n';
    if( !outfile )
	std::cerr << "Output stage skipped\n";
    else if( !strcmp( outfile, "-" ) )
	std::cerr << "Output file = standard output\n";
    else
	std::cerr << "Output file = " << outfile << '\n';
    std::cerr << "srand(" << rnd_init << ")\n";
    std::cerr << "TF/IDF by words = " << ( by_words ? "true\n" : "false\n" );
    std::cerr << "TF/IDF sort = " << ( do_sort ? "true\n" : "false\n" );
    std::cerr << "K-Means number of clusters = " << num_clusters << '\n';
    std::cerr << "K-Means maximum iterations = " << max_iters << '\n';
}

int main(int argc, char **argv) {
    struct timespec begin, end;
    struct timespec veryStart;

    srand( rnd_init == 0 ? time(NULL) : rnd_init );

    get_time( begin );
    veryStart = begin;

    // read args
    parse_args(argc,argv);

    std::cerr << "Available threads: " << __cilkrts_get_nworkers() << "\n";

    get_time (end);
    print_time("init", begin, end);

    // Directory listing
    get_time( begin );
    typedef asap::word_list<std::deque<const char*>, asap::word_bank_managed>
	directory_listing_type;
    directory_listing_type dir_list;
    asap::get_directory_listing( indir, dir_list );
    get_time (end);
    print_time("directory listing", begin, end);
    std::cerr << "Found " << dir_list.size() << " files\n";

    // word count
    get_time( begin );
#if 0
    typedef asap::word_map<std::unordered_map<const char *, size_t, asap::text::charp_hash, asap::text::charp_eql>, asap::word_bank_pre_alloc> word_map_type;
#else
    typedef asap::word_map<std::map<const char *, size_t, asap::text::charp_cmp>, asap::word_bank_pre_alloc> word_map_type;
#endif
    typedef asap::kv_list<std::vector<std::pair<const char *, size_t>>, asap::word_bank_pre_alloc> word_list_type;

    typedef asap::sparse_vector<size_t, float, false,
				asap::mm_no_ownership_policy>
	vector_type;
#if 1
    typedef asap::word_map<std::unordered_map<const char *,
				    asap::appear_count<size_t,
						       typename vector_type::index_type>,
				    asap::text::charp_hash, asap::text::charp_eql>,
			   asap::word_bank_pre_alloc> word_map_type2;
#else
    typedef asap::word_map<std::map<const char *,
				    asap::appear_count<size_t,
						       typename vector_type::index_type>,
				    asap::text::charp_cmp>,
			   asap::word_bank_pre_alloc> word_map_type2;
#endif

    size_t num_files = dir_list.size();
    std::vector<word_list_type> catalog;
    catalog.resize( num_files );

    asap::word_container_reducer<word_map_type2> allwords;
    cilk_for( size_t i=0; i < num_files; ++i ) {
	std::string filename = *std::next(dir_list.cbegin(),i);
	// std::cerr << "Read file " << filename;
	{
	    // Build up catalog for each file using a map
	    word_map_type wmap;
	    asap::word_catalog<word_map_type>( filename, wmap );
	    // Convert file's catalog to a (sorted) list of pairs
	    catalog[i].reserve( wmap.size() );    // avoid re-allocations
	    catalog[i].insert( std::move(wmap) ); // move out wmap contents
	} // delete wmap

	// The list of pairs is sorted if word_map_type is based on std::map
	// but it is not sorted if based on std::unordered_map
	if( do_sort )
	    std::sort( catalog[i].begin(), catalog[i].end(),
		       asap::pair_cmp<word_map_type::value_type,
		       word_map_type::value_type>() );

	// std::cerr << ": " << catalog[i].size() << " words\n";
	// Reading from std::vector rather than std::map should be faster...
	// Validated: about 10% on word count, 20% on TF/IDF, 16 threads
	allwords.count_presence( catalog[i] );
    }
    get_time (end);
    print_time("word count", begin, end);

    get_time( begin );
    typedef asap::data_set<vector_type, word_map_type2, directory_listing_type> data_set_type;
    // TODO: consider linearising the word_map to a word_list with exchanged
    //       word_bank in order to avoid storing the ID? Problem: lookup
    //       during TF/IDF computation
    // TODO: infer word_map_type2 from word_map_type* in template definition?
    // TODO: construct aggregate word_map_type2 during wc loop above
    std::shared_ptr<word_map_type2> allwords_ptr
	= std::make_shared<word_map_type2>();
    allwords_ptr->swap( allwords.get_value() );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    data_set_type data_set;
    if( by_words ) {
	data_set = asap::tfidf_by_words<vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, dir_list_ptr,
	    do_sort ); // whether catalogs are sorted
    } else {
	data_set = asap::tfidf<vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, *allwords_ptr, dir_list_ptr , true, true);
    }
    get_time( end );
    print_time("TF/IDF", begin, end);
    std::cerr << "TF/IDF number of words: " << data_set.get_dimensions()
	      << "\nTF/IDF number of files: " << data_set.get_num_points()
	      << std::endl;

    // Normalize data for improved clustering results
    get_time( begin );
    std::vector<std::pair<float, float>> extrema
	= asap::normalize( data_set );
    get_time( end );
    print_time("normalize", begin, end);

    // K-means clustering
    get_time( begin );
    auto kmeans_op = asap::kmeans( data_set, num_clusters, max_iters );
    get_time( end );
    print_time("K-Means", begin, end);
    std::cerr << "K-Means iterations: " << kmeans_op.num_iterations()
	      << "\nK-Means within-cluster SSE: " << kmeans_op.within_sse()
	      << std::endl;

    // Unscale data
    get_time( begin );
    asap::denormalize( extrema, data_set );
    get_time( end );        
    print_time("denormalize", begin, end);

    get_time( begin );
    if( !outfile )
	; // skip output
    else if( !strcmp( outfile, "-" ) ) {
	kmeans_op.output( std::cout );
    } else {
	std::ofstream of( outfile, std::ios_base::out );
	kmeans_op.output( of );
	of.close();
    }
    get_time( end );
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
