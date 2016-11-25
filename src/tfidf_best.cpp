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
//#include "cilkpub/sort.h"

#include "asap/utils.h"
#include "asap/arff.h"
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/word_count.h"
#include "asap/normalize.h"
#include "asap/io.h"
#include "asap/hashtable.h"
#include "asap/hashindex.h"
#include "asap/traits.h"

#include <stddefines.h>

#define DEF_NUM_MEANS 8

enum algorithm_t {
    a_baseline,
    a_unsorted_fast,
    a_sorted_fast
};

char const * indir = nullptr;
char const * outfile = nullptr;
bool do_sort = false;
algorithm_t algo = a_baseline;

static void help(char *progname) {
    std::cout << "Usage: " << progname << " -i <indir> -o <outfile> [-a {hus}] [-w] [-s]\n";
}

algorithm_t decode_char( char c ) {
    switch(std::tolower(c)) {
    case 'h': return a_baseline;
    case 'u': return a_unsorted_fast;
    case 's': return a_sorted_fast;
    default: fatal( "configuration string can only be h, u or s" );
    }
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:wsa:")) != EOF) {
        switch (c) {
	case 'i':
	    indir = optarg;
	    break;
	case 'o':
	    outfile = optarg;
	    break;
	case 's':
	    do_sort = true;
	    break;
	case 'a':
	    algo = decode_char(*optarg);
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( !indir )
	fatal( "Input directory must be supplied." );

    if( do_sort && algo != a_sorted_fast )
	fatal( "Only the sorted-fast algorithm supports sorting" );

    std::cerr << "Input directory = " << indir << '\n';
    if( !outfile )
	std::cerr << "Output stage skipped\n";
    else if( !strcmp( outfile, "-" ) )
	std::cerr << "Output file = standard output\n";
    else
	std::cerr << "Output file = " << outfile << '\n';
    std::cerr << "TF/IDF list sorted = " << ( do_sort ? "true\n" : "false\n" );
}




template<typename directory_listing_type, typename vector_type,
	 typename word_bank_type>
void tfidf_all_hash( directory_listing_type & dir_list, const char * outfile,
		     size_t total_size, timespec veryStart ) {
    typedef asap::hash_table<const char*, size_t, asap::text::charp_hash,
			     asap::text::charp_eql> wc_map_type;
    typedef asap::word_map<wc_map_type, word_bank_type> internal_map_type;

    typedef asap::hash_table<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql>
    dc_map_type;
    typedef asap::word_map<dc_map_type, word_bank_type> aggregate_map_type;

    typedef asap::data_set<vector_type, aggregate_map_type,
			   directory_listing_type> data_set_type;

    struct timespec wc_end, sort_end, tfidf_begin, tfidf_end;

    // word count
    get_time( tfidf_begin );
    size_t num_files = dir_list.size();
    std::vector<internal_map_type> catalog;
    catalog.resize( num_files );

    // The final result must be sorted by key (words).
    // Sorting the intermediate lists achieves this goal only if the aggregate
    // map type is not automatically sorted.

    asap::word_container_reducer<aggregate_map_type> allwords;
    cilk::reducer< cilk::op_add<size_t> > total_num_words(0);

    cilk_for( size_t i=0; i < num_files; ++i ) {
	// File to read
	std::string filename = *std::next(dir_list.cbegin(),i);
	// Internally use the type internal_map_type, then merge into the catalog[i]
	size_t num_words =
	    asap::word_catalog<internal_map_type>( std::string(filename),
						   catalog[i] );
	*total_num_words += num_words;
	allwords.count_presence( catalog[i] );
    }
    get_time( wc_end );

    // Aggregate map has 4 phases:
    // 1. reduce/count_presence
    // 2. does not apply (sort)
    // 3. assign_ids
    // 4. random lookup
    // Phases 2 and 3 are similar; a single data structure suffices.
    asap::internal::assign_ids( allwords.get_value().begin(),
				allwords.get_value().end() );
    get_time( sort_end );

    std::shared_ptr<aggregate_map_type> allwords_ptr
	= std::make_shared<aggregate_map_type>();
    allwords_ptr->swap( allwords.get_value() );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    data_set_type
	tfidf = asap::tfidf<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, *allwords_ptr,
	    dir_list_ptr,
	    false, // whether joint_word_map is sorted
	    false );  // whether catalogs are sorted
    get_time(tfidf_end);

    print_time("word count", tfidf_begin, wc_end);
    std::cerr << "word count sort intm: " << false << '\n';
    std::cerr << "word count is sorted: " << false << '\n';
    print_time("word sort", wc_end, sort_end);
    print_time("TF/IDF", sort_end, tfidf_end);
    std::cerr << "Total words: " << total_num_words.get_value() << '\n';
    std::cerr << "TF/IDF vectors: " << tfidf.get_num_points() << '\n';
    std::cerr << "TF/IDF dimensions: " << tfidf.get_dimensions() << '\n';
    std::cerr << "TF/IDF indices sorted by word: " << false << '\n';
    std::cerr << "TF/IDF iterate catalog in ascending order: "
	      << false << '\n';
    print_time("library", tfidf_begin, tfidf_end);

    struct timespec begin, end;

    get_time( begin );
    if( outfile )
	asap::arff_write( outfile, tfidf );
    get_time (end);
    print_time("output", begin, end);
    print_time("complete time", veryStart, begin); // no output
    std::cerr << "Rate: "
	      << double(total_size)/double(time_diff(begin,veryStart))
	/double(1024*2014)
	      << " MB/s\n";
}

template<typename directory_listing_type, typename vector_type,
	 typename word_bank_type>
void tfidf_switch_hash_list( directory_listing_type & dir_list,
			     const char * outfile,
			     size_t total_size, timespec veryStart ) {
    typedef asap::hash_table<const char*, size_t, asap::text::charp_hash,
			     asap::text::charp_eql> wc_map_type;
    typedef asap::word_map<wc_map_type, word_bank_type> internal_map_type;

    typedef std::vector<std::pair<const char *, size_t>> intm_map_type;
    typedef asap::kv_list<intm_map_type, word_bank_type> intermediate_map_type;

    typedef asap::hash_table<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql>
    dc_map_type;
    typedef asap::word_map<dc_map_type, word_bank_type> aggregate_map_type;

    typedef aggregate_map_type dataset_index_type;

    typedef asap::data_set<vector_type, dataset_index_type,
			   directory_listing_type> data_set_type;

    struct timespec wc_end, sort_end, tfidf_begin, tfidf_end;

    // word count
    get_time( tfidf_begin );
    size_t num_files = dir_list.size();
    std::vector<intermediate_map_type> catalog;
    catalog.resize( num_files );

    // The final result must be sorted by key (words).
    // Sorting the intermediate lists achieves this goal only if the aggregate
    // map type is not automatically sorted.
    asap::word_container_reducer<aggregate_map_type> allwords;
    cilk::reducer< cilk::op_add<size_t> > total_num_words(0);

    cilk_for( size_t i=0; i < num_files; ++i ) {
	// File to read
	std::string filename = *std::next(dir_list.cbegin(),i);
	// Internally use the type internal_map_type, then merge
	// into the catalog[i]
	size_t num_words =
	    asap::word_catalog<internal_map_type>( std::string(filename),
						   catalog[i] );
	*total_num_words += num_words;
	allwords.count_presence( catalog[i] );
    }
    get_time( wc_end );

    // Aggregate map has 4 phases:
    // 1. reduce/count_presence
    // 2. sort (optional, only if phase 1 does not produce sorted results)
    // 3. assign_ids
    // 4. random lookup
    // Phases 2 and 3 are similar; a single data structure suffices.
    assert( !do_sort );
    asap::internal::assign_ids( allwords.get_value().begin(),
				allwords.get_value().end() );
    get_time( sort_end );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    std::shared_ptr<aggregate_map_type> allwords_ptr
	= std::make_shared<aggregate_map_type>();
    allwords_ptr->swap( allwords.get_value() );

    data_set_type
	tfidf = asap::tfidf<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, dir_list_ptr,
	    false, // whether joint_word_map is sorted
	    true );  // whether catalogs iterated in ascending order
    get_time(tfidf_end);

    print_time("word count", tfidf_begin, wc_end);
    std::cerr << "word count sort intm: " << false << '\n';
    std::cerr << "word count is sorted: " << false << '\n';
    print_time("word sort", wc_end, sort_end);
    print_time("TF/IDF", sort_end, tfidf_end);
    std::cerr << "Total words: " << total_num_words.get_value() << '\n';
    std::cerr << "TF/IDF vectors: " << tfidf.get_num_points() << '\n';
    std::cerr << "TF/IDF dimensions: " << tfidf.get_dimensions() << '\n';
    std::cerr << "TF/IDF indices sorted by word: " << false << '\n';
    std::cerr << "TF/IDF iterate catalog in ascending order: " << true << '\n';
    print_time("library", tfidf_begin, tfidf_end);

    struct timespec begin, end;
    get_time( begin );
    if( outfile )
	asap::arff_write( outfile, tfidf );
    get_time (end);
    print_time("output", begin, end);
    print_time("complete time", veryStart, begin); // no output
    std::cerr << "Rate: "
	      << double(total_size)/double(time_diff(begin,veryStart))
	/double(1024*2014)
	      << " MB/s\n";
}

template<typename directory_listing_type, typename vector_type,
	 typename word_bank_type>
void tfidf_switch_sortable( directory_listing_type & dir_list, const char * outfile,
			    size_t total_size, timespec veryStart ) {
    typedef asap::hash_table<const char*, size_t, asap::text::charp_hash,
			     asap::text::charp_eql> intl_map_type;
    typedef asap::word_map<intl_map_type, word_bank_type> internal_map_type;

    typedef std::vector<std::pair<const char *, size_t>> intm_map_type;
    typedef asap::kv_list<intm_map_type, word_bank_type> intermediate_map_type;

    typedef asap::hash_table<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql> agg1_map_type;
    typedef asap::word_map<agg1_map_type, word_bank_type> aggregate1_map_type;

    typedef std::vector<std::pair<const char *,
				  asap::appear_count<size_t,size_t>>> agg2_map_type;
    typedef asap::kv_list<agg2_map_type, word_bank_type> aggregate2_map_type;

    // Just a hash table, no word bank associated
    typedef agg1_map_type aggregate3_map_type;

    typedef asap::data_set<vector_type, aggregate2_map_type,
			   directory_listing_type> data_set_type;

    struct timespec wc_end, sort_end, tfidf_begin, tfidf_end;

    // word count
    get_time( tfidf_begin );
    size_t num_files = dir_list.size();
    std::vector<intermediate_map_type> catalog;
    catalog.resize( num_files );

    asap::word_container_reducer<aggregate1_map_type> allwords;
    cilk::reducer< cilk::op_add<size_t> > total_num_words(0);

    cilk_for( size_t i=0; i < num_files; ++i ) {
	// File to read
	std::string filename = *std::next(dir_list.cbegin(),i);
	// Internally use the type internal_map_type, then merge
	// into the catalog[i]. This uses a hash table (internal_map_type)
	// to calculate term frequency, then converts to a list (intermediate).
	size_t num_words =
	    asap::word_catalog<internal_map_type>( std::string(filename),
						   catalog[i] );
	// Reductions. Merge catalog[i] (list, intermediate_map_type)
	// into the document frequency (hash table, aggregate1_map_type).
	*total_num_words += num_words;
	allwords.count_presence( catalog[i] );
    }
    get_time( wc_end );

    // Aggregate map has 4 phases:
    // 1. reduce/count_presence
    // 2. sort (optional, only if phase 1 does not produce sorted results)
    // 3. assign_ids
    // 4. random lookup
    // Phases 2 and 3 are similar; a single data structure suffices.
    aggregate2_map_type allwords2;
    asap::reserve_space( allwords2, allwords.get_value().size() );
    allwords2.insert( std::move(allwords.get_value()) );
    allwords.get_value().clear();

    if( do_sort ) {
	/*
	cilkpub::cilk_sort( allwords2.begin(), allwords2.end(),
			    asap::pair_cmp<typename aggregate2_map_type::value_type,
			    typename aggregate2_map_type::value_type>() );
	 */
	std::sort( allwords2.begin(), allwords2.end(),
		   asap::pair_cmp<typename aggregate2_map_type::value_type,
		   typename aggregate2_map_type::value_type>() );
    }

    asap::internal::assign_ids( allwords2.begin(), allwords2.end() );

    // Construct an index (hash table) for fast lookup
    aggregate3_map_type allwords3;
    asap::reserve_space( allwords3, allwords2.size() );
    allwords3.insert( allwords2.begin(), allwords2.end() );
    get_time( sort_end );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    std::shared_ptr<aggregate2_map_type> allwords_ptr
	= std::make_shared<aggregate2_map_type>();
    allwords_ptr->swap( allwords2 );

    data_set_type
	tfidf = asap::tfidf<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr,
	    allwords3, dir_list_ptr,
	    do_sort, // whether joint_word_map is sorted
	    do_sort );  // whether catalogs are sorted
    get_time(tfidf_end);

    print_time("word count", tfidf_begin, wc_end);
    std::cerr << "word count sort intm: " << false << '\n';
    std::cerr << "word count is sorted: " << false << '\n';
    print_time("word sort", wc_end, sort_end);
    print_time("TF/IDF", sort_end, tfidf_end);
    std::cerr << "Total words: " << total_num_words.get_value() << '\n';
    std::cerr << "TF/IDF vectors: " << tfidf.get_num_points() << '\n';
    std::cerr << "TF/IDF dimensions: " << tfidf.get_dimensions() << '\n';
    std::cerr << "TF/IDF indices sorted by word: " << do_sort << '\n';
    std::cerr << "TF/IDF iterate catalog in ascending order: "
	      << do_sort << '\n';
    print_time("library", tfidf_begin, tfidf_end);

    struct timespec begin, end;
    get_time( begin );
    if( outfile )
	asap::arff_write( outfile, tfidf );
    get_time (end);
    print_time("output", begin, end);
    print_time("complete time", veryStart, begin); // no output
    std::cerr << "Rate: "
	      << double(total_size)/double(time_diff(begin,veryStart))
	/double(1024*2014)
	      << " MB/s\n";
}

/*
 * TODO:
 *  + sort files by descending size prior to processing.
 *    advantages:
 *     - parallelization (bin packing)
 *     - merge: global list size >> per-file size
 */
int main(int argc, char **argv) {
    struct timespec begin, end;
    struct timespec veryStart;

    srand( time(NULL) );

    get_time( begin );
    get_time( veryStart );

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
    size_t total_size = asap::get_directory_listing( indir, dir_list );
    get_time (end);
    print_time("directory listing", begin, end);
    std::cerr << "total bytes: " << total_size << '\n';

    typedef size_t index_type;
#if MEM == 0 // default
    typedef asap::word_bank_pre_alloc word_bank_type;
#elif MEM == 1
    typedef asap::word_bank_malloc word_bank_type;
#elif MEM == 2
    typedef asap::word_bank_managed word_bank_type;
#endif

    typedef asap::sparse_vector<index_type, float, false,
				asap::mm_no_ownership_policy>
	vector_type;

#define CMD(intm,agg2,agg3)				      \
    tfidf_switch<directory_listing_type, vector_type, word_bank_type, \
		 intm,agg2,agg3>( dir_list, outfile, total_size, veryStart );

    switch( algo ) {
    case a_baseline:
	tfidf_all_hash<directory_listing_type, vector_type, word_bank_type>(
	    dir_list, outfile, total_size, veryStart );
	break;
    case a_unsorted_fast:
	tfidf_switch_hash_list<directory_listing_type, vector_type, word_bank_type>( dir_list, outfile, total_size, veryStart );
	break;
    case a_sorted_fast:
	tfidf_switch_sortable<directory_listing_type, vector_type, word_bank_type>( dir_list, outfile, total_size, veryStart );
	break;
    default:
	fatal( "unsupported configuration." );
    }
#undef CMD

    return 0;
}
