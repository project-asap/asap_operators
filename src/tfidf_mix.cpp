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
#include "cilkpub/sort.h"

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

char const * indir = nullptr;
char const * outfile = nullptr;
bool by_words = false;
bool do_sort = false;
int config = 0;

static void help(char *progname) {
    std::cout << "Usage: " << progname << " -i <indir> -o <outfile> -c \"[HLM][HLM][HLM][HLM][HLM]\" [-w] [-s]\n";
}

int decode_char( char c ) {
    switch(std::tolower(c)) {
    case 'h': return 0;
    case 'l': return 1;
    case 'm': return 2;
    case 'i': return 3;
    case 'd': return 4;
    default: fatal( "configuration string can only contain HLMID" );
    }
}

int decode( const char *str ) {
    if( strlen(str) != 5 )
	fatal("configuration string must be [HLMD][HLMD][HLMD][HLMD][HLMID]" );
    int c = 0;
    for( int i=0; i < 5; ++i )
	c = c * 16 + decode_char(str[i]);
    return c;
}


static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:wsc:")) != EOF) {
        switch (c) {
	case 'i':
	    indir = optarg;
	    break;
	case 'o':
	    outfile = optarg;
	    break;
	case 'w':
	    by_words = true;
	    break;
	case 's':
	    do_sort = true;
	    break;
	case 'c':
	    config = decode(optarg);
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( !indir )
	fatal( "Input directory must be supplied." );

    if( (config & 15) == 0 && do_sort )
	fatal( "cannot sort by words if stored in hash table" );
    // This is to ensure consistent numbering in output
    if( (config & 15) == 0 && (config & 255) != 0 )
	fatal( "if last type is hash table, so must next to last type" );
    
    std::cerr << "Input directory = " << indir << '\n';
    if( !outfile )
	std::cerr << "Output stage skipped\n";
    else if( !strcmp( outfile, "-" ) )
	std::cerr << "Output file = standard output\n";
    else
	std::cerr << "Output file = " << outfile << '\n';
    std::cerr << "TF/IDF by words = " << ( by_words ? "true\n" : "false\n" );
    std::cerr << "TF/IDF list sorted = " << ( do_sort ? "true\n" : "false\n" );
}

template<typename map_type>
typename std::enable_if<map_type::can_sort>::type
kv_sort( map_type & m ) {
    cilkpub::cilk_sort( m.begin(), m.end(),
			asap::pair_cmp<typename map_type::value_type,
			typename map_type::value_type>() );
/*
    std::sort( m.begin(), m.end(),
	       asap::pair_cmp<typename map_type::value_type,
	       typename map_type::value_type>() );
*/
}

template<typename map_type>
typename std::enable_if<!map_type::can_sort>::type
kv_sort( map_type & m ) {
}

template<typename map_type1, typename map_type2>
typename std::enable_if<!std::is_same<map_type1,map_type2>::value>::type
move_map( map_type2 & mm, map_type1 && m ) {
    asap::reserve_space( mm, m.size() );
    mm.insert( std::move(m) );
    m.clear();
}

template<typename map_type>
void move_map( map_type & mm, const map_type & m ) {
    mm.swap( m );
}

template<typename map_type>
void move_map( map_type & mm, map_type && m ) {
    mm.swap( m );
}

template<typename map_type1, typename map_type2>
typename std::enable_if<asap::is_specialization_of<std::map, map_type1>::value
|| asap::is_specialization_of<std::vector, map_type1>::value
|| asap::is_specialization_of<asap::hash_table, map_type1>::value>::type
copy_index( map_type1 & i, map_type2 & m ) {
    asap::reserve_space( i, m.size() );
    i.insert( m.begin(), m.end() );
}

template<typename map_type1, typename map_type2>
typename std::enable_if<!asap::is_specialization_of<std::map, map_type1>::value
&& !asap::is_specialization_of<std::vector, map_type1>::value
&& !asap::is_specialization_of<asap::hash_table, map_type1>::value>::type
copy_index( map_type1 & i, map_type2 & m ) { }

template<bool lookup, typename map_type1, typename map_type2>
typename std::enable_if<lookup>::type
init_agg3( map_type1 & i, map_type2 && m ) {
    copy_index( i, m );
}

template<bool lookup, typename map_type1, typename map_type2>
typename std::enable_if<!lookup && !std::is_same<map_type1,map_type2>::value>::type
init_agg3( map_type1 & i, map_type2 && m ) {
    move_map( i, m );
}

template<bool lookup, typename map_type>
typename std::enable_if<!lookup>::type
init_agg3( map_type & i, map_type && m ) {
    i.swap( m );
}

template<typename directory_listing_type,
	 typename Iterator,
	 typename agg2_map_type,
	 typename agg3_map_type,
	 bool agg3_lookup_only,
	 typename data_set_type>
typename std::enable_if<agg3_lookup_only, data_set_type>::type
tfidf_call( Iterator I, Iterator E,
	    agg2_map_type & allwords2,
	    agg3_map_type & allwords3,
	    directory_listing_type & dir_list_ptr,
	    bool is_sorted, bool iterate_ascending ) {
    std::shared_ptr<agg2_map_type> allwords_ptr
	= std::make_shared<agg2_map_type>();
    allwords_ptr->swap( allwords2 );

    return asap::tfidf<typename data_set_type::vector_type>(
	I, E, allwords_ptr, allwords3, dir_list_ptr,
	is_sorted, // whether joint_word_map is sorted
	iterate_ascending );  // whether catalogs are sorted
}

template<typename directory_listing_type,
	 typename Iterator,
	 typename agg2_map_type,
	 typename agg3_map_type,
	 bool agg3_lookup_only,
	 typename data_set_type>
typename std::enable_if<!agg3_lookup_only, data_set_type>::type
tfidf_call( Iterator I, Iterator E,
	    agg2_map_type & allwords2,
	    agg3_map_type & allwords3,
	    directory_listing_type & dir_list_ptr,
	    bool is_sorted, bool iterate_ascending ) {
    std::shared_ptr<agg3_map_type> allwords_ptr
	= std::make_shared<agg3_map_type>();
    allwords_ptr->swap( allwords3 );

    return asap::tfidf<typename data_set_type::vector_type>(
	I, E, allwords_ptr, dir_list_ptr,
	is_sorted, // whether joint_word_map is sorted
	iterate_ascending );  // whether catalogs are sorted
}


template<typename directory_listing_type,
	 typename intl_map_type,
	 typename intm_map_type,
	 typename agg1_map_type,
	 typename agg2_map_type,
	 typename agg3_map_type,
	 bool agg3_lookup_only,
	 typename data_set_type>
data_set_type tfidf_driver( directory_listing_type & dir_list ) {
    struct timespec wc_end, sort_end, tfidf_begin, tfidf_end;

    // word count
    get_time( tfidf_begin );
    size_t num_files = dir_list.size();
    std::vector<intm_map_type> catalog;
    catalog.resize( num_files );

    // The final result must be sorted by key (words).
    // Sorting the intermediate lists achieves this goal only if the aggregate
    // map type is not automatically sorted.

    // Sort intm if we must, if we can, if the target retains the sort and
    // if the target is not automatically sorted
    bool do_sort_intm = do_sort && intm_map_type::can_sort
	&& agg1_map_type::can_sort && !agg1_map_type::always_sorted;
    // Also sort intm if intl is not sorted and intm is a list
    // do_sort_intm |= do_sort && !intl_map_type::always_sorted && intm_map_type::can_sort;
    // Agg1 is sorted if we sorted the intermediate list or if it is always
    // sorted
    bool agg1_is_sorted = ( do_sort_intm || agg1_map_type::always_sorted )
	&& agg1_map_type::can_sort;

    asap::word_container_reducer<agg1_map_type> allwords;
    cilk::reducer< cilk::op_add<size_t> > total_num_words(0);

    cilk_for( size_t i=0; i < num_files; ++i ) {
	// File to read
	std::string filename = *std::next(dir_list.cbegin(),i);
	// Internally use the type intl_map_type, then merge into the catalog[i]
	size_t num_words =
	    asap::word_catalog<intl_map_type>( std::string(filename),
					       catalog[i] );
	*total_num_words += num_words;

	// The list of pairs is sorted if intl_map_type is based on std::map
	// but it is not sorted if based on std::unordered_map
	if( do_sort_intm )
	    kv_sort<intm_map_type>( catalog[i] );

	// std::cerr << ": " << catalog[i].size() << " words\n";

	// TODO: merge hash -> list conversion and count_presence() in one
	//       step. Motivation: possibly need to access LLC/memory during
	//       traversal. More iops/memop.

	// TODO: use two different global (allwords) data structures:
	//  1. during the wc phase, use a hash map(?) with 1 value only (#files)
	//  2. then assign uniq IDs. Hereto need to iterate the hash map(?)
	//     construct a parallel iteration over the buckets of the hash map.
	//  3. construct a data structure suitable for the last phase with all
	//     features (#files, uniq ID)

	// TODO: replace by post-processing parallel multi-way merge?
	allwords.count_presence( catalog[i] );
    }
    get_time( wc_end );

    // Aggregate map has 4 phases:
    // 1. reduce/count_presence
    // 2. sort (optional, only if phase 1 does not produce sorted results)
    // 3. assign_ids
    // 4. random lookup
    // Phases 2 and 3 are similar; a single data structure suffices.
    agg2_map_type allwords2;
    move_map( allwords2, std::move(allwords.get_value()) );

    assert( agg2_map_type::can_sort || agg2_map_type::always_sorted
	    || !do_sort );
    if( do_sort && !agg1_is_sorted )
	kv_sort<agg2_map_type>( allwords2 );
    bool is_sorted = do_sort || agg1_is_sorted || agg2_map_type::always_sorted;
    bool iterate_ascending = is_sorted
	|| std::is_same<agg2_map_type,agg3_map_type>::value;
    asap::internal::assign_ids( allwords2.begin(), allwords2.end() );

    agg3_map_type allwords3;
    init_agg3<agg3_lookup_only>( allwords3, std::move(allwords2) );
    get_time( sort_end );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    /*
     * TODO: explore two approaches to pruning terms:
     * 1. after all is done, post-process matrix and remove terms, compact
     * 2. when assigning IDs, prune and update all data structures
     *    this would be ok on hash tables, maps, but not on sorted lists...
     * 1 vs 2 may depend on rate of pruning, but likely biased towards 1.
     */

    data_set_type tfidf;
/*
    if( by_words ) {
	std::shared_ptr<agg3_map_type> allwords_ptr
	    = std::make_shared<agg3_map_type>();
	allwords_ptr->swap( allwords3 );

	tfidf = asap::tfidf_by_words<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, dir_list_ptr,
	    is_sorted ); // whether catalogs are sorted
	    } else */ {
	tfidf = tfidf_call<std::shared_ptr<directory_listing_type>,
			   decltype(catalog.cbegin()),
			   agg2_map_type,
			   agg3_map_type,
			   agg3_lookup_only,
			   data_set_type>(
			       catalog.cbegin(), catalog.cend(), allwords2,
			       allwords3, dir_list_ptr,
			       is_sorted, // whether joint_word_map is sorted
			       iterate_ascending );  // whether catalogs are sorted
    }
    get_time(tfidf_end);

    print_time("word count", tfidf_begin, wc_end);
    std::cerr << "word count sort intm: " << do_sort_intm << '\n';
    std::cerr << "word count is sorted: " << agg1_is_sorted << '\n';
    print_time("word sort", wc_end, sort_end);
    print_time("TF/IDF", sort_end, tfidf_end);
    std::cerr << "Total words: " << total_num_words.get_value() << '\n';
    std::cerr << "TF/IDF vectors: " << tfidf.get_num_points() << '\n';
    std::cerr << "TF/IDF dimensions: " << tfidf.get_dimensions() << '\n';
    std::cerr << "TF/IDF indices sorted by word: " << is_sorted << '\n';
    std::cerr << "TF/IDF iterate catalog in ascending order: "
	      << iterate_ascending << '\n';
    print_time("library", tfidf_begin, tfidf_end);

    return tfidf;
}

template<typename word_bank_type, char m>
struct wc_container_type {
};

template<typename word_bank_type>
struct wc_container_type<word_bank_type,'h'> {
    typedef asap::hash_table<const char*, size_t, asap::text::charp_hash,
			     asap::text::charp_eql> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct wc_container_type<word_bank_type,'d'> {
    typedef asap::hash_index<const char*, size_t, asap::text::charp_hash,
			     asap::text::charp_eql> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct wc_container_type<word_bank_type,'l'> {
    typedef std::vector<std::pair<const char *, size_t>> map_type;
    typedef asap::kv_list<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct wc_container_type<word_bank_type,'m'> {
    typedef std::map<const char *, size_t, asap::text::charp_cmp> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename word_bank_type, char m>
struct dc_container_type {
};

template<typename word_bank_type>
struct dc_container_type<word_bank_type,'h'> {
    typedef asap::hash_table<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct dc_container_type<word_bank_type,'d'> {
    typedef asap::hash_index<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct dc_container_type<word_bank_type,'i'> {
    typedef asap::hash_table<const char *,
			     asap::appear_count<size_t, size_t>,
			     asap::text::charp_hash, asap::text::charp_eql> map_type;
    typedef map_type type;
};


template<typename word_bank_type>
struct dc_container_type<word_bank_type,'l'> {
    typedef std::vector<std::pair<const char *,
				  asap::appear_count<size_t,size_t>>> map_type;
    typedef asap::kv_list<map_type, word_bank_type> type;
};

template<typename word_bank_type>
struct dc_container_type<word_bank_type,'m'> {
    typedef std::map<const char *,
		     asap::appear_count<size_t, size_t>,
		     asap::text::charp_cmp> map_type;
    typedef asap::word_map<map_type, word_bank_type> type;
};

template<typename DirListTy, typename VectorTy, typename WordBankTy,
	 char intl, char intm, char agg1, char agg2, char agg3>
struct type_select {
    typedef DirListTy directory_listing_type;
    typedef VectorTy vector_type;
    typedef WordBankTy word_bank_type;

    typedef typename
    wc_container_type<word_bank_type,intl>::type internal_map_type;
    typedef typename
    wc_container_type<word_bank_type,intm>::type intermediate_map_type;
    typedef typename
    dc_container_type<word_bank_type,agg1>::type aggregate1_map_type;
    typedef typename
    dc_container_type<word_bank_type,agg2>::type aggregate2_map_type;
    typedef typename
    dc_container_type<word_bank_type,agg3>::type aggregate3_map_type;
    typedef typename
    std::conditional<agg3=='i',aggregate2_map_type,aggregate3_map_type>::type
    dataset_index_type;

    static const bool aggregate3_lookup_only = agg3 == 'i';

    typedef asap::data_set<vector_type, dataset_index_type,
			   directory_listing_type> data_set_type;
};

template<typename directory_listing_type, typename vector_type,
	 typename word_bank_type, char intl, char intm, char agg1,
	 char agg2, char agg3>
void tfidf_switch( directory_listing_type & dir_list, const char * outfile,
		   size_t total_size, timespec veryStart ) {
    typedef type_select<directory_listing_type, vector_type,
			word_bank_type, intl, intm, agg1, agg2, agg3> types;

    typename types::data_set_type tfidf
	= tfidf_driver<directory_listing_type,
		       typename types::internal_map_type,
		       typename types::intermediate_map_type,
		       typename types::aggregate1_map_type,
		       typename types::aggregate2_map_type,
		       typename types::aggregate3_map_type,
		       types::aggregate3_lookup_only,
		       typename types::data_set_type>( dir_list );

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
    typedef asap::word_bank_pre_alloc word_bank_type;

    typedef asap::sparse_vector<index_type, float, false,
				asap::mm_no_ownership_policy>
	vector_type;

#define CMD(intl,intm,agg1,agg2,agg3)				      \
    tfidf_switch<directory_listing_type, vector_type, word_bank_type, \
		 intl,intm,agg1,agg2,agg3>( dir_list, outfile, total_size, veryStart );

    /*
     * H: 0
     * L: 1
     * M: 2
     * I: 3
     */
    switch( config ) {
    case 0x00000: CMD('h','h','h','h','h'); break;
    case 0x00011: CMD('h','h','h','l','l'); break;
    case 0x00111: CMD('h','h','l','l','l'); break;
    case 0x00222: CMD('h','h','m','m','m'); break;
    case 0x00444: CMD('h','h','d','d','d'); break;
    case 0x00210: CMD('h','h','m','l','h'); break;
    case 0x00211: CMD('h','h','m','l','l'); break;
    case 0x00212: CMD('h','h','m','l','m'); break;
    case 0x01000: CMD('h','l','h','h','h'); break;
    case 0x01011: CMD('h','l','h','l','l'); break;
    case 0x01012: CMD('h','l','h','l','m'); break;
    case 0x01013: CMD('h','l','h','l','i'); break;
    case 0x01021: CMD('h','l','h','m','l'); break;
    case 0x01022: CMD('h','l','h','m','m'); break;
    case 0x01044: CMD('h','l','h','d','d'); break;
    case 0x01100: CMD('h','l','l','h','h'); break;
    case 0x01111: CMD('h','l','l','l','l'); break;
    case 0x01113: CMD('h','l','l','l','i'); break;
    case 0x01212: CMD('h','l','m','l','m'); break;
    case 0x01222: CMD('h','l','m','m','m'); break;
    case 0x01444: CMD('h','l','d','d','d'); break;
    case 0x02000: CMD('h','m','h','h','h'); break;
	// case CFG_HMH: CMD('h','m','h'); break;
	// case CFG_HMM: CMD('h','m','m'); break;
	// case CFG_LHH: CMD('l','h','h'); break;
	// case CFG_LHM: CMD('l','h','m'); break;
	// case CFG_LLH: CMD('l','l','h'); break;
	// case CFG_LLM: CMD('l','l','m'); break;
	// case CFG_LMH: CMD('l','m','h'); break;
	// case CFG_LMM: CMD('l','m','m'); break;
    case 0x20000: CMD('m','h','h','h','h'); break;
    case 0x20111: CMD('m','h','l','l','l'); break;
    case 0x20222: CMD('m','h','m','m','m'); break;
    case 0x21000: CMD('m','l','h','h','h'); break;
    case 0x21111: CMD('m','l','l','l','l'); break;
    case 0x21222: CMD('m','l','m','m','m'); break;
    case 0x22000: CMD('m','m','h','h','h'); break;
    case 0x22111: CMD('m','m','l','l','l'); break;
    case 0x22222: CMD('m','m','m','m','m'); break;
    default:
	fatal( "unsupported configuration." );
    }
#undef CMD

    return 0;
}
