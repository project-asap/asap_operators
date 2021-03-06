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
#include "asap/ngram_bank.h"
#include "asap/normalize.h"
#include "asap/io.h"
#include "asap/hashtable.h"

#include <stddefines.h>

#define DEF_NUM_MEANS 8

#ifndef N_IN_NGRAM
static const size_t N = 3;
#else
static const size_t N = N_IN_NGRAM;
#endif

char const * indir = nullptr;
char const * outfile = nullptr;
bool by_words = false;
bool do_sort = false;
bool intm_map = false;

static void help(char *progname) {
    std::cout << "Usage: " << progname << " -i <indir> -o <outfile> [-w] [-s] [-m]\n";
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:wsm")) != EOF) {
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
	case 'm':
	    intm_map = true;
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( !indir )
	fatal( "Input directory must be supplied." );
    
    std::cerr << "Input directory = " << indir << '\n';
    if( !outfile )
	std::cerr << "Output stage skipped\n";
    else if( !strcmp( outfile, "-" ) )
	std::cerr << "Output file = standard output\n";
    else
	std::cerr << "Output file = " << outfile << '\n';
    std::cerr << "TF/IDF by words = " << ( by_words ? "true\n" : "false\n" );
    std::cerr << "TF/IDF list sorted = " << ( do_sort ? "true\n" : "false\n" );
    std::cerr << "N-grams, N = " << N << '\n';
}

template<typename map_type, bool can_sort = true>
typename std::enable_if<can_sort>::type
kv_sort( map_type & m ) {
    std::sort( m.begin(), m.end(),
	       asap::pair_cmp<typename map_type::value_type,
	       typename map_type::value_type>() );
}

template<typename map_type, bool can_sort>
typename std::enable_if<!can_sort>::type
kv_sort( map_type & m ) {
}

template<typename directory_listing_type, typename intl_map_type, typename intm_map_type, typename agg_map_type, typename data_set_type, bool can_sort>
data_set_type tfidf_driver( directory_listing_type & dir_list ) {
    struct timespec wc_end, tfidf_begin, tfidf_end;

    // word count
    get_time( tfidf_begin );
    size_t num_files = dir_list.size();
    std::vector<intm_map_type> catalog;
    catalog.resize( num_files );

    asap::ngram_container_reducer<agg_map_type> allwords;
    allwords.get_value().set_growth( 1, 2 );

    cilk_for( size_t i=0; i < num_files; ++i ) {
	// File to read
	std::string filename = *std::next(dir_list.cbegin(),i);
	catalog[i].set_growth( 1, 2 );
	size_t ngrams = asap::ngram_catalog<intl_map_type>( filename, catalog[i] );

	// The list of pairs is sorted if intl_map_type is based on std::map
	// but it is not sorted if based on std::unordered_map
	if( do_sort )
	    kv_sort<intm_map_type,can_sort>( catalog[i] );

	// std::cerr << filename << ": " << ngrams << " ngrams\n";
	allwords.count_presence( catalog[i] );
    }
    get_time( wc_end );

    std::shared_ptr<agg_map_type> allwords_ptr
	= std::make_shared<agg_map_type>();
    allwords_ptr->swap( allwords.get_value() );

    std::shared_ptr<directory_listing_type> dir_list_ptr
	= std::make_shared<directory_listing_type>();
    dir_list_ptr->swap( dir_list );

    asap::internal::assign_ids( allwords_ptr->begin(), allwords_ptr->end() );

    data_set_type tfidf(
	by_words
	? asap::tfidf_by_words<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, dir_list_ptr,
	    do_sort ) // whether catalogs are sorted
	: asap::tfidf<typename data_set_type::vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr, dir_list_ptr,
	    do_sort, // whether sorted by word
	    do_sort )
	); // whether catalogs are sorted
    get_time(tfidf_end);

    print_time("ngram count", tfidf_begin, wc_end);
    print_time("TF/IDF", wc_end, tfidf_end);
    std::cerr << "TF/IDF vectors: " << tfidf.get_num_points() << '\n';
    std::cerr << "TF/IDF dimensions: " << tfidf.get_dimensions() << '\n';
    print_time("library", tfidf_begin, tfidf_end);

    return tfidf;
}

#if 0
// a single null-terminated word
struct wc_word {
    const char* data;
    
    wc_word(const char * d = 0) : data( d ) { }
    wc_word( const wc_word & w ) : data( w.data ) { }
    
    // necessary functions to use this as a key
    bool operator<(wc_word const& other) const {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(wc_word const& other) const {
        return strcmp(data, other.data) == 0;
    }

    operator const char * () const { return data; }
    operator char * () const { return (char *)data; } // output
};


// a hash for the word
struct wc_word_hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(wc_word const& key) const
    {
        const char* h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};
#endif

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
    asap::get_directory_listing( indir, dir_list );
    get_time (end);
    print_time("directory listing", begin, end);

    typedef size_t index_type;
    typedef asap::word_bank_pre_alloc word_bank_type;

    typedef asap::sparse_vector<index_type, float, false,
				asap::mm_no_ownership_policy>
	vector_type;

/*
    typedef asap::word_map<
	std::unordered_map<const char *, size_t, asap::text::charp_hash,
			   asap::text::charp_eql>,
	word_bank_type> internal_map_type;

    typedef asap::kv_list<std::vector<std::pair<const char *, size_t>>,
			  word_bank_type> intermediate_map_type;

    typedef asap::word_map<
	std::unordered_map<const char *,
			   asap::appear_count<size_t, index_type>,
			   asap::text::charp_hash, asap::text::charp_eql>,
	word_bank_type> aggregate_map_type;
*/
    typedef asap::hash_table<asap::text::ngram<N>, size_t, asap::text::ngram_hash, asap::text::ngram_eql> wc_unordered_map;
    typedef asap::hash_table<asap::text::ngram<N>,
		       asap::appear_count<size_t, index_type>,
		       asap::text::ngram_hash, asap::text::ngram_eql> dc_unordered_map;
    typedef asap::ngram_map<dc_unordered_map, word_bank_type, N> aggregate_map_type;
/*
    typedef std::vector<std::pair<asap::text::ngram<N>,
				  asap::appear_count<size_t, index_type>>> dc_unordered_map;
    typedef asap::ngram_kv_list<dc_unordered_map, word_bank_type, N> aggregate_map_type;
*/

    typedef asap::ngram_map<wc_unordered_map, word_bank_type, N> internal_map_type;
    typedef asap::ngram_kv_list<std::vector<std::pair<asap::text::ngram<N>, size_t>>,
				word_bank_type, N> intermediate_map_type;

    typedef asap::data_set<vector_type, aggregate_map_type,
			   directory_listing_type> data_set_type;

    data_set_type tfidf(
	intm_map
	? tfidf_driver<directory_listing_type, internal_map_type,
	internal_map_type, aggregate_map_type,
	data_set_type, false>( dir_list )
	: tfidf_driver<directory_listing_type, internal_map_type,
	intermediate_map_type, aggregate_map_type,
	data_set_type, true>( dir_list )
	);

    get_time( begin );
    if( outfile )
	asap::arff_write( outfile, tfidf );
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
