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

#include <stddefines.h>

#define DEF_NUM_MEANS 8

char const * indir = nullptr;
char const * outfile = nullptr;
bool by_words = false;
bool do_sort = false;

static void help(char *progname) {
    std::cout << "Usage: " << progname << " -i <indir> -o <outfile> [-w] [-s]\n";
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:ws")) != EOF) {
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
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( !indir )
	fatal( "Input directory must be supplied." );
    if( !outfile )
	fatal( "Output file must be supplied." );
    
    std::cerr << "Input directory = " << indir << '\n';
    std::cerr << "Output file = " << outfile << '\n';
    std::cerr << "TF/IDF by words = " << ( by_words ? "true\n" : "false\n" );
    std::cerr << "TF/IDF list sorted = " << ( do_sort ? "true\n" : "false\n" );
}

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

    // word count
    get_time( begin );
    typedef asap::word_map<std::unordered_map<const char *, size_t, asap::text::charp_hash, asap::text::charp_eql>, asap::word_bank_pre_alloc> word_map_type;
    typedef asap::kv_list<std::vector<std::pair<const char *, size_t>>, asap::word_bank_pre_alloc> word_list_type;

    typedef asap::sparse_vector<size_t, float, false,
				asap::mm_no_ownership_policy>
	vector_type;
    typedef asap::word_map<std::unordered_map<const char *,
				    asap::appear_count<size_t,
						       typename vector_type::index_type>,
				    asap::text::charp_hash, asap::text::charp_eql>,
			   asap::word_bank_pre_alloc> word_map_type2;
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
	    asap::word_catalog( std::string(*std::next(dir_list.cbegin(),i)),
				wmap ); // catalog[i] );
	    // Convert file's catalog to a list of pairs
	    catalog[i].reserve( wmap.size() );    // avoid re-allocations
	    catalog[i].insert( std::move(wmap) ); // move out wmap contents

	    // The list of pairs is sorted if word_map_type is based on std::map
	    // but it is not sorted if based on std::unordered_map
	    if( do_sort )
		std::sort( catalog[i].begin(), catalog[i].end(),
			   asap::pair_cmp<word_map_type::value_type,
			   word_map_type::value_type>() );
	} // delete wmap

	// std::cerr << ": " << catalog[i].size() << " words\n";
	// Reading from std::vector rather than std::map should be faster...
	// Validated: about 10% on word count, 20% on TF/IDF, 16 threads
	allwords.count_presence( catalog[i] );
    }
    get_time (end);
    print_time("word count", begin, end);

    get_time( begin );
    typedef asap::data_set<vector_type, word_map_type2> data_set_type;
    // TODO: consider linearising the word_map to a word_list with exchanged
    //       word_bank in order to avoid storing the ID? Problem: lookup
    //       during TF/IDF computation
    // TODO: infer word_map_type2 from word_map_type* in template definition?
    // TODO: construct aggregate word_map_type2 during wc loop above
    std::shared_ptr<word_map_type2> allwords_ptr
	= std::make_shared<word_map_type2>();
    allwords_ptr->swap( allwords.get_value() );

    data_set_type tfidf;
    if( by_words ) {
	tfidf = asap::tfidf_by_words<vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr,
	    do_sort ); // whether catalogs are sorted
    } else {
	tfidf = asap::tfidf<vector_type>(
	    catalog.cbegin(), catalog.cend(), allwords_ptr,
	    do_sort ); // whether catalogs are sorted
    }

    get_time (end);
    print_time("TF/IDF", begin, end);

    get_time( begin );
    std::ofstream of( outfile, std::ios_base::out );

    if( by_words ) {
	auto WI = allwords_ptr->begin();
	for( auto I=tfidf.vector_cbegin(), E=tfidf.vector_cend();
	     I != E; ++I, ++WI ){
	    of << WI->first << ": " << *I << std::endl;
	}
    } else {
	size_t i=0;
	auto WI = dir_list.begin();
	for( auto I=tfidf.vector_cbegin(), E=tfidf.vector_cend();
	     I != E; ++I, ++WI ){
	    of << *WI << ": " << *I << std::endl;
	}
    }

    of.close();
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
