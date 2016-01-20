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
#include <container.h>

#define DEF_NUM_MEANS 8

char const * infile = nullptr;
char const * outfile = nullptr;
bool do_sort = false;
size_t disp_num = 10;

static void help(char *progname) {
    std::cout << "Usage: " << progname << " -i <infile> -o <outfile> [-s] [-d <displaynum>]\n";
}

static void parse_args(int argc, char **argv) {
    int c;
    extern char *optarg;
    
    while ((c = getopt(argc, argv, "i:o:sd:")) != EOF) {
        switch (c) {
	case 'i':
	    infile = optarg;
	    break;
	case 'o':
	    outfile = optarg;
	    if( !strcmp( outfile, "-" ) )
		outfile = nullptr;
	    break;
	case 'd':
	    disp_num = atoi(optarg);
	    break;
	case 's':
	    do_sort = true;
	    break;
	case '?':
	    help(argv[0]);
	    exit(1);
        }
    }
    
    if( !infile )
	fatal( "Input file must be supplied." );
    
    std::cerr << "Input file = " << infile << '\n';
    std::cerr << "Output file = " << ( outfile ? outfile : "standard output" ) << '\n';
    std::cerr << "Word count list sorted = " << ( do_sort ? "true\n" : "false\n" );
    std::cerr << "Word count display number = " << disp_num << "\n";
}

struct hash_word {
    const char * p;
    size_t h;

    hash_word( const char *p_ ) : p( p_ ), h( asap::text::charp_hash()( p ) ) {
    }
    hash_word( const hash_word & hw ) : p( hw.p ), h( hw.h ) { }
    hash_word & operator = ( const hash_word & hw ) {
	p = hw.p;
	h = hw.h;
	return *this;
    }

    operator const char * () const { return p; }
    operator char * () const { return (char*)p; }
};

struct hash_word_hash {
    size_t operator() ( const hash_word & w ) const {
	return w.h;
    }
};

struct hash_word_eql {
    bool operator () ( const hash_word & lhs, const hash_word & rhs ) const {
	return strcmp( lhs.p, rhs.p ) == 0;
    }
};

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


template<typename Pair>
struct cmp_2nd_rev {
    bool operator() ( const Pair & p1, const Pair & p2 ) const {
	return p1.second > p2.second;
    }
};

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

    // word count
    get_time( begin );
    // typedef asap::word_map<std::unordered_map<hash_word, size_t, hash_word_hash, hash_word_eql>, asap::word_bank_pre_alloc> word_map_type;
    // typedef asap::kv_list<std::vector<std::pair<hash_word, size_t>>, asap::word_bank_pre_alloc> word_list_type;
    // typedef asap::word_map<std::unordered_map<const char *, size_t, asap::text::charp_hash, asap::text::charp_eql>, asap::word_bank_pre_alloc> word_map_type;
    // typedef asap::kv_list<std::vector<std::pair<const char *, size_t>>, asap::word_bank_pre_alloc> word_list_type;
    typedef hash_table<wc_word, size_t, wc_word_hash> wc_unordered_map;
    typedef asap::word_map<wc_unordered_map, asap::word_bank_pre_alloc> word_map_type;
    typedef asap::kv_list<std::vector<std::pair<wc_word, size_t>>, asap::word_bank_pre_alloc> word_list_type;

    word_list_type catalog;
    asap::word_catalog<word_map_type>( std::string(infile), catalog );
    get_time( end );
    print_time("word count", begin, end);

    get_time( begin );
    // The list of pairs is sorted if word_map_type is based on std::map
    // but it is not sorted if based on std::unordered_map
    if( do_sort )
	std::sort( catalog.begin(), catalog.end(),
		   cmp_2nd_rev<typename word_list_type::value_type>() );
    get_time( end );
    print_time("sort", begin, end);

    get_time( begin );
    FILE *fp = stdout;
    if( outfile ) {
	if( !(fp = fopen( outfile, "w" )) )
	    fatale( "fopen", outfile );
    }

    size_t dn = std::min(disp_num, catalog.size());
    fprintf( fp, "\nWordcount: Results (TOP %d of %lu):\n", dn, catalog.size());
    uint64_t total = 0;
    auto I = catalog.cbegin();
    auto E = catalog.cend();
    for( size_t i=0; I != E && i < dn; ++i, ++I ) {
        fprintf( fp, "%15s - %lu\n", (char *)I->first, I->second );
	total += I->second;
    }
    for( ; I != E; ++I )
	total += I->second;

    fprintf( fp, "Total: %lu\n", total );
    if( outfile )
	fclose( fp );
    get_time (end);
    print_time("output", begin, end);

    print_time("complete time", veryStart, end);

    return 0;
}
