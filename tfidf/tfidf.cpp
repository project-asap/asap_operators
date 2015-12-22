/* Copyright (c) 2007-2011, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <vector>
#include <algorithm>
#include <iostream>
#include <memory>
#ifdef P2_UNORDERED_MAP
#include "p2_unordered_map.h"
#endif // P2_UNORDERED_MAP

#if SEQUENTIAL && PMC
#include <likwid.h>
#endif

#if !SEQUENTIAL
#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/reducer_opadd.h>
// #include "cilkpub/sort.h"
#else
#define cilk_sync
#define cilk_spawn
#define cilk_for for
#endif

#if TRACING
#include "tracing/events.h"
#include "tracing/events.cc"
#define TRACE(e)  event_tracer::get().record( event_tracer::e, 0, 0 )
#else
#define TRACE(e)  do { } while( 0 )
#endif

#include "stddefines.h"
#include <tuple>
#include <map>
#include <set>
#include <fstream>
#include <math.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

// kmeans merged header section start
#include <stdlib.h>
#include <cilk/cilk_api.h>
#include <cerrno>
#include <cmath>
#include <limits>
#include <cassert>

#define DEF_NUM_POINTS 100000
#define DEF_NUM_MEANS 100
#define DEF_DIM 3
#define DEF_GRID_SIZE 1000
#define DEF_NUM_THREADS 8

#define REAL_IS_INT 0
typedef float real;

int num_points; // number of vectors
int num_dimensions;         // Dimension of each vector
int num_clusters; // number of clusters
bool force_dense; // force a (slower) dense calculation
bool kmeans_workflow = false;
bool tfidf_unit = true;
int max_iters; // maximum number of iterations
real * min_val; // min value of each dimension of vector space
real * max_val; // max value of each dimension of vector space
const char * fname= NULL; // input file
const char * outfile = NULL; // output file

#define CROAK(x)   croak(x,__FILE__,__LINE__)
// kmeans merged header sections end

// a passage from the text. The input data to the Map-Reduce
struct wc_string {
    char* data;
    uint64_t len;
};

// a single null-terminated word
struct wc_word {
    char* data;
    
    wc_word(char * d = 0) : data( d ) { }
    
    // necessary functions to use this as a key
    bool operator<(wc_word const& other) const {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(wc_word const& other) const {
        return strcmp(data, other.data) == 0;
    }
};


// a hash for the word
struct wc_word_hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(wc_word const& key) const
    {
        char* h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};

struct wc_word_pred
{
    bool operator() ( const wc_word & a, const wc_word & b ) const {
	return !strcmp( a.data, b.data );
    }
};

struct wc_sort_pred_by_first
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return a.first < b.first;
    }
};

struct wc_sort_pred
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return a.second > b.second 
	  || (a.second == b.second && strcmp( a.first.data, b.first.data ) > 0);
    }
};

struct wc_merge_pred
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return strcmp( a.first.data, b.first.data ) < 0;
    }
};

// Use inheritance for convenience, should use encapsulation.
static size_t nfiles = 0;
class fileVector : public std::vector<size_t> {
public:
    fileVector() { }
    fileVector(bool) : std::vector<size_t>( nfiles, 0 ) { }
};

#ifdef P2_UNORDERED_MAP
typedef std::p2_unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(STD_UNORDERED_MAP)
#include <unordered_map>
typedef std::unordered_map<wc_word, fileVector, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(PHOENIX_MAP)
#include "container.h"
typedef hash_table<wc_word, fileVector, wc_word_hash> wc_unordered_map;
#else
#include "container.h"
typedef hash_table_stored_hash<wc_word, fileVector, wc_word_hash> wc_unordered_map;

#endif // P2_UNORDERED_MAP

#if !SEQUENTIAL

static double merge_time = 0; 

void merge_two_dicts( wc_unordered_map & m1, wc_unordered_map & m2 ) {
    struct timespec begin, end;
    get_time (begin);
    // std::cerr << "merge 2...\n"; 
    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
	std::vector<size_t> & counts1 =  m1[I->first];
	const std::vector<size_t> & counts2 =  I->second;
	// Vectorized
	size_t * v1 = &counts1.front();
	const size_t * v2 = &counts2.front();
	v1[0:nfiles] += v2[0:nfiles];
    }
    m2.clear();
    get_time (end);
    merge_time += time_diff(end, begin);
    // std::cerr << "merge 2 done...\n";
}

class dictionary_reducer {
    struct Monoid : cilk::monoid_base<wc_unordered_map> {
	static void reduce( wc_unordered_map * left,
			    wc_unordered_map * right ) {
	    TRACE( e_sreduce );
	    merge_two_dicts( *left, *right );
	    TRACE( e_ereduce );
	}
	static void identity( wc_unordered_map * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) wc_unordered_map(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    dictionary_reducer() : imp_() { }

    void swap( wc_unordered_map & c ) {
	imp_.view().swap( c );
    }

    fileVector & operator []( wc_word idx ) {
	return imp_.view()[idx];
    }

    size_t empty() const {
	return imp_.view().size() == 0;
    }

    size_t size() const {
	return imp_.view().size(); 
    }

    typename wc_unordered_map::iterator begin() { return imp_.view().begin(); }
    // typename wc_unordered_map::const_iterator cbegin() { return imp_.view().cbegin(); }
    typename wc_unordered_map::iterator end() { return imp_.view().end(); }
    // typename wc_unordered_map::const_iterator cend() { return imp_.view().cend(); }

};
#else
typedef wc_unordered_map dictionary_reducer;
#endif

// kmeans merged declarations sections start
inline void __attribute__((noreturn))
croak( const char * msg, const char * srcfile, unsigned lineno ) {
    const char * es = strerror( errno );
    std::cerr << srcfile << ':' << lineno << ": " << msg
	      << ": " << es << std::endl;
    exit( 1 );
}

struct point;

struct sparse_point {
    int * c;
    real * v;
    int nonzeros;
    int cluster;

    sparse_point() { c = NULL; v = NULL; nonzeros = 0; cluster = -1; }
    sparse_point(int *c, real* d, int nz, int cluster)
	: c(c), v(v), nonzeros(nz), cluster(cluster) { }
    sparse_point(const point&pt);
    
    bool normalize() {
	if( cluster == 0 ) {
	    std::cerr << "empty cluster...\n";
	    return true;
	} else {
#if VECTORIZED
	    v[0:nonzeros] /= (real)cluster;
#else
	    for(int i = 0; i < nonzeros; ++i)
		v[i] /= (real)cluster;
#endif
	    return false;
	}
    }

    void clear() {
#if VECTORIZED
	c[0:nonzeros] = (int)0;
	v[0:nonzeros] = (real)0;
#else
        for(int i = 0; i < nonzeros; ++i) {
	    c[i] = (int)0;
	    v[i] = (real)0;
	}
#endif
    }
    
    real sq_dist(point const& p) const;
    bool equal(point const& p) const;
    
    void dump() const {
        for(int j = 0; j < nonzeros; j++) {
#if REAL_IS_INT
	    printf("%d: %5ld ", (int)c[j], (long)v[j]);
#else
	    printf("%d: %6.4f ", (int)c[j], (double)v[j]);
#endif
	}
        printf("\n");
    }
};

struct point
{
    real * d;
    int cluster;

    point() { d = NULL; cluster = -1; }
    point(real* d, int cluster) { this->d = d; this->cluster = cluster; }
    
    bool normalize() {
	if( cluster == 0 ) {
	    std::cerr << "empty cluster...\n";
	    return true;
	} else {
#if VECTORIZED
	    d[0:num_dimensions] /= (real)cluster;
#else
	    for(int i = 0; i < num_dimensions; ++i)
		d[i] /= (real)cluster;
#endif
	    return false;
	}
    }

    void clear() {
#if VECTORIZED
	d[0:num_dimensions] = (real)0;
#else
        for(int i = 0; i < num_dimensions; ++i)
	    d[i] = (real)0;
#endif
    }
    
#if VECTORIZED
    static unsigned real esqd( real a, real b ) {
	real diff = a - b;
need to adjust ...
	return diff * diff;
    }
    real sq_dist(point const& p) const {
	return __sec_reduce_add( esqd( d[0:num_dimensions],
				       p.d[0:num_dimensions] ) );
    }
#else
    real sq_dist(point const& p) const {
        real sum = 0;
        for (int i = 0; i < num_dimensions; i++) {
            real diff = d[i] - p.d[i];
	    // if( diff < (real)0 )
		// diff = -diff;
	    // diff = (diff - min_val[i]) / (max_val[i] - min_val[i] + 1);
            sum += diff * diff;
        }
        return sum;
    }
#endif
    
    void dump() const {
        for(int j = 0; j < num_dimensions; j++) {
#if REAL_IS_INT
	    printf("%5ld ", (long)d[j]);
#else
	    printf("%6.4f ", (double)d[j]);
#endif
	}
        printf("\n");
    }
    
    // For reduction of centre computations
    const point & operator += ( const point & pt ) {
#if VECTORIZED
	d[0:num_dimensions] += pt.d[0:num_dimensions];
#else
        for(int j = 0; j < num_dimensions; j++)
	    d[j] += pt.d[j];
#endif
	cluster += pt.cluster;
	return *this;
    }
    const point & operator += ( const sparse_point & pt ) {
// #if VECTORIZED
	// d[0:num_dimensions] += pt.d[0:num_dimensions];
// #else
        for(int j = 0; j < pt.nonzeros; j++)
	    d[pt.c[j]] += pt.v[j];
// #endif
	cluster += pt.cluster;
	return *this;
    }
};
typedef struct point Point;

sparse_point::sparse_point(const point&pt) {
    nonzeros=0;
    for( int i=0; i < num_dimensions; ++i )
	if( pt.d[i] != (real)0 )
	    ++nonzeros;

    c = new int[nonzeros];
    v = new real[nonzeros];

    int k=0;
    for( int i=0; i < num_dimensions; ++i )
	if( pt.d[i] != (real)0 ) {
	    c[k] = i;
	    v[k] = pt.d[i];
	    ++k;
	}
    assert( k == nonzeros );
}

real sparse_point::sq_dist(point const& p) const {
    real sum = 0;
    for (int i = 0; i < nonzeros; i++) {
	real diff = v[i] - p.d[c[i]];
	sum += diff * diff;
    }
    return sum;
}

bool sparse_point::equal(point const& p) const {
    int k=0;
    for( int i=0; i < nonzeros; ++i ) {
	while( k < c[i] ) {
	    if( p.d[k++] != (real)0 )
		return false;
	}
	if( p.d[k++] != v[i] )
	    return false;
    }
    while( k < num_dimensions ) {
	if( p.d[k++] != (real)0 )
	    return false;
    }
    return true;
}

class Centres {
    Point * centres;
    real * data;

public:
    Centres() {
	// Allocate backing store and initalize to zero
	data = new real[num_clusters * num_dimensions]();
	centres = new Point[num_clusters];
	for( int c=0; c < num_clusters; ++c ) {
	    centres[c] = Point( &data[c*num_dimensions], 0 );
	    centres[c].cluster = 0;
	}
    }
    ~Centres() {
	delete[] centres;
	delete[] data;
    }

    void clear() {
	for( int c=0; c < num_clusters; ++c ) {
	    centres[c].clear();
	    centres[c].cluster = 0;
	}
    }
    void add_point( Point * pt ) {
	int c = pt->cluster;
	for( int i=0; i < num_dimensions; ++i )
	    centres[c].d[i] += pt->d[i];
	centres[c].cluster++;
    }
    void add_point( sparse_point * pt ) {
	int c = pt->cluster;
	for( int i=0; i < pt->nonzeros; ++i )
	    centres[c].d[pt->c[i]] += pt->v[i];
	centres[c].cluster++;
    }

    void normalize( int c ) {
    	centres[c].normalize();
    }
    bool normalize() {
	bool modified = false;
	cilk_for( int c=0; c < num_clusters; ++c )
	    modified |= centres[c].normalize();
	return modified;
    }

    void select( const point * pts ) {
	for( int c=0; c < num_clusters; ) {
	    int pi = rand() % num_points;

	    // Check if we already have this point (may have duplicates)
	    bool incl = false;
	    for( int k=0; k < c; ++k ) {
		if( memcmp( centres[k].d, pts[pi].d,
			    sizeof(real) * num_dimensions ) ) {
		    incl = true;
		    break;
		}
	    }
	    if( !incl ) {
		for( int i=0; i < num_dimensions; ++i )
		    centres[c].d[i] = pts[pi].d[i];
		++c;
	    }
	}
    }
    void select( const sparse_point * pts ) {
	for( int c=0; c < num_clusters; ) {
	    int pi = rand() % num_points;

	    // Check if we already have this point (may have duplicates)
	    bool incl = false;
	    for( int k=0; k < c; ++k ) {
		if( pts[pi].equal( centres[k] ) ) {
		    incl = true;
		    break;
		}
	    }
	    if( !incl ) {
		centres[c].clear();
		for( int i=0; i < pts[pi].nonzeros; ++i )
		    centres[c].d[pts[pi].c[i]] = pts[pi].v[i];
		++c;
	    }
	}
    }


    const Point & operator[] ( int c ) const {
	return centres[c];
    }

    void reduce( Centres * cc ) {
	for( int c=0; c < num_clusters; ++c )
	    centres[c] += cc->centres[c];
    }

    void swap( Centres & c ) {
    	std::swap( data, c.data );
    	std::swap( centres, c.centres );
    }
};

#if !SEQUENTIAL
class centres_reducer {
    struct Monoid : cilk::monoid_base<Centres> {
	static void reduce( Centres * left, Centres * right ) {
#if TRACING
	    event_tracer::get().record( event_tracer::e_sreduce, 0, 0 );
#endif
	    left->reduce( right );
#if TRACING
	    event_tracer::get().record( event_tracer::e_ereduce, 0, 0 );
#endif
	}
    };

private:
    cilk::reducer<Monoid> imp_;

public:
    centres_reducer() : imp_() { }

    const Point & operator[] ( int c ) const {
	return imp_.view()[c];
    }

    void swap( Centres & c ) {
	imp_.view().swap( c );
    }

    void add_point( Point * pt ) {
	imp_.view().add_point( pt );
    }
    void add_point( sparse_point * pt ) {
	imp_.view().add_point( pt );
    }
};

#else
typedef Centres centres_reducer;
#endif

// kmeans merged declarations sections end

void wc( char * data, uint64_t data_size, uint64_t chunk_size, dictionary_reducer & dict, unsigned int file) {

    uint64_t splitter_pos = 0;
    while( 1 ) {
	TRACE( e_ssplit );

        /* End of data reached, return FALSE. */
        if ((uint64_t)splitter_pos >= data_size) {
	    TRACE( e_esplit );
            break;
	}

        /* Determine the nominal end point. */
        uint64_t end = std::min(splitter_pos + chunk_size, data_size);
        /* Move end point to next word break */
        while(end < data_size && 
            data[end] != ' ' && data[end] != '\t' &&
            data[end] != '\r' && data[end] != '\n')
            end++;
	if( end < data_size )
	    data[end] = '\0';

        /* Set the start of the next data. */
	wc_string s;
        s.data = data + splitter_pos;
        s.len = end - splitter_pos;
        
        splitter_pos = end;
	TRACE( e_esplit );

        /* Continue with map since the s data is valid. */
	cilk_spawn [&] (wc_string s) {
	    TRACE( e_smap );

	    // TODO: is it better for locatiy to move toupper() into the inner loop?
	    for (uint64_t i = 0; i < s.len; i++)
		s.data[i] = toupper(s.data[i]);

	    uint64_t i = 0;
            uint64_t start;
            wc_word word = { s.data+start };
	    while(i < s.len) {            
		while(i < s.len && (s.data[i] < 'A' || s.data[i] > 'Z'))
		    i++;
		start = i;
		/* and can we also vectorize toupper?
		while( i < s.len ) {
		   s.data[i] = toupper(s.data[i]);
		   if(((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
		      i++;
		   else
		      break;
		}
		*/
		// while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
		while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') ))
		    i++;
		if(i > start)
		{
		    s.data[i] = 0;
		    word = { s.data+start };
		    dict[word][file]++;
		}
	    }
	    TRACE( e_emap );
        }( s );
    }
    cilk_sync;

    TRACE( e_synced );
    // std::cout << "final hash table size=" << final_dict.bucket_count() << std::endl;
}

#define NO_MMAP

// vim: ts=8 sw=4 sts=4 smarttab smartindent

using namespace std;

int getdir (std::string dir, std::vector<std::string> &files)
{
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
        std::cerr << "Error(" << errno << ") opening " << dir << std::endl;
        return errno;
    }

    while ((dirp = readdir(dp)) != NULL) {
        std::string relFilePath=dir + "/" + dirp->d_name;
        struct stat buf;
        if( lstat(relFilePath.c_str(), &buf) < 0 ) {
            std::cerr << "Error(" << errno << ") lstat " << relFilePath << std::endl;
            return errno;
        }

        if (S_ISREG(buf.st_mode))
            files.push_back(relFilePath);
    }
    closedir(dp);
    return 0;
}

#include <unordered_map>
size_t is_nonzero( size_t s ) {
    return s != 0;
}


// kmeans merged reading sections start 
template<typename DSPoint>
int kmeans_cluster(Centres & centres, DSPoint * points) {
    int modified = 0;

    centres_reducer new_centres;

#if GRANULARITY
    int nmap = std::min(num_points, 16) * 16;
    int g = std::max(1, (int)((double)(num_points+nmap-1) / nmap));
#pragma cilk grainsize = g
    cilk_for(int i = 0; i < num_points; i++) {
#else
    cilk_for(int i = 0; i < num_points; i++) {
#endif
#if TRACING
	event_tracer::get().record( event_tracer::e_smap, 0, 0 );
#endif
        //assign points to cluster
        real smallest_distance = std::numeric_limits<real>::max();
        int new_cluster_id = -1;
        for(int j = 0; j < num_clusters; j++) {
            //assign point to cluster with smallest total squared difference (for all d dimensions)
            real total_distance = points[i].sq_dist(centres[j]);
            if(total_distance < smallest_distance) {
                smallest_distance = total_distance;
                new_cluster_id = j;
            }
        }

        //if new cluster then update modified flag
        if(new_cluster_id != points[i].cluster)
        {
	    // benign race; works well. Alternative: reduction(|: modified)
            modified = 1;
            points[i].cluster = new_cluster_id;
        }

	new_centres.add_point( &points[i] );
#if TRACING
	event_tracer::get().record( event_tracer::e_emap, 0, 0 );
#endif
    }

#if TRACING
    event_tracer::get().record( event_tracer::e_synced, 0, 0 );
#endif

/*
    cilk_for(int i = 0; i < num_clusters; i++) {
	if( new_centres[i].cluster == 0 ) {
	    cilk_for(int j = 0; j < num_dimensions; j++) {
		new_centres[i].d[j] = centres[i].d[j];
	    }
	}
    }
*/

    // for(int i = 0; i < num_clusters; i++) {
	// std::cout << "in cluster " << i << " " << new_centres[i].cluster << " points\n";
    // }

    new_centres.swap( centres );
    centres.normalize();
    return modified;
}

void parse_args(int argc, char **argv)
{
    int c;
    extern char *optarg;
    
    // num_points = DEF_NUM_POINTS;
    num_clusters = DEF_NUM_MEANS;
    max_iters = 0;
    // num_dimensions = DEF_DIM;
    // grid_size = DEF_GRID_SIZE;
    
    while ((c = getopt(argc, argv, "c:i:m:d:o:D:")) != EOF) 
    {
        switch (c) {
            // case 'd':
                // num_dimensions = atoi(optarg);
                // break;
	    case 'D':
		force_dense = true;
		break;
	    case 'd':
	        fname = optarg;
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
	    case 'k':
		kmeans_workflow = true;
		break;
	    case 'u':
		tfidf_unit = true;
		break;
            // case 'p':
                // num_points = atoi(optarg);
                // break;
	    // case 's':
		// grid_size = atoi(optarg);
		// break;
            case '?':
                printf("Usage: %s -d <vector dimension> -c <num clusters> -p <num points> -s <max value> -t <number of threads>\n", argv[0]);
                exit(1);
        }
    }
    
    // Make sure a filename is specified
    if( !fname ) {
        printf("USAGE: %s -d <directory name> [-c]\n", argv[0]);
        exit(1);
    }
    
    if( num_clusters <= 0 )
	CROAK( "Number of clusters must be larger than 0." );
    if( !fname )
	CROAK( "Input file must be supplied." );
    
    std::cerr << "Number of clusters = " << num_clusters << '\n';
    std::cerr << "Input file = " << fname << '\n';
}

struct arff_file {
    std::vector<const char *> idx;
    std::vector<Point> points;
    char * fdata;
    char * relation;
    real * minval, * maxval;
    bool sparse_data;

public:
    arff_file() : sparse_data(true) { }

};
// kmeans merged reading sections end

int main(int argc, char *argv[]) 
{
    struct timespec begin, end, all_begin;
    std::vector<std::string> files;

    dictionary_reducer dict;

    get_time (begin);
    all_begin = begin;

#if TRACING
    event_tracer::init();
#endif

    char *outfile = 0;
    bool checkResults=false;
    int c;

    //read args
    parse_args(argc,argv);

    getdir(fname,files);
    nfiles = files.size();
    char * fdata[files.size()];
#ifndef NO_MMAP
    struct stat finfo_array[files.size()];
#endif
    get_time (end);

    print_time("initialize", begin, end);

    get_time(begin);
    cilk::reducer< cilk::op_add<double> > time_read(0);
    cilk::reducer< cilk::op_add<double> > time_wc(0);

    cilk_for (unsigned int i = 0;i < files.size();i++) {
#ifndef NO_MMAP
	struct stat & finfo = finfo_array[i];
#else
	struct stat finfo;
#endif
	int fd;
        struct timespec beginI, endI, beginWC, endWC;
        get_time(beginI);

        // dict.setReserve(files.size());

        // Read in the file
        fd = open(files[i].c_str(), O_RDONLY);
       
        // Get the file info (for file length)
        fstat(fd, &finfo);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
	// Memory map the file
        fdata[i] = (char*)mmap(0, finfo.st_size + 1, 
			       PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
#else
        // Memory map the file
        fdata[i] = (char*)mmap(0, finfo.st_size + 1, 
			       PROT_READ, MAP_PRIVATE, fd, 0);
#endif
#else
        uint64_t r = 0;

        fdata[i] = (char *)malloc (finfo.st_size);
        while(r < (uint64_t)finfo.st_size)
            r += pread (fd, fdata[i] + r, finfo.st_size, r);
#endif    
    
        get_time (endI);
	*time_read += time_diff(endI, beginI);

        // print_time("thread file-read", beginI, endI);

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
#else
#endif
#else
        close(fd);
#endif

        get_time (beginWC);

        wc(fdata[i], finfo.st_size, 1024*1024, dict, i);

        get_time (endWC);
	*time_wc += time_diff(endWC, beginWC);
        // print_time("thread WC ", beginWC, endWC);

        TRACE( e_smerge );
    }

    get_time (end);
    print_time("input (work)", time_read.get_value());
    print_time("wc (work)", time_wc.get_value());
    print_time("input+wc (elapsed)", begin, end);

    printf( "writing output data and calculating TF-IDF\n" );
    get_time (begin);

#ifdef KMEANS
    arff_file arff_data;
    arff_data.relation = "tfidf";
#endif

    // File initialisation
    string strFilename(fname);
    string arffTextFilename = outfile ? outfile : (strFilename + ".arff");
    ofstream resFileTextArff;
    resFileTextArff.open(arffTextFilename, ios::out | ios::trunc);
    
#define SS(str) (str), (sizeof((str))-1)/sizeof((str)[0])
#define XS(str) (str).c_str(), (str).size()
#define ST(i)   ((const char *)&(i)), sizeof((i))

    // Char initialisations
    const char nline='\n';
    const char space=' ';
    const char tab='\t';
    const char comma=',';
    const char colon=':';
    const char lbrace='{';
    const char rbrace='}';

    //
    // print out arff text format
    //
    const string loopStart = "@attribute ";
    const string typeStr = "numeric";
    const string dataStr = "@data";
    const string headerTextArff("@relation tfidf");
    const string classTextArff("@attribute @@class@@ {text}");

    resFileTextArff << headerTextArff << "\n" << classTextArff << "\n";;
    resFileTextArff.flush();

    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

        resFileTextArff << "\t";

        const string & str = I->first.data;
/*
#ifndef STD_UNORDERED_MAP
        uint64_t id = I.getIndex();
#else
        uint64_t id = fn(I->first);
#endif
*/

        resFileTextArff << loopStart << str << ' ' << typeStr << "\n";
#ifdef KMEANS
        arff_data.idx.push_back(str.c_str());
#endif
    }

    resFileTextArff << "\n\n" << dataStr << "\n\n";
    resFileTextArff.flush();

    //
    // print the data
    //

#ifdef KMEANS
    // in-memory workflow setup
    int ndim = dict.size();
    real * coord = new real[ndim](); // zero init
#endif

    for (unsigned int i = 0;i < files.size();i++) {

        const string & keyStr = files[i];

	if( dict.empty() )
	    continue;

        resFileTextArff << "\t{";

        // iterate over each word to collect total counts of each word in all files (reducedCount)
        // OR the number of files that contain the work (existsInFilesCount)
        long id=1;
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I, ++id ) {

                size_t tf = I->second[i];
	        if (!tf) 
		    continue;

                // todo: workout how the best way to calculate and store each 
                // word total once for all files
#if 0
	        cilk::reducer< cilk::op_add<size_t> > existsInFilesCount(0);
	        cilk_for (int j = 0; j < I->second.size(); ++j) {
		    // *reducedCount += I->second[j];  // Use this if we want to count every occurence
		    if (I->second[j] > 0) *existsInFilesCount += 1;
	        }
	        size_t fcount = existsInFilesCount.get_value();
#else
	        const size_t * v = &I->second.front();
	        // size_t len = I->second.size();
	        size_t fcount = __sec_reduce_add( is_nonzero(v[0:nfiles]) );
#endif

                //     Calculate tfidf  ---   Alternative versions of tfidf:
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) sumOccurencesOfWord + 1.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) numOfOtherDocsWithWord + 2.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) reducedCount.get_value() + 1.0)); 
                // Sparks version;
                double tfidf = (double) tf * log10(((double) files.size() + 1.0) / ((double) fcount + 1.0)); 
/*
#ifndef STD_UNORDERED_MAP
                uint64_t id = I.getIndex();
#else
                uint64_t id = fn(I->first);
#endif
*/
#ifdef KMEANS
                coord[id] = tfidf;
                arff_data.points.push_back( point( coord, -1 ) );
#endif

                // Note:
                // If Weka etc doesn't care if there is an extra unnecessary comma at end
                // of a each record then we'd rather avoid the branch test here, so leave comma in
                resFileTextArff << id << ' ' << tfidf << ',';

        }
        resFileTextArff << "}\n";
    }
    resFileTextArff.close();

#ifdef KMEANS
        ndim = arff_data.idx.size();
	arff_data.minval = new real[ndim];
	arff_data.maxval = new real[ndim];
	for( int i=0; i < ndim; ++i ) {
	    arff_data.minval[i] = std::numeric_limits<real>::max();
	    arff_data.maxval[i] = std::numeric_limits<real>::min();
	}
	cilk_for( int i=0; i < ndim; ++i ) {
	    for( int j=0; j < arff_data.points.size(); ++j ) {
		real v = arff_data.points[j].d[i];
		if( arff_data.minval[i] > v )
		    arff_data.minval[i] = v;
		if( arff_data.maxval[i] < v )
		    arff_data.maxval[i] = v;
	    }
	    for( int j=0; j < arff_data.points.size(); ++j ) {
		arff_data.points[j].d[i] = (arff_data.points[j].d[i] - arff_data.minval[i])
		    / (arff_data.maxval[i] - arff_data.minval[i]+1);
	    }
	}
 
        std::cerr << "@relation: " << arff_data.relation << "\n";
        std::cerr << "@attributes: " << arff_data.idx.size() << "\n";
        std::cerr << "@points: " << arff_data.points.size() << "\n";

        // From kmeans main, the rest of kmeans computation and output

        num_dimensions = arff_data.idx.size();
        num_points = arff_data.points.size();
        min_val = arff_data.minval;
        max_val = arff_data.maxval;

        // allocate memory
        // get points
        point * points = &arff_data.points[0];
    
        // get means
        Centres centres;
    
        for( int i=0; i < num_points; ++i ) {
	    points[i].cluster = rand() % num_clusters;
	    centres.add_point( &points[i] );
        }
        centres.normalize();
    
        // for(int i = 0; i < num_clusters; i++) {
	    // std::cout << "in cluster " << i << " " << centres[i].cluster << " points\n";
        // }
    
        get_time (end);
        print_time("initialize", begin, end);
    
        printf("KMeans: Calling MapReduce Scheduler\n");
    
        // keep re-clustering until means stabilise (no points are reassigned
        // to different clusters)
#if SEQUENTIAL && PMC
        LIKWID_MARKER_START("mapreduce");
#endif // SEQUENTIAL && PMC
        get_time (begin);        
        int niter = 1;
        if( arff_data.sparse_data /* && !force_dense */ ) {
	    // First build sparse representation
	    std::vector<sparse_point> spoints;
	    spoints.reserve( num_points );
	    for( int i=0; i < num_points; ++i )
	        spoints.push_back( sparse_point( points[i] ) );
    
	    while(kmeans_cluster(centres, &spoints[0])) {
	        if( ++niter >= max_iters && max_iters > 0 )
		    break;
	    }
    
	    for( int i=0; i < num_points; ++i ) {
	        delete[] spoints[i].c;
	        delete[] spoints[i].v;
	    }
        } else {
	    while(kmeans_cluster(centres, points)) {
	        if( ++niter >= max_iters && max_iters > 0 )
		    break;
	    }
        }
        get_time (end);        
#if SEQUENTIAL && PMC
        LIKWID_MARKER_STOP("mapreduce");
#endif // SEQUENTIAL && PMC
    
        print_time("library", begin, end);
    
        get_time (begin);
    
        //print means
        printf("KMeans: MapReduce Completed\n");  
        fprintf( stdout, "iterations: %d\n", niter );
    
        real sse = 0;
        for( int i=0; i < num_points; ++i ) {
	    sse += centres[points[i].cluster].sq_dist( points[i] );
        }
        fprintf( stdout, "within cluster sum of squared errors: %11.4lf\n", sse );
    
        fprintf( stdout, "%37s\n", "Cluster#" );
        fprintf( stdout, "%-16s", "Attribute" );
        fprintf( stdout, "%10s", "Full Data" );
        for( int i=0; i < num_clusters; ++i )
	    fprintf( stdout, "%11d", i );
        fprintf( stdout, "\n" );
    
        char buf[32];
        sprintf( buf, "(%d)", num_points );
        fprintf( stdout, "%26s", buf );
        for( int i=0; i < num_clusters; ++i ) {
	    sprintf( buf, "(%d)", centres[i].cluster );
	    fprintf( stdout, "%11s", buf );
        }
        fprintf( stdout, "\n" );
    
        fprintf( stdout, "================" );
        fprintf( stdout, "==========" );
        for( int i=0; i < num_clusters; ++i )
	    fprintf( stdout, "===========" );
        fprintf( stdout, "\n" );
    
        for( int i=0; i < num_dimensions; ++i ) {
	    fprintf( stdout, "%-16s", arff_data.idx[i] );
	    real s = 0;
	    for( int j=0; j < num_points; ++j )
	        s += points[j].d[i];
	    s /= (real)num_points;
	    s = min_val[i] + s * (max_val[i] - min_val[i] + 1);
	    fprintf( stdout, "%10.4lf", s );
	    for( int k=0; k < num_clusters; ++k ) {
	        real s = 0;
	        for( int j=0; j < num_points; ++j )
		    if( points[j].cluster == k )
		        s += points[j].d[i];
	        s /= (real)centres[k].cluster;
	        s = min_val[i] + s * (max_val[i] - min_val[i] + 1);
	        fprintf( stdout, "%11.4lf", s );
	    }
	    fprintf( stdout, "\n" );
        }
    
        //free memory
        // delete[] points; -- done in arff_file
        // oops, not freeing points[i].d 

#endif

    get_time (end);
    print_time("output", begin, end);
    print_time("merge", merge_time);
    print_time("complete time", all_begin, end);

    get_time (begin);

#if TRACING
    event_tracer::destroy();
#endif

    for(int i = 0; i < files.size() ; ++i) {
#ifndef NO_MMAP
        munmap(fdata[i], finfo_array[i].st_size + 1);
#else
        free (fdata[i]);
#endif
    }

    get_time (end);
    print_time("finalize", begin, end);

    return 0;
}

// Unused function, code for future reference
void recordOfCodeForALLOutputFormats() {

    //
    // This func is Non-compilable, for future record for cleaner output formats code
    //
#if 0
    string strFilename(fname);
    string txtFilename(strFilename + ".txt");
    string binFilename(strFilename + ".bin");
    string arffTextFilename(strFilename + ".arff");
    string arffBinFilename(strFilename + ".arff.bin");
    ofstream resFile (txtFilename, ios::out | ios::trunc | ios::binary);
    ofstream resFileArff ( arffBinFilename, ios::out | ios::trunc | ios::binary);
    auto name_max = pathconf(fname, _PC_NAME_MAX);
    ofstream resFileTextArff;
    resFileTextArff.open(arffTextFilename, ios::out | ios::trunc );
    
    get_time (begin);

    string headerText("Document Vectors (sequencefile @ hdfs):");
    string headerTextArff("@relation tfidf");
    string classText("Key class: class org.apache.hadoop.io.Text Value Class: class org.apache.mahout.math.VectorWritable");
    string classTextArff("@attribute @@class@@ {text}");

#define SS(str) (str), (sizeof((str))-1)/sizeof((str)[0])
#define XS(str) (str).c_str(), (str).size()
#define ST(i)   ((const char *)&(i)), sizeof((i))

    char nline='\n';
    char space=' ';
    char tab='\t';
    char comma=',';
    char colon=':';
    char lbrace='{';
    char rbrace='}';
    char what;

    // print arff
    string loopStart = "@attribute ";
    string typeStr = "numeric";
    string dataStr = "@data";
    resFileArff.write (headerTextArff.c_str(), headerTextArff.size());
    resFileArff.write ((char *)&nline,1);
    resFileArff.write ((char *)&tab,1);
    resFileArff.write (classTextArff.c_str(), classTextArff.size());
    resFileArff.write ((char *)&nline,1);

    resFileTextArff << headerTextArff << "\n" << classTextArff << "\n";;
    resFileTextArff.flush();
    // uint64_t indices[dict.size()];
    unordered_map<uint64_t, uint64_t> idMap;
    // hash_table<uint64_t, uint64_t, wc_word_hash> idMap;
    int i=1;
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        resFileArff.write((char *)&tab, 1);

        resFileTextArff << "\t";

        uint64_t id = I.getIndex();
        string str = I->first.data;
        resFileArff.write((char *) loopStart.c_str(), loopStart.size());
        resFileArff.write((char *) str.c_str(), str.size());
        resFileArff.write((char *) &space, sizeof(char));
        resFileArff.write((char *) typeStr.c_str(), typeStr.size());
        resFileArff.write((char *) &nline, sizeof(char));

        resFileTextArff << loopStart << str << " " << typeStr << "\n";

        idMap[id]=i;
        i++;
        // cout << "\t" << loopStart << id << "\n";
    }
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) dataStr.c_str(), dataStr.size());
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) &nline, sizeof(char));

    resFileTextArff << "\n\n" << dataStr << "\n\n";

    // printing mahoot Dictionary
    cout << headerText << nline;
    cout << "\t" << classText << nline;

    // printing mahoot Dictionary
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write ((char *)&nline,1);
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write ((char *)&nline,1);
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        uint64_t id = I.getIndex();
        const string & str = I->first.data;
	resFile.write( SS( "\tKey: " ) )
	    .write( XS( str ) )
	    .write( SS( " Value: " ) )
	    .write( ST( id ) )
	    .write( SS( "\n" ) );
    }

    // printing mahoot output header
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write((char *) &nline, sizeof(char));
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write((char *) &nline, sizeof(char));
    // cout << headerText << nline << "\t" << classText << nline;
    
    resFileTextArff.flush();

    for (unsigned int i = 0;i < files.size();i++) {
        // printing mahoot loop start text including filename twice !
        string & keyStr = files[i];

	if( dict.empty() ) {
	    // string loopStart = "Key: " + keyStr + ": " + "Value: "
	    // + keyStr + ":";
	    // resFile.write ((char *)&tab, 1);
	    // resFile.write (loopStart.c_str(), loopStart.size());
	    // resFile.write ((char *)&rbrace, 1);
	    // resFile.write ((char *)&nline, 1);
	    resFile.write( SS( "\tKey: " ) )
		.write( XS( keyStr ) )
		.write( SS( ": Value: " ) )
		.write( XS( keyStr ) )
		.write( SS( ":{}\n" ) );
	    continue;
	}

	resFile.write( SS( "\tKey: " ) )
	    .write( XS( keyStr ) )
	    .write( SS( ": Value: " ) )
	    .write( XS( keyStr ) )
	    .write( SS( ":{" ) );

        resFileTextArff << "\t{";

        // iterate over each word to collect total counts of each word in all files (reducedCount)
        // OR the number of files that contain the work (existsInFilesCount)
        for( auto I=dict.begin(), E=dict.end(); I != E; ) {

                size_t tf = I->second[i];
	        if (!tf) {
		    ++I;
		    continue;
	        }

                // todo: workout how the best way to calculate and store each 
                // word total once for all files
#if 0
	        cilk::reducer< cilk::op_add<size_t> > existsInFilesCount(0);
	        cilk_for (int j = 0; j < I->second.size(); ++j) {
		    // *reducedCount += I->second[j];  // Use this if we want to count every occurence
		    if (I->second[j] > 0) *existsInFilesCount += 1;
	        }
	        size_t fcount = existsInFilesCount.get_value();
#else
	        const size_t * v = &I->second.front();
	        size_t len = I->second.size();
	        size_t fcount = __sec_reduce_add( is_nonzero(v[0:len]) );
#endif

                //     Calculate tfidf  ---   Alternative versions of tfidf:
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) sumOccurencesOfWord + 1.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) numOfOtherDocsWithWord + 2.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) reducedCount.get_value() + 1.0)); 
                // Sparks version;
                double tfidf = (double) tf * log10(((double) files.size() + 1.0) / ((double) fcount + 1.0)); 

                uint64_t id = I.getIndex();

	        resFile.write( ST(id) )
		    .write( SS(":") )
		    .write( ST( tfidf ) );

                resFileTextArff << idMap[id] << space << tfidf;

	        // if( I != E )
                    resFile.write ((char *) &comma, sizeof(char));

                    resFileArff.write ((char *) &comma, sizeof(char));


                ++I;

                // Note:
                // If Weka etc doesn't care if there is an extra unnecessary comma at end
                // of a each record then we'd rather avoid the branch test here, so leave it in
                    cout << ",";
                    resFileArff.write ((char *) &comma, sizeof(char));
                    resFileTextArff << comma;

        }
        // cout << "\n";
	// What is this? Reverse one character?
	// In order to avoid this, need to count number of words in each
	// file during wc(). Not sure if that pays off...
        long pos = resFile.tellp();
        resFile.seekp (pos-1);

        resFile.write ((char *)&rbrace, 1);
        resFile.write ((char *)&nline, 1);

        pos = resFileArff.tellp();
        resFileArff.seekp (pos-1);
	resFileArff.write( SS( "}\n" ) );

	resFile.write( SS( "}\n" ) );

         resFileTextArff << "}\n";

        cout << "}\n";
    }
    resFileArff.close();
    resFileTextArff.close();
    resFile.close();

    get_time (end);
    print_time("output", begin, end);
    print_time("all", all_begin, end);

    // Check on binary file:
    // -c at the command line with try to read results back in from binary file and display 
    // but note there will be odd chars before unsigned int64's as we do simple read of unsigned 
    // int 64 for 'id' Examining the binary file itself shows the binary file has the correct values for 'id'
    if (checkResults) {
        char colon, comma, cbrace, nline, tab;
        uint64_t id;
        double tfidf;
        char checkHeaderText[headerText.size()];
        char checkClassText[classText.size()];

        ifstream inResFile;
        inResFile.open("testRes.txt", ios::binary);

	std::cerr << "\nREADING IN --------------------------------" << "\n" ;
        // reading mahoot Dictionary
        inResFile.read( (char*)&checkHeaderText, headerText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        inResFile.read( (char*)&tab, 1);
        inResFile.read( (char*)&checkClassText, classText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        // checkClassText[classText.size()+1]=0;
	std::cerr << checkHeaderText << nline << tab << checkClassText << nline;
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
            inResFile.read( (char*)&tab, 1);

	    string str =  I->first.data;
            string iterStartCheck = "Key: " + str + " Value: ";
            char preText[iterStartCheck.size() +1 ];
            inResFile.read((char *)&preText, iterStartCheck.size());
            inResFile.read( (char*)&id, sizeof(uint64_t));
            inResFile.read( (char*)&nline, sizeof(char));
	    std::cerr << tab << preText << id << nline;
            // 	std::cerr << tab << preText << id;
        }

	// reading mahoot TFIDF mappings per word per file
	inResFile.read( (char*)checkHeaderText, headerText.size());
	inResFile.read( (char*)&nline, sizeof(char));
	inResFile.read( (char*)&tab, 1);
	inResFile.read( (char*)checkClassText, classText.size());
	inResFile.read( (char*)&nline, sizeof(char));
	std::cerr << checkHeaderText << nline << tab << checkClassText << nline;

	// read for each files
	for (unsigned int i = 0;i < files.size();i++) {
	    string & keyStr = files[i];
	    string loopStart = "Key: " + keyStr + ": " + "Value: " + keyStr + ":" + "{";
	    char preText[loopStart.size() +1];
	    inResFile.read( (char*)&tab, 1);
	    inResFile.read( (char*)&preText, loopStart.size());
	    std::cerr << tab << preText;
                
	    // iterate over each word to collect total counts of each word in all files
	    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

		size_t tf = I->second[i];
		if (!tf) continue;
		inResFile.read( (char*)&id, sizeof(uint64_t));
		inResFile.read( (char*)&colon, sizeof(char));
		inResFile.read( (char*)&tfidf, sizeof(double));
		inResFile.read( (char*)&comma, sizeof(char));
		std::cerr << id << colon << tfidf << comma ;
	    }
	    // inResFile.read((char*)&cbrace, 1);  // comma will contain cbrace after last iteration
	    cbrace=comma;
	    inResFile.read((char*)&nline, 1);
	    std::cerr << nline;
	}
    }
#endif

}
