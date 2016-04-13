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
#include <pthread.h>
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
typedef double real;

int num_points; // number of vectors
int num_dimensions;         // Dimension of each vector
int num_clusters; // number of clusters
bool force_dense; // force a (slower) dense calculation
bool kmeans_workflow = false;
int max_iters; // maximum number of iterations
real * min_val; // min value of each dimension of vector space
real * max_val; // max value of each dimension of vector space
const char * fname= NULL; // input file
const char * outfile = NULL; // output file

#define CROAK(x)   croak(x,__FILE__,__LINE__)
// kmeans merged header sections end

struct point;

template<typename Value>
struct sparse_point {
    typedef Value value_type;

    int * c;
    value_type * v;
    int nonzeros;
    int cluster;

    sparse_point() { c = NULL; v = NULL; nonzeros = 0; cluster = -1; }
    sparse_point(int *c, value_type* v, int nz, int cluster)
	: c(c), v(v), nonzeros(nz), cluster(cluster) { }
    sparse_point(const point&pt);

    void swap( sparse_point & pt ) {
	std::swap( c, pt.c );
	std::swap( v, pt.v );
	std::swap( nonzeros, pt.nonzeros );
	std::swap( cluster, pt.cluster );
    }

    // ~sparse_point() {
	// delete[] c;
	// delete[] v;
    // }
    
#if 0 // untested
    const sparse_point & operator += ( const sparse_point & pt ) {
	int new_nonzeros = 0;
	int ti = 0, pi = 0;
	while( ti < nonzeros && pi < pt.nonzeros ) {
	    if( c[ti] <= pt.c[pi] )
		++ti;
	    if( c[ti] >= pt.c[pi] )
		++pi;
	    ++new_nonzeros;
	}
	new_nonzeros += (nonzeros - ti) + (pt.nonzeros - pi);

	int *new_c = new int[new_nonzeros];
	value_type *new_v = new value_type[new_nonzeros];

	int idx = 0;
	ti = 0, pi = 0;
	while( ti < nonzeros && pi < pt.nonzeros ) {
	    if( c[ti] < pt.c[pi] ) {
		new_c[idx] = c[ti];
		new_v[idx] = v[ti];
		++ti;
		++idx;
	    } else if( c[ti] == pt.c[pi] ) {
		new_c[idx] = c[ti];
		new_v[idx] = v[ti] + pt.v[pi];
		++ti;
		++idx;
	    } else {
		new_c[idx] = pt.c[pi];
		new_v[idx] = pt.v[pi];
		++pi;
		++idx;
	    }
	}
	while( ti < nonzeros ) {
	    new_c[idx] = c[ti];
	    new_v[idx] = v[ti];
	    ++ti;
	    ++idx;
	}
	while( pi < pt.nonzeros ) {
	    new_c[idx] = pt.c[pi];
	    new_v[idx] = pt.v[pi];
	    ++pi;
	    ++idx;
	}

	delete[] c;
	delete[] v;

	c = new_c;
	v = new_v;
	nonzeros = new_nonzeros;

	return *this;
    }
#endif

    bool normalize() {
	if( cluster == 0 ) {
	    std::cerr << "empty cluster...\n";
	    return true;
	} else {
#if VECTORIZED
	    v[0:nonzeros] /= (value_type)cluster;
#else
	    for(int i = 0; i < nonzeros; ++i)
		v[i] /= (value_type)cluster;
#endif
	    return false;
	}
    }

    void clear() {
#if VECTORIZED
	c[0:nonzeros] = (int)0;
	v[0:nonzeros] = (value_type)0;
#else
        for(int i = 0; i < nonzeros; ++i) {
	    c[i] = (int)0;
	    v[i] = (value_type)0;
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
    real sumsq;
    int cluster;

    point() { d = NULL; cluster = -1; }
    point(real* d, int cluster) { this->d = d; this->cluster = cluster; }
    point(const point &pt) {
	d = pt.d;
	sumsq = pt.sumsq;
	cluster = pt.cluster;
    }
    
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

    void update_sum_sq() {
	real ssq = 0;
        for (int i = 0; i < num_dimensions; i++) {
	    ssq += d[i] * d[i];
	}
	sumsq = ssq;
    }
    real get_sum_sq() const {
	return sumsq;
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
    const point & operator += ( const sparse_point<real> & pt ) {
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

struct wc_word_cmp
{
    bool operator() ( const wc_word & a, const wc_word & b ) const {
	return strcmp( a.data, b.data ) < 0;
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
#if 1
class fileVector : public std::vector<size_t> {
public:
    fileVector() { }
    fileVector(bool) : std::vector<size_t>( nfiles, 0 ) { }
};
#else
class fileVector {
    sparse_point<size_t> sparse;
public:
    fileVector() { }
    fileVector(bool) { }

    fileVector & operator += ( const fileVector & fv ) {
	sparse += fv.sparse;
	return *this;
    }
};
#endif

#ifdef P2_UNORDERED_MAP
typedef std::p2_unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
typedef std::p2_unordered_map<wc_word, fileVector, wc_word_hash, wc_word_pred> tfidf_unordered_map;
#elif defined(STD_MAP)
#include <map>
typedef std::map<wc_word, size_t, wc_word_cmp> wc_unordered_map;
typedef std::map<wc_word, std::pair<size_t, size_t>, wc_word_cmp> wc_unordered_pair_map;
typedef std::map<wc_word, fileVector, wc_word_cmp> tfidf_unordered_map;
#elif defined(STD_UNORDERED_MAP)
#include <unordered_map>
typedef std::unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
typedef std::unordered_map<wc_word, std::pair<size_t, size_t>, wc_word_hash, wc_word_pred> wc_unordered_pair_map;
typedef std::unordered_map<wc_word, fileVector, wc_word_hash, wc_word_pred> tfidf_unordered_map;
#elif defined(PHOENIX_MAP)
#include "container.h"
typedef hash_table<wc_word, size_t, wc_word_hash> wc_unordered_map;
typedef hash_table<wc_word, std::pair<size_t, size_t>, wc_word_hash> wc_unordered_pair_map;
typedef hash_table<wc_word, fileVector, wc_word_hash> tfidf_unordered_map;
#else
#include "container.h"
typedef hash_table_stored_hash<wc_word, size_t, wc_word_hash> wc_unordered_map;
typedef hash_table_stored_hash<wc_word, fileVector, wc_word_hash> tfidf_unordered_map;

#endif // P2_UNORDERED_MAP

#if !SEQUENTIAL

static double merge_time_wc = 0; 
static double merge_time_tfidf = 0; 

void merge_two_dicts( wc_unordered_map & m1, wc_unordered_map & m2 ) {
    struct timespec begin, end;
    get_time (begin);
    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
	m1[I->first] += I->second;
    }
    m2.clear();
    get_time (end);
    merge_time_wc += time_diff(end, begin);
}

void merge_two_dicts( wc_unordered_pair_map & m1, wc_unordered_pair_map & m2 ) {
    struct timespec begin, end;
    get_time (begin);
    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
	m1[I->first].first += I->second.first;
    }
    m2.clear();
    get_time (end);
    merge_time_wc += time_diff(end, begin);
}

void merge_two_dicts( tfidf_unordered_map & m1, tfidf_unordered_map & m2 ) {
    struct timespec begin, end;
    get_time (begin);
    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
#if 1
	fileVector & counts1 =  m1[I->first];
	const fileVector & counts2 =  I->second;
#if defined(STD_UNORDERED_MAP)
	if( counts1.size() == 0 )
	    counts1 = fileVector(true);
#endif
	// Vectorized
	size_t * v1 = &counts1.front();
	const size_t * v2 = &counts2.front();
	v1[0:nfiles] += v2[0:nfiles];
#else
	m1[I->first] += I->second;
#endif
    }
    m2.clear();
    get_time (end);
    merge_time_tfidf += time_diff(end, begin);
}

template<typename MapType>
class dictionary_reducer {
    struct Monoid : cilk::monoid_base<MapType> {
	static void reduce( MapType * left,
			    MapType * right ) {
	    TRACE( e_sreduce );
	    merge_two_dicts( *left, *right );
	    TRACE( e_ereduce );
	}
	static void identity( MapType * p ) {
	    // Initialize to useful default size depending on chunk size
#ifndef STD_MAP
	    new (p) MapType(); // 1<<16);
#else
	    new (p) MapType();
#endif
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    dictionary_reducer() : imp_() { }
    dictionary_reducer(size_t n) : imp_() {
#ifndef STD_MAP
	MapType init(n);
	imp_.view().swap( init );
#endif
    }

    void swap( MapType & c ) {
	imp_.view().swap( c );
    }

    typename MapType::mapped_type & operator []( wc_word idx ) {
	return imp_.view()[idx];
    }

    size_t empty() const {
	return imp_.view().size() == 0;
    }

    size_t size() const {
	return imp_.view().size(); 
    }

    typename MapType::iterator begin() { return imp_.view().begin(); }
    // typename wc_unordered_map::const_iterator cbegin() { return imp_.view().cbegin(); }
    typename MapType::iterator end() { return imp_.view().end(); }
    // typename wc_unordered_map::const_iterator cend() { return imp_.view().cend(); }

    typename MapType::const_iterator find( wc_word idx ) {
	return imp_.view().find( idx );
    }
};
typedef dictionary_reducer<wc_unordered_map> wc_dictionary_reducer;
typedef dictionary_reducer<wc_unordered_pair_map> wc_dictionary_pair_reducer;
typedef dictionary_reducer<tfidf_unordered_map> tfidf_dictionary_reducer;
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

typedef struct point Point;

template<typename Value>
sparse_point<Value>::sparse_point(const point&pt) {
    nonzeros=0;
    for( int i=0; i < num_dimensions; ++i )
	if( pt.d[i] != (value_type)0 )
	    ++nonzeros;

    c = new int[nonzeros];
    v = new value_type[nonzeros];

    int k=0;
    for( int i=0; i < num_dimensions; ++i )
	if( pt.d[i] != (value_type)0 ) {
	    c[k] = i;
	    v[k] = pt.d[i];
	    ++k;
	}
    assert( k == nonzeros );
}

template<typename Value>
real sparse_point<Value>::sq_dist(point const& p) const {
    real sum = 0;
#if 0
    int j=0;
    for( int i=0; i < num_dimensions; ++i ) {
	real diff;
	if( j < nonzeros && i == c[j] ) {
	    diff = v[j] - p.d[i];
	    ++j;
	} else
	    diff = p.d[i];
	sum += diff * diff;
    }
#else
    sum = p.get_sum_sq();
    for( int i=0; i < nonzeros; ++i ) { 
	sum += v[i] *  ( v[i] - real(2) * p.d[c[i]] );
	// assert( sum1 > real(0) );
    }
#endif
    // printf( "sum=%f sum1=%f\n", sum, sum1 );
    // assert( ( sum - sum1 ) / sum1 < 1e-3 );
    return sum;
}

template<typename Value>
bool sparse_point<Value>::equal(point const& p) const {
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
	if( p.d[k++] != (value_type)0 )
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
	Point &tgt = centres[c];
	for( int i=0; i < num_dimensions; ++i )
	    tgt.d[i] += pt->d[i];
	tgt.cluster++;
    }
    void add_point( sparse_point<real> * pt ) {
	int c = pt->cluster;
	Point &tgt = centres[c];
	for( int i=0; i < pt->nonzeros; ++i )
	    tgt.d[pt->c[i]] += pt->v[i];
	tgt.cluster++;
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

    void update_sum_sq() {
	cilk_for( int c=0; c < num_clusters; ++c )
	    centres[c].update_sum_sq();
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
    void select( const sparse_point<real> * pts ) {
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

    template<typename DSPoint>
    real within_sse( DSPoint * points ) {
	real sse = 0;
	for( int i=0; i < num_points; ++i ) {
	    sse += points[i].sq_dist( centres[points[i].cluster] );
	}
	return sse;
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

    void update_sum_sq() {
	imp_.view().update_sum_sq();
    }

    void swap( Centres & c ) {
	imp_.view().swap( c );
    }

    void add_point( Point * pt ) {
	imp_.view().add_point( pt );
    }
    void add_point( sparse_point<real> * pt ) {
	imp_.view().add_point( pt );
    }
};

#else
typedef Centres centres_reducer;
#endif

// kmeans merged declarations sections end

void wc( char * data, uint64_t data_size, uint64_t chunk_size, wc_unordered_map & wc_dict, unsigned int file) {
    uint64_t splitter_pos = 0;
    wc_dictionary_reducer dict; // (1<<16);
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
		    // dict[word][file]++;
		    dict[word]++;
		}
	    }
	    TRACE( e_emap );
        }( s );
    }
    cilk_sync;

    dict.swap( wc_dict );

    TRACE( e_synced );
    // std::cout << "final hash table size=" << final_dict.bucket_count() << std::endl;

#if 0
    // Merge dict for file into tfidf_dict
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
#if defined(STD_UNORDERED_MAP)
	if( tfidf_dict[I->first].size() == 0 )
	    tfidf_dict[I->first] = fileVector(true);
#endif
	tfidf_dict[I->first][file] += I->second;
    }
#endif
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

    if( std::is_same<sparse_point<real>,DSPoint>::value )
	centres.update_sum_sq();

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
    
    while ((c = getopt(argc, argv, "c:i:m:d:o:Dk")) != EOF) 
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
    
#ifdef KMEANS
    std::cerr << "Number of clusters = " << num_clusters << '\n';
#endif
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
    // Applies to all I/O -- no difference observed
    // std::cout << std::ios_base::sync_with_stdio(false);

    // For repeatability
    srand(1);

    struct timespec begin, end, all_begin;
    std::vector<std::string> files;

    // tfidf_dictionary_reducer dict;
    // tfidf_unordered_map dict(1<<16);
    // pthread_mutex_t dict_mux = PTHREAD_MUTEX_INITIALIZER;

    get_time (begin);
    all_begin = begin;

#if TRACING
    event_tracer::init();
#endif

    bool checkResults=false;
    int c;

    //read args
    parse_args(argc,argv);

    getdir(fname,files);
    nfiles = files.size();
    printf("number of files: %d\n", nfiles);

    wc_dictionary_pair_reducer total_dict_red; // (1<<16);
    wc_unordered_map file_dict[nfiles];

    char * fdata[files.size()];
#ifndef NO_MMAP
    struct stat finfo_array[files.size()];
#endif
    get_time (end);

    print_time("initialize", begin, end);

    get_time(begin);
    cilk::reducer< cilk::op_add<double> > time_read(0);
    cilk::reducer< cilk::op_add<double> > time_wc(0);
    cilk::reducer< cilk::op_add<double> > time_lock(0);
    cilk::reducer< cilk::op_add<double> > time_merge(0);

    cilk::reducer< cilk::op_add<size_t> > total_bytes(0);

    cilk_for (unsigned int i = 0;i < files.size();i++) {
#ifndef NO_MMAP
	struct stat & finfo = finfo_array[i];
#else
	struct stat finfo;
#endif
	int fd;
        struct timespec beginI, endI, beginWC;
        get_time(beginI);

        // Read in the file
        fd = open(files[i].c_str(), O_RDONLY);
       
        // Get the file info (for file length)
        fstat(fd, &finfo);
	*total_bytes += finfo.st_size;
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
	// Memory map the file
        fdata[i] = (char*)mmap(0, finfo.st_size + 1, 
			       PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_POPULATE, fd, 0);
#else
        // Memory map the file
        fdata[i] = (char*)mmap(0, finfo.st_size + 1, 
			       PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
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


	{
	    struct timespec begin_lock, end_lock;
	    get_time (beginWC);
	    wc_unordered_map & wc_dict = file_dict[i];
	    wc(fdata[i], finfo.st_size, 1024*1024, wc_dict, i);

#if 0
	    get_time(begin_lock);
	    *time_wc += time_diff(begin_lock, beginWC);
	    // Merge dict for file into tfidf_dict
	    pthread_mutex_lock( &dict_mux );
	    get_time(end_lock);
	    *time_lock += time_diff(end_lock, begin_lock);
	    for( auto I=wc_dict.begin(), E=wc_dict.end(); I != E; ++I ) {
#if defined(STD_UNORDERED_MAP)
		if( dict[I->first].size() == 0 )
		    dict[I->first] = fileVector(true);
#endif
		// dict[I->first][i] += I->second;
		dict[I->first][i] = I->second; // only one file i
	    }
	    pthread_mutex_unlock( &dict_mux );
	    get_time(begin_lock);
	    *time_merge += time_diff(begin_lock, end_lock);
#endif
	    // Copy-merge into total dictionary to count total number of
	    // distinct words as well as number of files each word occurs in.
	    for( auto I=wc_dict.begin(), E=wc_dict.end(); I != E; ++I ) {
		total_dict_red[I->first].first += (I->second > 0);
	    }
	}

        TRACE( e_smerge );
    }

#ifndef STD_MAP
    wc_unordered_pair_map total_dict; // (1<<16);
#else
    wc_unordered_pair_map total_dict;
#endif
    total_dict_red.swap( total_dict );

    get_time (end);
    printf("total file size: %d\n", total_bytes.get_value());
    print_time("input (work)", time_read.get_value());
    print_time("wc (work)", time_wc.get_value());
    print_time("input+wc (elapsed)", begin, end);
    print_time("merge (wc)", merge_time_wc);
    print_time("merge (tfidf)", merge_time_tfidf);
    print_time("lock (work)", time_lock.get_value());
    print_time("merge (files, work)", time_merge.get_value());

#ifndef KMEANS
    printf( "Transforming data for output and calculating TF-IDF\n" );
    get_time (begin);

    size_t ndim = total_dict.size();
    arff_file arff_data;

    num_dimensions = ndim; // arff_data.idx.size();
    num_points = nfiles; // arff_data.points.size();

    get_time (end);
    print_time("construct points", begin, end);
    
    printf( "writing output data and calculating TF-IDF\n" );
    get_time (begin);

    double time_tfidf = 0;

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

    for( auto I=total_dict.begin(), E=total_dict.end(); I != E; ++I ) {
        resFileTextArff << "\t";
        const string & str = I->first.data;
        resFileTextArff << loopStart << str << ' ' << typeStr << "\n";
    }

    resFileTextArff << "\n\n" << dataStr << "\n\n";
    resFileTextArff.flush();

    //
    // print the data
    //

    // Assign unique and successive IDs to each word
    // IDEA: sparse vectors need not work with successive IDs but dense ones
    //       do...
    size_t id = 0;
    for( auto I=total_dict.begin(), E=total_dict.end(); I != E; ++I, ++id ) {
	I->second.second = id;
    }

    // Build vectors
    for( size_t i=0; i < num_points; ++i ) {
        resFileTextArff << "\t{";
	size_t fcount = file_dict[i].size();
	assert( fcount > 0 );
	std::vector<std::pair<size_t, real> > data;
	data.reserve(fcount);
	int f = 0;
	for( auto I=file_dict[i].begin(), E=file_dict[i].end();
	     I != E; ++I, ++id ) {
	    // Should always find the word!
	    wc_unordered_pair_map::const_iterator TI
		= total_dict.find( I->first );
	    if( TI != total_dict.cend() ) {
		size_t tcount = TI->second.first;
		size_t id = TI->second.second;

		size_t tf = I->second;
		real norm = log10(((double) nfiles + 1.0) / ((double) tcount + 1.0)); 
		real tfidf = ((real)tf) * norm; // tfidf
		++f;
                // resFileTextArff << id << ' ' << tfidf << ',';
		data.push_back( std::make_pair( id+1, tfidf ) );
	    }
	}
	std::sort( data.begin(), data.end() );

	for( auto I=data.begin(), E=data.end(); I != E; ++I ) {
	    resFileTextArff << I->first << ' ' << I->second << ',';
	}

	assert( f == fcount );
        resFileTextArff << "}\n";
    }
    resFileTextArff.close();

    get_time (end);
    print_time("output", begin, end);
    print_time("tfidf part of output", time_tfidf);
#endif

#ifdef KMEANS
    {
	printf( "Transforming data for K-means and calculating TF-IDF\n" );
	get_time (begin);

	size_t ndim = total_dict.size();
	arff_file arff_data;
	arff_data.relation = "tfidf";
	arff_data.fdata = 0;

        num_dimensions = ndim; // arff_data.idx.size();
        num_points = nfiles; // arff_data.points.size();

	arff_data.minval = new real[ndim];
	arff_data.maxval = new real[ndim];
	for( int i=0; i < ndim; ++i ) {
	    arff_data.minval[i] = std::numeric_limits<real>::max();
	    arff_data.maxval[i] = std::numeric_limits<real>::min();
	}
    
	std::vector<sparse_point<real>> spoints;
	spoints.resize(num_points); // plan direct and parallel access
	sparse_point<real> * spoints_p = spoints.data();
	// memset( spoints_p, 0, sizeof(*spoints_p)*num_points );

	// Assign unique and successive IDs to each word
	// IDEA: sparse vectors need not work with successive IDs but dense ones
	//       do...
	size_t id = 0;
	for( auto I=total_dict.begin(), E=total_dict.end(); I != E; ++I, ++id ) {
	    I->second.second = id;

	    // If sparse (fewer non-zeroes then points), then minimum
	    // value of coordinate across points is 0. Setting now
	    // saves work later on.
	    size_t tcount = I->second.first;
	    if( tcount < nfiles )
		arff_data.minval[id] = 0;
	}

	// Accelerate memory allocation and reduce memory fragmentation
	// present in the standard memory allocator due to variable allocation
	// sizes.
	// Can be parallelized as reduction.
	size_t total_vec_elems = 0;
	size_t * vec_start = new size_t[num_points];
	for( size_t i=0; i < num_points; ++i ) {
	    size_t fcount = file_dict[i].size();
	    vec_start[i] = total_vec_elems;
	    total_vec_elems += fcount;
	}

	int  * alloc_c = new int[total_vec_elems];
	real * alloc_v = new real[total_vec_elems];

	// Build vectors
	cilk_for( size_t i=0; i < num_points; ++i ) {
	    size_t fcount = file_dict[i].size();
	    assert( fcount > 0 );
	    // real *v = new real[fcount];
	    // int  *c = new int[fcount];
	    real *v = &alloc_v[vec_start[i]];
	    int  *c = &alloc_c[vec_start[i]];
	    int   f = 0;
	    for( auto I=file_dict[i].begin(), E=file_dict[i].end();
		 I != E; ++I, ++id ) {
		// Should always find the word!
		wc_unordered_pair_map::const_iterator TI
		    = total_dict.find( I->first );
		if( TI != total_dict.cend() ) {
		    size_t tcount = TI->second.first;
		    size_t id = TI->second.second;

		    size_t tf = I->second;
		    real norm = log10(((double) nfiles + 1.0) / ((double) tcount + 1.0)); 
		    c[f] = id;
		    v[f] = ((real)tf) * norm; // tfidf
		    ++f;
		}
	    }
	    assert( f == fcount );

	    spoints_p[i].v = v;
	    spoints_p[i].c = c;
	    spoints_p[i].nonzeros = fcount;
	}

        get_time (end);
        print_time("construct points", begin, end);
    
        get_time (begin);

	// TODO: iteration order is wrong (sparse accesses)
	//       alternative: define and use a min/max point reducer,
	//       ideally accepting sparse_point arguments to compare against
	for( int j=0; j < num_points; ++j ) {
	    const sparse_point<real> & pt = spoints[j];
	    for( int i=0; i < pt.nonzeros; ++i ) {
		real v = pt.v[i];
		int  c = pt.c[i];
		if( arff_data.minval[c] > v )
		    arff_data.minval[c] = v;
		if( arff_data.maxval[c] < v )
		    arff_data.maxval[c] = v;
	    }
	}
	cilk_for( int j=0; j < num_points; ++j ) {
	    sparse_point<real> & pt = spoints[j];
	    for( int i=0; i < pt.nonzeros; ++i ) {
		real &v = pt.v[i];
		int   c = pt.c[i];
		if( arff_data.minval[c] != arff_data.maxval[c] ) {
		    v = (v - arff_data.minval[c])
			/ (arff_data.maxval[c] - arff_data.minval[c]+1);
		} else {
		    v = (real)1;
		}
	    }
	}
        get_time (end);
        print_time("normalize", begin, end);
    
        get_time (begin);
 
        std::cerr << "@relation: " << arff_data.relation << "\n";
        std::cerr << "@attributes: " << ndim << "\n";
        std::cerr << "@points: " << num_points << "\n";

        // From kmeans main, the rest of kmeans computation and output

        min_val = arff_data.minval;
        max_val = arff_data.maxval;

        // allocate memory
        // get points
        // point * points = &arff_data.points[0];
    
        // get means
        Centres centres;
    
	// TODO: integrate loop above
        for( int i=0; i < num_points; ++i ) {
	    spoints[i].cluster = rand() % num_clusters;
	    centres.add_point( &spoints[i] );
        }
        centres.normalize();
    
        get_time (end);
        print_time("kmeans initialize", begin, end);
    
        printf("KMeans: Calling MapReduce Scheduler\n");
    
        // keep re-clustering until means stabilise (no points are reassigned
        // to different clusters)
#if SEQUENTIAL && PMC
        LIKWID_MARKER_START("mapreduce");
#endif // SEQUENTIAL && PMC
        get_time (begin);        
        int niter = 1;
    
	while(kmeans_cluster(centres, &spoints[0])) {
	    if( ++niter >= max_iters && max_iters > 0 )
		break;
	    // centres.update_sum_sq();
	    // fprintf( stdout, "within cluster SSE: %11.4lf\n", centres.within_sse( &spoints[0] ) );
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
    
        if( arff_data.sparse_data && !force_dense )
	    centres.update_sum_sq();
	fprintf( stdout, "within cluster SSE: %11.4lf\n", centres.within_sse( &spoints[0] ) );

	get_time (end);
	print_time("final SSE", begin, end);
    
	// Output data
	get_time(begin);

	FILE * outfp = fopen( outfile, "w" );
	if( !outfp )
	    CROAK( "cannot open output file for writing" );

        fprintf( outfp, "%37s\n", "Cluster#" );
        fprintf( outfp, "%-16s", "Attribute" );
        fprintf( outfp, "%10s", "Full Data" );
        for( int i=0; i < num_clusters; ++i )
	    fprintf( outfp, "%11d", i );
        fprintf( outfp, "\n" );
    
        char buf[32];
        sprintf( buf, "(%d)", num_points );
        fprintf( outfp, "%26s", buf );
        for( int i=0; i < num_clusters; ++i ) {
	    sprintf( buf, "(%d)", centres[i].cluster );
	    fprintf( outfp, "%11s", buf );
        }
        fprintf( outfp, "\n" );
    
        fprintf( outfp, "================" );
        fprintf( outfp, "==========" );
        for( int i=0; i < num_clusters; ++i )
	    fprintf( outfp, "===========" );
        fprintf( outfp, "\n" );
    
	auto I=total_dict.begin(), E=total_dict.end();
        for( int i=0; i < num_dimensions; ++i, ++I ) {
	    // assert( I != E );
	    fprintf( outfp, "%-16s", I->first.data ); // arff_data.idx[i] );
	    real s = 0;
/*
	    for( int j=0; j < num_points; ++j )
	        s += points[j].d[i];
*/
	    for( int k=0; k < num_clusters; ++k )
		s += centres[k].d[i];

	    s /= (real)num_points;
	    s = min_val[i] + s * (max_val[i] - min_val[i] + 1);
	    fprintf( outfp, "%10.4lf", s );
	    for( int k=0; k < num_clusters; ++k ) {
	        real s = 0;
/*
	        for( int j=0; j < num_points; ++j )
		    if( spoints[j].cluster == k )
		        s += spoints[j].d[i];
*/
		s = centres[k].d[i];
	        s /= (real)centres[k].cluster;
	        s = min_val[i] + s * (max_val[i] - min_val[i] + 1);
	        fprintf( outfp, "%11.4lf", s );
	    }
	    fprintf( outfp, "\n" );
        }
	fclose( outfp );

	get_time (end);
	print_time("output", begin, end);
    
        //free memory
        // delete[] points; -- done in arff_file
        // oops, not freeing points[i].d 
    }
#endif // KMEANS

    get_time (end);
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
