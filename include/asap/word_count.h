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


#ifndef INCLUDED_ASAP_WORD_COUNT_H
#define INCLUDED_ASAP_WORD_COUNT_H

#include <memory>
#include <algorithm>
#include <cctype>
#include <map>
#include <cmath>
#include <type_traits>
#include <iterator>
#include <cilk/cilk.h>
#include <cilk/reducer_opadd.h>

#include "asap/word_bank.h"

namespace asap {

namespace internal {
// This code checks whether the type T is a class with a member
// public: void reserve(size_t);
template<class T>
struct class_has_reserve_decl {
    template<void (T::*)()> struct tester;

    template<typename U>
    static small_type has_matching_member( tester<&U::reserve>*);
    template<typename U>
    static large_type has_matching_member(...);

    static const bool value =
	sizeof(has_matching_member<T>(0))==sizeof(small_type);
};
    
// Default case is no match (struct has bool value member = false).
template<typename T,bool is_class_type=std::is_class<T>::value>
struct has_reserve : std::false_type { };

// Case for structs where type matching with class_has_asap_decl succeeds
template<typename T>
struct has_reserve<T,true>
    : std::integral_constant<bool, class_has_reserve_decl<T>::value> { };

} // namespace internal

namespace text {

struct charp_hash {
    // FNV-1a hash for 64 bits
    size_t operator()( char const * key ) const {
        size_t v = 14695981039346656037ULL;
	for( char const *p = key; *p; ++p )
            v = (v ^ size_t(*p)) * 1099511628211ULL;
	return v;
    }
    size_t append( size_t prev, char const * key ) const {
        size_t v = prev;
	v = (v ^ size_t('#')) * 1099511628211ULL;
	for( char const *p = key; *p; ++p )
            v = (v ^ size_t(*p)) * 1099511628211ULL;
	return v;
    }
};

struct IsSpace {
    bool operator() ( char c ) const {
	return std::isspace( c );
    }
};

struct ToUpper {
    void operator() ( char & c ) const {
	c = std::toupper( c );
    }
};

struct IsUpper {
    bool operator() ( char c ) const {
	return std::isupper( c );
    }
};

struct IsUpperOrQuote {
    bool operator() ( char c ) const {
	return std::isupper( c ) || c == '\'';
    }
};

struct charp_cmp {
    bool operator () ( const char * lhs, const char * rhs ) const {
	return strcmp( lhs, rhs ) < 0;
    }
};

struct charp_eql {
    bool operator () ( const char * lhs, const char * rhs ) const {
	return strcmp( lhs, rhs ) == 0;
    }
};

template<typename MapTy /*, typename Reducer*/>
class map_reducer {
    typedef MapTy type;
    typedef typename MapTy::key_type key_type;
    typedef typename MapTy::mapped_type mapped_type;
    typedef typename MapTy::value_type value_type;

    struct Monoid : cilk::monoid_base<type> {
#if 1
	static void reduce( type * left, type * right ) {
	    if( left->size() < right->size() )
		// Assumes Reducer is commutative
		std::swap( *left, *right );
	    for( typename type::iterator I=right->begin(), E=right->end();
		 I != E; ++I ) {
		typename type::iterator T = left->find( I->first );
		if( T != left->end() )
		    T->second += I->second;
		else
		    left->insert( *I );
	    }
	    right->clear();
	}
#elif 0
	static void reduce( type * left, type * right ) {
	    if( left->size() < right->size() )
		// Assumes Reducer is commutative
		std::swap( *left, *right );
	    Reducer mapped_reducer();
	    for( typename type::iterator I=right->begin(), E=right->end();
		 I != E; ++I ) {
		typename type::iterator T = left->find( I->first );
		if( T != left->end() )
		    mapped_reducer( T->second, I->second );
		else
		    left->insert( *I );
	    }
	    right->clear();
	}
#else
	// Assumes iterators follow sorted order
	static void reduce( type * left, type * right ) {
	    type * rfrom = right;
	    type * rto = left;
	    if( left->size() < right->size() )
		std::swap( *rfrom, *rto );
	    Reducer mapped_reducer();
	    typename type::key_compare cmp = left->key_comp();
	    typename type::iterator hint = rto->begin();
	    for( typename type::iterator I=rfrom->begin(), E=rfrom->end();
		 I != E; ++I ) {
		typename type::iterator T = rto->lower_bound( I->first );
		if( T != rto->end() 
		    && !cmp( T->first, I->first )
		    && !cmp( I->first, T->first ) )
		    // Found
		    mapped_reducer( T->second, I->second );
		else {
		    typename type::iterator where = T;
		    if( where == rto->end() )
			where = hint;
		    hint = rto->insert( where, *I );
		}
	    }
	    right->clear();
	}
#endif
	static void identity( type * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) type();
	    // TODO: p->private_reserve(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    map_reducer() : imp_() { }

    void swap( type & c ) {
	imp_.view().swap( c );
    }

    mapped_type & operator [] ( const key_type & key ) {
	return imp_.view()[key];
    }
    const mapped_type & operator [] ( const key_type & key ) const {
	return imp_.view()[key];
    }

    type & get_value() { return imp_.view(); }
};

template<typename WordListTy>
class word_list_reducer {
    typedef WordListTy type;
    struct Monoid : cilk::monoid_base<type> {
	static void reduce( type * left, type * right ) {
	    left->reduce( *right );
	}
	static void identity( type * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) type();
	    // TODO: p->private_reserve(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    word_list_reducer() : imp_() { }
    word_list_reducer(size_t n) : imp_() {
	// private_reserve( n );
    }

    void swap( type & c ) {
	imp_.view().swap( c );
    }

    const char * index( char * p, size_t len ) {
	return imp_.view().index( p, len );
    }
};


template<size_t N_>
class ngram {
public:
    static const size_t N = N_;
    typedef const char ** iterator;
    typedef const char * const * const_iterator;

    ngram() { std::fill( begin(), end(), (char *)0 ); }
    ngram( const ngram & ng ) { std::copy( ng.cbegin(), ng.cend(), begin() ); }
    ngram & operator = ( const ngram & ng ) {
	std::copy( ng.cbegin(), ng.cend(), begin() );
	return *this;
    }

    const char * const & operator[]( size_t i ) const { return words[i]; }
    const char * & operator[]( size_t i ) { return words[i]; }

    iterator begin() { return &words[0]; }
    iterator end() { return &words[N]; }
    const_iterator begin() const { return &words[0]; }
    const_iterator end() const { return &words[N]; }
    const_iterator cbegin() const { return &words[0]; }
    const_iterator cend() const { return &words[N]; }

    bool push_back( const char * w ) {
	for( size_t i=0; i < N-1; ++i )
	    words[i] = words[i+1];
	words[N-1] = w;
	return words[0] != 0; // all have been initialised
    }

private:
    const char * words[N];
};

struct ngram_hash {
    template<size_t N>
    size_t operator()( const ngram<N> & key ) const {
	charp_hash ch;
	size_t h = ch( key[0] );
	for( size_t i=1; i < N; ++i )
	    h = ch.append( h, key[i] );
	return h;
    }
};
	
struct ngram_cmp {
    template<size_t N>
    bool operator () ( const ngram<N> & lhs, const ngram<N> & rhs ) const {
	for( size_t i=0; i < N; ++i ) {
	    int c = strcmp( lhs[i], rhs[i] );
	    if( c < 0 )
		return true;
	    if( c > 0 )
		return false;
	}
	return false;
    }
};

struct ngram_eql {
    template<size_t N>
    bool operator () ( const ngram<N> & lhs, const ngram<N> & rhs ) const {
	for( size_t i=0; i < N; ++i ) {
	    if( strcmp( lhs[i], rhs[i] ) != 0 )
		return false;
	}
	return true;
    }
};

template<typename WordListTy>
class ngram_list_reducer {
    typedef WordListTy type;
    struct Monoid : cilk::monoid_base<type> {
	static void reduce( type * left, type * right ) {
	    left->reduce( *right );
	}
	static void identity( type * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) type();
	    // TODO: p->private_reserve(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    ngram_list_reducer() : imp_() { }
    ngram_list_reducer(size_t n) : imp_() {
	// private_reserve( n );
    }

    void swap( type & c ) {
	imp_.view().swap( c );
    }

    const char * store( char * p, size_t len ) {
	return imp_.view().store( p, len );
    }

    void index( const ngram<type::N> & ng ) {
	return imp_.view().index( ng );
    }
};


template<typename MapTy>
size_t word_catalog( char * data, size_t data_size,
		   MapTy & catalog, size_t chunk_size ) {
    // Create a reducer hyperobject and prime it with the existing content
    word_list_reducer<MapTy> reduce_catalog(1<<16);
    cilk::reducer< cilk::op_add<size_t> > reduce_num_words(0);
    reduce_catalog.swap( catalog );

    char * const data_end = &data[data_size];
    char * split = data;

    while( split != data_end ) {
	// Split the data at the chunk_size.
        char * end = std::min(split + chunk_size, data_end);
	// Adjust the split to word boundaries.
	// end = std::find_if( end, data_end, IsSpace() );
	while( end != data_end && 
	       *end != ' ' && *end != '\t' &&
	       *end != '\r' && *end != '\n' )
	    ++end;
	if( end != data_end )
	    *end = '\0';

	// Process the chunk from split to end
	cilk_spawn [&] ( char * split, char * end ) {
	    // TODO: is it better for locality to move toupper() into the
	    //       inner loop, transforming while checking?
	    // Or better: use a word_bank with self-allocated storage and
	    // replace_copy_if words (need to pay attention to allocation of
	    // word_bank).
	    // std::for_each( split, end, ToUpper() );
	    for( char *I=split; I != end; ++I ) {
		// TODO: check performance with std::for_each and std::find
		// but with optimized codes in functors.
		// *I = std::toupper( *I );
		// TODO: vectorize this expression
		if( *I >= 'a' && *I <= 'z' )
		    *I = ( *I - 'a' ) + 'A';
	    }

	    while( split != end ) {
		// Skip non-upper characters
		// split = std::find_if( split, end, IsUpper() );
		while( split != end && !(*split >= 'A' && *split <= 'Z') )
		    ++split;
		// Pass over word
		char * w = split;
		// split = std::find_if_not( split, end, IsUpperOrQuote() );
		while( split != end
		       && ((*split >= 'A' && *split <= 'Z') || *split == '\'') )
		    ++split;
		*split = '\0'; // terminate

		if( split != w ) {
		    reduce_catalog.index( w, split-w );
		    *reduce_num_words += 1;
		}
	    }
        }( split, end );
        
        split = end;
    }
    cilk_sync;

    reduce_catalog.swap( catalog );
    return reduce_num_words.get_value();
}

template<typename MapTy>
size_t ngram_catalog( char * data, size_t data_size,
		      MapTy & catalog, size_t chunk_size ) {
    // Create a reducer hyperobject and prime it with the existing content
    ngram_list_reducer<MapTy> reduce_catalog(1<<16);
    cilk::reducer< cilk::op_add<size_t> > reduce_num_ngrams(0);
    reduce_catalog.swap( catalog );

    char * const data_end = &data[data_size];
    char * split = data;

    while( split != data_end ) {
	// Split the data at the chunk_size.
        char * end = std::min(split + chunk_size, data_end);
	// Adjust the split to word boundaries.
	// end = std::find_if( end, data_end, IsSpace() );
	while( end != data_end && 
	       *end != ' ' && *end != '\t' &&
	       *end != '\r' && *end != '\n' )
	    ++end;
	if( end != data_end )
	    *end = '\0';

	// Process the chunk from split to end
	cilk_spawn [&] ( char * split, char * end ) {
	    ngram<MapTy::N> ng;

	    // TODO: is it better for locality to move toupper() into the
	    //       inner loop, transforming while checking?
	    // Or better: use a word_bank with self-allocated storage and
	    // replace_copy_if words (need to pay attention to allocation of
	    // word_bank).
	    // std::for_each( split, end, ToUpper() );
	    for( char *I=split; I != end; ++I ) {
		// TODO: check performance with std::for_each and std::find
		// but with optimized codes in functors.
		// *I = std::toupper( *I );
		// TODO: vectorize this expression
		if( *I >= 'a' && *I <= 'z' )
		    *I = ( *I - 'a' ) + 'A';
	    }

	    while( split != end ) {
		// Skip non-upper characters
		// split = std::find_if( split, end, IsUpper() );
		while( split != end && !(*split >= 'A' && *split <= 'Z') )
		    ++split;
		// Pass over word
		char * w = split;
		// split = std::find_if_not( split, end, IsUpperOrQuote() );
		while( split != end
		       && ((*split >= 'A' && *split <= 'Z') || *split == '\'') )
		    ++split;
		*split = '\0'; // terminate

		if( split != w ) {
		    if( ng.push_back( reduce_catalog.store( w, split-w ) ) ) {
			reduce_catalog.index( ng );
			*reduce_num_ngrams += 1;
		    }
		}
	    }
        }( split, end );
        
        split = end;
    }
    cilk_sync;

    reduce_catalog.swap( catalog );
    return reduce_num_ngrams.get_value();
}


} // namespace text

namespace internal {

template<typename To, typename From>
void move_word_container( To & to, From && from ) {
    sizeable_methods<To>::reserve( to, to.size() + from.size() );
    to.insert( std::move(from) );
}

template<typename To, typename From,
	 typename = typename std::enable_if<std::is_same<To,From>::value>::type>
void move_word_container( To & to, From && from ) {
    if( to.empty() ) {
	to.swap( from );
    } else {
	sizeable_methods<To>::reserve( to, to.size() + from.size() );
	to.insert( std::move(from) );
    }
}

} // namespace internal

template<typename InternalContainerTy,
	 typename WordContainerTy = InternalContainerTy>
typename std::enable_if<!std::is_same<InternalContainerTy,WordContainerTy>::value, size_t>::type
word_catalog( const std::string & filename,
	      WordContainerTy & word_container,
	      size_t chunk_size = size_t(1)<<20 ) {
    typedef InternalContainerTy word_container_type;
    word_container_type intl_container;
    word_container_file_builder<word_container_type>
	builder( filename, intl_container );
    size_t nwords =
	text::word_catalog( builder.get_buffer(),
			    builder.get_buffer_end()-builder.get_buffer(),
			    builder.get_word_list(), chunk_size );

    internal::move_word_container( word_container, std::move(intl_container) );
    intl_container.mark_clear();

    return nwords;
}

template<typename InternalContainerTy,
	 typename WordContainerTy = InternalContainerTy>
typename std::enable_if<std::is_same<InternalContainerTy,WordContainerTy>::value, size_t>::type
word_catalog( const std::string & filename,
	      WordContainerTy & word_container,
	      size_t chunk_size = size_t(1)<<20 ) {
    typedef WordContainerTy word_container_type;
    word_container_file_builder<word_container_type>
	builder( filename, word_container );
    return text::word_catalog( builder.get_buffer(),
			       builder.get_buffer_end()-builder.get_buffer(),
			       builder.get_word_list(), chunk_size );
}

template<typename InternalContainerTy,
	 typename WordContainerTy = InternalContainerTy>
typename std::enable_if<!std::is_same<InternalContainerTy,WordContainerTy>::value, size_t>::type
ngram_catalog( const std::string & filename,
	       WordContainerTy & word_container,
	       size_t chunk_size = size_t(1)<<20 ) {
    typedef InternalContainerTy word_container_type;
    word_container_type intl_container;
    word_container_file_builder<word_container_type>
	builder( filename, intl_container );
    size_t ngrams =
	text::ngram_catalog( builder.get_buffer(),
			    builder.get_buffer_end()-builder.get_buffer(),
			    builder.get_word_list(), chunk_size );

    internal::move_word_container( word_container, intl_container );

    return ngrams;
}

template<typename InternalContainerTy,
	 typename WordContainerTy = InternalContainerTy>
typename std::enable_if<std::is_same<InternalContainerTy,WordContainerTy>::value, size_t>::type
ngram_catalog( const std::string & filename,
	       WordContainerTy & word_container,
	       size_t chunk_size = size_t(1)<<20 ) {
    typedef WordContainerTy word_container_type;
    word_container_file_builder<word_container_type>
	builder( filename, word_container );
    return text::ngram_catalog( builder.get_buffer(),
				builder.get_buffer_end()-builder.get_buffer(),
				builder.get_word_list(), chunk_size );
}


template<typename MapTy>
struct SizeCounter {
    size_t size;

    SizeCounter() : size( 0 ) { }
    void operator () ( const MapTy & map ) { size += map.size(); }
};

template<typename Iterator, typename = void>
struct its_const_type {
    typedef decltype(Iterator()->cbegin()) const_iterator;
};

template<typename Iterator, typename = void>
struct its_type {
    typedef decltype(Iterator()->cbegin()) const_iterator;
    typedef decltype(Iterator()->begin()) iterator;
};

// Auxiliary struct to calculate across a set of word_map's
// first: how many time each word occurs across the corpus
// second: in how many word_map's does the word occur
template<typename Type, typename IndexType>
struct appear_count {
    typedef Type type;
    typedef IndexType index_type;

    type first;		// how many columns does the word appear in
    index_type second;	// unique ID

    appear_count() { }
    appear_count(type f) : first(f) { }

    appear_count & operator += ( const appear_count & ac ) {
	first += ac.first;
	// second += ac.second;
	return *this;
    }

    appear_count & operator += ( const type & t ) {
	++first; // appears, don't care how often
	return *this;
    }
    appear_count & operator ++ () {
	++first;
	// ++second;
	return *this;
    }
};

template<typename Type, typename IndexType>
std::ostream & operator << ( std::ostream & os, const appear_count<Type,IndexType> & ac ) {
    return os << "{appear=" << ac.first << ", id=" << ac.second << '}';
}

template<typename Integral, typename Iterator>
struct ii_pair {
    Integral i;
    Iterator I;
    size_t s;

    ii_pair( Integral i_, const Iterator & I_, size_t s_ )
	: i(i_), I(I_), s(s_) { }

    ii_pair & operator = ( const ii_pair & p ) { i=p.i; I=p.I; return *this; }
    ii_pair & operator ++ () { i++; std::advance(I,s); return *this; }
    ii_pair & operator += ( size_t ss ) {
	i += ss;
	std::advance(I, ss*s);
    }
    bool operator != ( const ii_pair & p ) const {
	return i != p.i;
    }
    bool operator < ( const ii_pair & p ) const {
	return i < p.i;
    }
    Integral operator - ( const ii_pair & p ) const {
	return p.i - i;
    }
};

namespace internal {

template<typename Iterator>
void clear_ids( Iterator I, Iterator E ) {
    for( Iterator JI=I; JI != E; ++JI )
	JI->second.second = 0;
}

template<typename Iterator,
	 typename = typename std::enable_if<
	     std::is_same<typename std::iterator_traits<Iterator>::iterator_tag,
			  std::random_access_iterator_tag>::value>::type>
void clear_ids( Iterator I, Iterator E ) {
    cilk_for( Iterator JI=I; JI != E; ++JI )
	JI->second.second = 0;
}

template<typename Iterator, typename Functor>
void assign_ids_if( Iterator I, Iterator E, Functor F ) {
    decltype(I->second.second) uniq_id = 0;
    for( Iterator JI=I; JI != E; ++JI )
	if( F( JI->first, JI->second.first ) )
	    JI->second.second = uniq_id++;
}

template<typename Iterator>
void assign_ids( Iterator I, Iterator E ) {
    decltype(I->second.second) uniq_id = 0;
    for( Iterator JI=I; JI != E; ++JI )
	JI->second.second = uniq_id++;
}

template<typename Iterator,
	 typename = typename std::enable_if<
	     std::is_same<typename std::iterator_traits<Iterator>::iterator_tag,
			  std::random_access_iterator_tag>::value>::type>
void assign_ids( Iterator I, Iterator E ) {
    cilk_for( Iterator JI=I; JI != E; ++JI )
	JI->second.second = std::distance(JI,I);
}

} // internal

template<bool enable_bin_search, typename lookup_type>
typename std::enable_if<enable_bin_search, typename lookup_type::const_iterator>::type
tfidf_lookup( lookup_type & joint_word_map, const char * key, bool is_sorted ) {
    return is_sorted
	? joint_word_map.binary_search( key )
	: joint_word_map.find( key );
}

template<bool enable_bin_search, typename lookup_type>
typename std::enable_if<!enable_bin_search, typename lookup_type::const_iterator>::type
tfidf_lookup( lookup_type & joint_word_map, const char * key, bool is_sorted ) {
    return joint_word_map.find( key );
}

template<bool enable_bin_search, typename lookup_type>
typename std::enable_if<enable_bin_search, typename lookup_type::const_iterator>::type
tfidf_lookup( lookup_type & joint_word_map, const text::ngram<lookup_type::N> & key, bool is_sorted ) {
    return is_sorted
	? joint_word_map.binary_search( key )
	: joint_word_map.find( key );
}

template<bool enable_bin_search, typename lookup_type>
typename std::enable_if<!enable_bin_search, typename lookup_type::const_iterator>::type
tfidf_lookup( lookup_type & joint_word_map, const text::ngram<lookup_type::N> & key, bool is_sorted ) {
    return joint_word_map.find( key );
}


template<bool WordContSameAsLookup, typename ValueTy, typename IndexTy,
	 typename InputIterator, typename WordLookupTy>
void
tfidf_map_word( ValueTy *v, IndexTy *c, InputIterator MI,
		WordLookupTy & joint_word_map,
		size_t num_points, bool is_sorted ) {
    typedef ValueTy value_type;

    // Should always find the word!
    typename WordLookupTy::const_iterator F
	= tfidf_lookup<
	    /*std::is_same<WordContainerTy,WordLookupTy>::value*/
	    WordContSameAsLookup>( joint_word_map, MI->first, is_sorted );
    assert( F != joint_word_map.cend() );

    size_t tcount = F->second.first;
    size_t id = F->second.second;

    size_t tf = MI->second;
    value_type norm
	= log10(value_type(num_points + 1) / value_type(tcount + 1)); 
    *c = id;
    *v = value_type(tf) * norm; // tfidf
}

template<bool WordContSameAsLookup, typename ValueTy, typename IndexTy,
	 typename InputIterator, typename WordLookupTy>
void
tfidf_map_catalog( ValueTy *v, IndexTy *c,
		   InputIterator I, InputIterator E,
		   WordLookupTy & joint_word_map,
		   size_t num_points, bool is_sorted ) {
    size_t f = 0;
    for( InputIterator MI=I, ME=E; MI != ME; ++MI ) {
	tfidf_map_word<WordContSameAsLookup>( &v[f], &c[f], MI, joint_word_map,
					      num_points, is_sorted );
	++f;
    }
}

template<bool WordContSameAsLookup, typename ValueTy, typename IndexTy,
	 typename InputIterator, typename WordLookupTy,
	 typename = typename std::enable_if<
	     std::is_same<typename std::iterator_traits<InputIterator>::iterator_tag,
			  std::random_access_iterator_tag>::value>::type>
void
tfidf_map_catalog( ValueTy *v, IndexTy *c,
		   InputIterator I, InputIterator E,
		   WordLookupTy & joint_word_map,
		   size_t num_points, bool is_sorted ) {
    typedef ValueTy value_type;

    if( std::distance( MI, ME ) > 1000 ) {
	cilk_for( InputIterator MI=I, ME=E; MI != ME; ++MI ) {
	    size_t f = std::distance( I, MI ); // O(1) for random access iterator
	    tfidf_map_word<WordContSameAsLookup>( &v[f], &c[f], MI, joint_word_map,
						  num_points, is_sorted );
	}
    } else {
	size_t f = 0;
	for( InputIterator MI=I, ME=E; MI != ME; ++MI ) {
	    tfidf_map_word<WordContSameAsLookup>( &v[f], &c[f], MI, joint_word_map,
						  num_points, is_sorted );
	    ++f;
	}
    }
}

template<typename VectorTy, typename InputIterator, typename WordContainerTy,
	 typename WordLookupTy, typename VectorNameTy>
data_set<VectorTy, WordContainerTy, VectorNameTy>
tfidf( InputIterator I, InputIterator E,
       std::shared_ptr<WordContainerTy> & joint_word_map_ptr,
       WordLookupTy & joint_word_lookup,
       std::shared_ptr<VectorNameTy> & vec_names_ptr,
       bool is_sorted, bool iterate_ascending ) {
    typedef data_set<VectorTy, WordContainerTy, VectorNameTy> data_set_type;
    typedef typename data_set_type::vector_list_type vector_list_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef WordLookupTy lookup_type;
    typedef typename vector_list_type::value_type value_type;
    typedef typename vector_list_type::index_type index_type;

    // Reference to work with
    lookup_type & joint_word_map = joint_word_lookup; // *joint_word_map_ptr;

    // Get statistics on input word maps
    size_t num_points = std::distance( I, E );
    size_t num_dimensions = joint_word_map.size();
    size_t nonzeros = std::for_each( I, E, SizeCounter<decltype(*I)>() ).size;

    // Construct set of vectors, either dense or sparse
    static_assert( is_sparse_vector<VectorTy>::value, "must be sparse - constructor" );
    std::shared_ptr<vector_list_type> vectors_ptr
	= std::make_shared<vector_list_type>( num_points, num_dimensions, nonzeros );
    vector_list_type & vectors = *vectors_ptr;

    // Prepare for parallel access
    size_t * vec_start = new size_t[num_points];
    size_t inc_nonzeros = 0;
    size_t i=0;
    for( auto II=I; II != E; ++II, ++i ) {
	size_t fcount = II->size();
	vec_start[i] = inc_nonzeros;
	inc_nonzeros += fcount;
	
	vectors.emplace_back( num_dimensions, fcount );
    }
    assert( nonzeros == inc_nonzeros );

#if 0 // do this before calling tfidf
    // Assign unique IDs to each word
#if 1
    internal::assign_ids( joint_word_map.begin(), joint_word_map.end() );
#else
    // Parallelize by explicitly identifying chunks, iteration range is IDs!
    // and std::next from begin once per chunk, then iterate step by step
    // This hardly provides speedup, and on a single thread it is much slower,
    // because std::advance on a std::map::iterator is very slow. The first
    // split halfway-through already takes half the time of the sequential
    // loop above and we haven't achieved anything yet!
    {
	index_type max_id = joint_word_map.size();
	index_type id_step = 4096;
	index_type nsteps = (max_id+id_step-1) / id_step;
	// cilk_for( index_type id=0; id < max_id; id += id_step, std::advance(Jid,id_step) ) {
	cilk_for( ii_pair<index_type, typename index_list_type::iterator>
		  ii(index_type(0), joint_word_map.begin(), id_step),
		  ie(nsteps, joint_word_map.end(), id_step); ii < ie; ++ii ) {
	    index_type my_id = ii.i;
	    index_type last_id = my_id + id_step;
	    for( typename index_list_type::iterator
		     JI=ii.I, JE=joint_word_map.end();
		 my_id < last_id && JI != JE; ++my_id, ++JI )
		JI->second.second = my_id;
	}
    }
#endif
#endif

    // Calculate TF/IDF scores
    cilk_for( size_t i=0; i < num_points; ++i ) {
	auto PI = std::next( I, i ); // Get word map to operate on
	size_t fcount = PI->size();

	value_type *v = &vectors.get_alloc_v()[vec_start[i]];
	index_type *c = &vectors.get_alloc_i()[vec_start[i]];

	tfidf_map_catalog<
	    std::is_same<WordContainerTy,WordLookupTy>::value>(
		v, c, PI->cbegin(), PI->cend(), joint_word_map,
		num_points, is_sorted );

	// In case of collections where IDs have not been assigned in the
	// natural iteration order, we need to now sort the sparse vectors.
	if( !iterate_ascending )
	    vectors[i].sort_by_index();
    }

    delete[] vec_start;

    const char * name = "tfidf";
    return data_set_type( name, joint_word_map_ptr, vec_names_ptr, vectors_ptr,
			  false );
}

template<typename VectorTy, typename InputIterator, typename WordContainerTy,
	 typename VectorNameTy>
data_set<VectorTy, WordContainerTy, VectorNameTy>
tfidf( InputIterator I, InputIterator E,
       std::shared_ptr<WordContainerTy> & joint_word_map_ptr,
       std::shared_ptr<VectorNameTy> & vec_names_ptr,
       bool is_sorted, bool iterate_ascending ) {
    return tfidf<VectorTy, InputIterator, WordContainerTy, WordContainerTy,
		 VectorNameTy>( I, E, joint_word_map_ptr, *joint_word_map_ptr,
				vec_names_ptr, is_sorted, iterate_ascending );
}

/*
 * tfidf_by_words: construct TF/IDF scores and structure output as one vector
 *                 per word.
 *
 * Pre-requisite: every input map (iterated over by InputIterator) must be
 * searchable with find(), so they must be sorted for binary search to work.
 */
template<typename VectorTy, typename InputIterator, typename WordContainerTy,
	 typename VectorNameTy>
data_set<VectorTy, WordContainerTy, VectorNameTy>
tfidf_by_words( InputIterator I, InputIterator E,
		std::shared_ptr<WordContainerTy> & joint_word_map_ptr,
		std::shared_ptr<VectorNameTy> & vec_names_ptr,
		bool is_sorted = true ) {
    typedef data_set<VectorTy, WordContainerTy, VectorNameTy> data_set_type;
    typedef typename data_set_type::vector_list_type vector_list_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_list_type::value_type value_type;
    typedef typename vector_list_type::index_type index_type;

    // Reference to work with
    index_list_type & joint_word_map = *joint_word_map_ptr;

    // Get statistics on input word maps
    size_t num_dimensions = std::distance( I, E );
    size_t num_points = joint_word_map.size();
    // TODO: parallelize if std::distance(I,E) large enough
    size_t nonzeros = std::for_each( I, E, SizeCounter<decltype(*I)>() ).size;

    // Construct set of vectors, either dense or sparse
    static_assert( is_sparse_vector<VectorTy>::value, "must be sparse - constructor" );
    std::shared_ptr<vector_list_type> vectors_ptr
	= std::make_shared<vector_list_type>( num_points, num_dimensions, nonzeros );
    vector_list_type & vectors = *vectors_ptr;

    // Prepare for parallel access. Vectors are by word, so we need to know
    // the number of files each word occurs in.
    // As we are iterating over all word in this version of TF/IDF, assign
    // unique IDs to each word in the process.
    size_t * vec_start = new size_t[num_points];
    size_t inc_nonzeros = 0;
    size_t i=0;
    decltype(joint_word_map.begin()->second.second) uniq_id = 0;
    // TODO: measure time spent specifically in the next loop
    // 	     if promising, consider parallelising this loop
    for( typename index_list_type::iterator JI=joint_word_map.begin(),
	     JE=joint_word_map.end(); JI != JE; ++JI, ++i ) {
	size_t fcount = JI->second.first; // number of documents containing word
	vec_start[i] = inc_nonzeros;
	inc_nonzeros += fcount;

	vectors.emplace_back( num_dimensions, fcount );

	JI->second.second = uniq_id++; // set unique ID for the word
    }
    assert( nonzeros == inc_nonzeros );

    // Counters for concurrent access
    size_t * word_ctr = new size_t [num_points];
    std::fill( &word_ctr[0], &word_ctr[num_points], 0 );

    // Calculate word-to-file mapping
    // For each file
    cilk_for( size_t i=0; i < num_dimensions; ++i ) {
	// The i-th file
	auto PI = std::next( I, i );

	// For each word in the file
	for( typename its_const_type<InputIterator>::const_iterator
		 MI=PI->cbegin(), ME=PI->cend(); MI != ME; ++MI ) {
	    // Should always find the word!
	    typename index_list_type::const_iterator F = is_sorted
		? joint_word_map.binary_search( MI->first )
		: joint_word_map.find( MI->first );
	    assert( F != joint_word_map.cend() );

	    size_t fcount = F->second.first; // Number of files involved in.
	    value_type norm
		= log10(value_type(num_dimensions + 1) / value_type(fcount + 1)); 
	    size_t id = F->second.second; // Word ID
	
	    size_t pos = __sync_fetch_and_add( &word_ctr[id], 1 );
	    assert( pos < fcount );

	    value_type *v = &vectors.get_alloc_v()[vec_start[id]];
	    index_type *c = &vectors.get_alloc_i()[vec_start[id]];

	    size_t tf = MI->second; // Term frequency
	    v[pos] = value_type(tf) * norm; // tfidf
	    c[pos] = i;
	}
    }

    // Note: sort vectors if needed, as we have stored them concurrently.
    cilk_for( size_t i=0; i < num_points; ++i )
	vectors[i].sort_by_index();

    delete[] vec_start;
    delete[] word_ctr;

    const char * name = "tfidf-by-words";
    return data_set_type( name, joint_word_map_ptr, vec_names_ptr, vectors_ptr,
			  true );
}

template<typename VectorTy, typename InputIterator, typename WordContainerTy,
	 typename VectorNameTy>
data_set<VectorTy, WordContainerTy, VectorNameTy>
tfidf( InputIterator I, InputIterator E,
       std::shared_ptr<VectorNameTy> & vec_names_ptr,
       bool is_sorted = true ) {
    typedef data_set<VectorTy, WordContainerTy> data_set_type;
    typedef typename data_set_type::vector_list_type vector_list_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_list_type::value_type value_type;
    typedef typename vector_list_type::index_type index_type;

    // Construct joint word list.
    std::shared_ptr<index_list_type> joint_word_map_ptr
	= std::make_shared<index_list_type>();
    index_list_type & joint_word_map = *joint_word_map_ptr;
    for( auto II=I; II != E; ++II ) {
	joint_word_map.copy( *II );
    }

    return tfidf<VectorTy>( I, E, joint_word_map_ptr, vec_names_ptr, is_sorted );
}

template<typename ValueTy, typename InputIterator, typename WordContainerTy>
void
tfidf_inplace( InputIterator I, InputIterator E,
	       WordContainerTy & joint_word_map ) {
    typedef WordContainerTy index_list_type;
    typedef ValueTy value_type;

    // Get statistics on input word maps
    size_t num_points = std::distance( I, E );
    size_t num_dimensions = joint_word_map.size();
    size_t nonzeros = std::for_each( I, E, SizeCounter<decltype(*I)>() ).size;

    // Calculate TF/IDF scores
    // Inplace version: TF/IDF scores are overwritten in the word count fields
    // instead of creating new vectors
    // This should be better when doing, e.g., output next
    // because
    //   (i) no new storage required
    //  (ii) seq loop over maps for preparing vectors is avoided
    // (iii) seq loop over joint_word_map setting IDs is avoided
    cilk_for( size_t i=0; i < num_points; ++i ) {
	auto PI = std::next( I, i ); // Get word map to operate on
	size_t fcount = PI->size();

	size_t f = 0;
	for( typename its_type<InputIterator>::iterator
		 MI=PI->begin(), ME=PI->end(); MI != ME; ++MI ) {
	    // Should always find the word!
	    typename index_list_type::const_iterator F
		= joint_word_map.find( MI->first );
	    assert( F != joint_word_map.cend() );

	    size_t tcount = F->second.first; // second.second not needed

	    size_t tf = MI->second;
	    value_type norm
		= log10(value_type(num_points + 1) / value_type(tcount + 1)); 
	    MI->second = value_type(tf) * norm; // tfidf
	}
    }
}

template<typename Type1, typename Type2>
union union_of {
    typedef Type1 type1;
    typedef Type2 type2;

private:
    type1 var1;
    type2 var2;

public:
    union_of( const Type1 & v1 ) : var1( v1 ) { }
    // union_of( const Type2 & v2 ) : var2( v2 ) { }

    operator type1 () const { return var1; }
    operator type2 () const { return var2; }

    union_of & operator = ( const type1 & v1 ) { var1 = v1; return *this; }
    union_of & operator = ( const type2 & v2 ) { var2 = v2; return *this; }

    union_of & operator += ( const union_of & uo ) {
	// Assume int usage, really need user-defined reduction operator
	var1 += uo.var1;
	return *this;
    }

    union_of & operator ++ () {
	// Assume int usage, really need user-defined reduction operator
	++var1;
	return *this;
    }
};


} // namespace asap

#endif // INCLUDED_ASAP_WORD_COUNT_H
