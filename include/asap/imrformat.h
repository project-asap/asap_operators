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


#ifndef INCLUDED_ASAP_ARFF_H
#define INCLUDED_ASAP_ARFF_H

#include <stdexcept>
#include <cctype>
#include <cstdlib>
#include <array>

#include "asap/memory.h"
#include "asap/traits.h"
#include "asap/data_set.h"
#include "asap/word_count.h"

namespace asap {

struct NZDelim {
    bool operator () ( char c ) const { 
	return ( c == ',' ) | ( c == ')' );
    }
};

struct IsAlnum {
    bool operator () ( char c ) const { 
	return std::isalnum(c);
    }
};

struct NewLine {
    bool operator () ( char c ) const { 
	return c == '\n';
    }
};

struct NonBlank {
    bool operator () ( char c ) const { 
	return !std::isspace(c);
    }
};

namespace array {

// Skip all white-space (blank lines) as well as lines with comments
#if 1
void skip_blank_lines( char * & p, char * end ) {
    while( std::isspace(*p) )
	++p;
    while( *p == '%' ) {
	++p;
	while( *p != '\n' )
	    if( *p++ == '\0' )
		return;
	while( std::isspace(*p) )
	    ++p;
    }
}
#else
void skip_blank_lines( char * & p, char * end ) {
    if( (p = std::find_if( p, end, NonBlank() )) == end )
	return;
    while( *p == '%' ) {
	++p;
	if( (p = std::find_if( p, end, NewLine() )) == end )
	    return;
	++p;
	if( (p=std::find_if( p, end, NonBlank() )) == end )
	    return;
    }
}
#endif

// Update dense_vector
template<typename vector_type, typename = void>
class record_value {
    vector_type & vector;
    typename vector_type::index_type nexti;
public:
    record_value( vector_type & vector_ ) : vector( vector_ ), nexti( 0 ) { }

    void operator () ( typename vector_type::value_type v ) {
	assert( nexti < vector.length() ); // throw
	vector[nexti++] = v;
    }
    void operator () ( typename vector_type::index_type i,
		       typename vector_type::value_type v ) {
	assert( 0 <= i && i < vector.length() ); // throw
	vector[i] = v;
    }
};

// Update sparse_vector
template<typename vector_type>
class record_value<vector_type,
		   typename std::enable_if<is_sparse_vector<vector_type>::value>::type> {
    vector_type & vector;
    typename vector_type::index_type pos;
public:
    record_value( vector_type & vector_ ) : vector( vector_ ), pos( 0 ) { }

    void operator () ( typename vector_type::value_type v ) {
	assert( pos < vector.length() ); // throw
	vector.set( pos, v, pos );
	++pos;
    }
    void operator () ( typename vector_type::index_type i,
		       typename vector_type::value_type v ) {
	assert( 0 <= i && i < vector.length() ); // throw
	vector.set( pos++, v, i );
    }
};


template<typename vector_type>
bool
read_dense_vector( char *& p, char * end, vector_type & vector ) {
#define ADVANCE(pp) do { if( *(pp) == '\0' ) { return false; } ++pp; } while( 0 )
    // assert( *p != '[' );
    while( *p != '[' )
        ADVANCE( p );
    ADVANCE( p );
    record_value<vector_type> recorder( vector );
    do {
	while( std::isspace( *p ) )
	    ADVANCE( p );
	if( *p == '?' )
	    fatal( "missing data not supported" );
	typename vector_type::value_type v = 0;
#if REAL_IS_INT
	v = std::strtoul( p, &p, 10 );
#else
	v = std::strtod( p, &p );
#endif
	recorder( v );
	// while( std::isspace( *p ) && *p != '\n' )
	while( ( std::isspace( *p ) || *p == ',' || *p == ']' || *p == ')' ) && (*p != '\n') )
	    ADVANCE( p );
	while( std::isspace( *p ) && *p != '\n' )
	    ADVANCE( p );
    } while( *p != '\n' );
    return true;
#undef ADVANCE
}

template<typename vector_type>
bool
read_sparse_vector( char *& pref, char * end, vector_type & vector ) {
    // We will only be overwriting a few elements, so clear everything first
    if( is_dense_vector<vector_type>::value )
	vector.clear();

    record_value<vector_type> recorder( vector );

    char * p = pref;

    while( *p != '[' )
        ++p;
 
    assert( *p == '[' );
    ++p;
    while( std::isspace( *p ) )
	++p;
    do {
	typename vector_type::index_type i = std::strtoul( p, &p, 10 );
	while( std::isspace( *p ) || *p == ':' )
	    ++p;
	if( *p == ']' )
	    break;
	if( *p == '?' )
	    fatal( "missing data not supported" );
	typename vector_type::value_type vv = 0;
#if REAL_IS_INT
	vv = std::strtoul( p, &p, 10 );
#else
	vv = std::strtod( p, &p );
#endif
	// vector[i] = vv;
	recorder( i, vv );

	if( (p = std::find_if( p, end, NZDelim() )) == end ) {
	    pref = p;
	    return false;
	}
	if( *p == ')' )
	    break;
	++p;
	while( std::isspace( *p ) )
	    ++p;
    } while( *p != ')' ); // && *p != '\n' );
    ++p;
    pref = p;
    return true;
}

template<typename vector_type>
bool read_vector( char *& p, char * end, vector_type & vector ) {
    if( *p == '{' ) 
	return read_sparse_vector( p, end, vector );
    else
	return read_dense_vector( p, end, vector );
}

template<typename WordBankTy>
const char * read_relation( char *& p, WordBankTy & idx ) {
    while( std::isspace(*p) && *p != '\n' )
	++p;
    char * relation = p;
    if( *p == '\'' ) { // scan until closing quote
	++p;
	while( *p != '\'' ) {
	    if( *p == '\\' ) // skip next character
		++p;
	    if( *p == '\0' )
		return 0;
	    ++p;
	}
	// Skip closing quote
	++p;
    } else { // scan until space
	while( !std::isspace( *p ) ) {
	    if( *p == '\0' )
		return 0;
	    ++p;
	}
    }
    const char * stored
	= idx.memorize( relation, p-relation ); // Store relation, not indexed
    ++p; // delimiter replaced by '\0'
    return stored;
}

template<typename WordBankTy>
const char * read_attribute( char *& p, WordBankTy & idx ) {
    // Isolate token
    while( std::isspace( *p ) )
	++p;
    char * name = p;
    while( !std::isspace( *p ) )
	if( *p++ == '\0' )
	    return 0;
    const char * stored = idx.index( name, p-name ); // Store name in index
    ++p; // space replaced by '\0'

    // Isolate type
    while( std::isspace( *p ) )
	++p;
    char * type = p;
    while( !std::isspace( *p ) )
	if( *p++ == '\0' )
	    return 0;
    // Delineate end of data type by overwriting space
    *p++ = '\0';
    if( strcmp( type, "numeric" ) ) {
	std::cerr << "Warning: treating non-numeric attribute '"
		  << stored << "' of type '" << type << "' as numeric\n";
    }

    return stored; // ignoring type as we only consider numeric types ...
}
template<typename WordBankTy>
const char * read_column_header( char *& p, WordBankTy & idx ) {
    char * name = p;
    const char * stored = idx.index( name, p-name ); // Store name in index
    return stored; // ignoring type as we only consider numeric types ...
}

}

size_t count_lines( const char * p ) {
    size_t nlines = 0;
    while( *p ) {
	if( *p == '\n' )
	    ++nlines;
	++p;
    }
    return nlines;
}

size_t count_nonzeros( const char * p, const char * end ) {
    size_t nvalues = 0;
    ++p; // skip opening {
    const char * prevp = p;
    while( (p = std::find_if( p, end, NZDelim() )) != end ) {
	if( *p == ']' )
	    break;
	++p;
	++nvalues;
	prevp = p;
    }
    if( std::find_if( prevp, p, IsAlnum() ) != p )
	++nvalues;
    return nvalues;
}

std::pair<size_t,size_t> count_values( const char * p, const char * end ) {
    size_t npoints = 0;
    size_t nvalues = 0;

    const char * prevp = p;
    while( (p=std::find_if( p, end, NZDelim() )) != end ) {
	if( *p == ',' ) {
	    ++nvalues;
	}
	else if( *p == ']' ) {
	    ++npoints;
	    if( std::find_if( prevp, p, IsAlnum() ) != p )
		++nvalues;
	}
	++p;
	prevp = p;
    }

    return std::make_pair( npoints, nvalues );
}

template<typename VectorTy, typename Container>
typename std::enable_if<is_dense_vector<VectorTy>::value>::type
create_vector( Container & container, const char * p, const char * end,
	       size_t ndim ) {
    container.emplace_back( ndim );
}

template<typename VectorTy, typename Container>
typename std::enable_if<is_sparse_vector<VectorTy>::value>::type
create_vector( Container & container, const char * p, const char * end,
	       size_t ndim ) {
    container.emplace_back( ndim, count_nonzeros( p, end ) );
}

template<typename VectorTy, typename Container>
typename std::enable_if<is_dense_vector<VectorTy>::value>::type
init_vector( Container & container, const char * p, const char * end,
	     size_t ndim ) {
}

template<typename VectorTy, typename Container>
typename std::enable_if<is_sparse_vector<VectorTy>::value>::type
init_vector( Container & container, const char * p, const char * end,
	     size_t ndim ) {
    container.emplace_back( ndim, count_nonzeros( p, end ) );
}


// Basic implementation where vectors are individually allocated
template<typename DataSetTy>
typename std::enable_if<
    std::is_same<typename DataSetTy::vector_type::memory_mgmt_type, mm_ownership_policy>::value,
    DataSetTy>::type
array_read( const std::string & filename, bool &is_stored_sparse ) {
    typedef DataSetTy data_set_type;
    typedef typename data_set_type::vector_type vector_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_type::index_type index_type;
    typedef typename vector_type::value_type value_type;
    typedef index_list_type word_container_type;

    std::shared_ptr<index_list_type> idx = std::make_shared<index_list_type>();

    word_container_type &                 m_container = *idx;


    word_container_file_builder<index_list_type> idx_builder( filename, *idx );

    std::shared_ptr<std::vector<vector_type>> vec_ptr
	= std::make_shared<std::vector<vector_type>>();
    std::vector<vector_type> & vec = *vec_ptr;

    const char * relation = "undefined";
    bool is_sparse = false;

    // Now parse the data
    char * p = idx_builder.get_buffer();
    char * end = idx_builder.get_buffer_end();

    // Fudge fake id's as we don't care about them for ASAP workflow
    char labels[24][10];
    for(int i = 0; i < 24; ++i)
    {
        sprintf(labels[i],"%d",i+1);
    }
    for(auto text : labels) {   // Range-for!
       const char * stored = (*idx).index( text, strlen(text)+1 ); // Store name in index
    }
#define ADVANCE(pp) do { if( *(pp) == '\0' ) goto END_OF_FILE; ++pp; } while( 0 )
    do {
        array::skip_blank_lines( p, end );
        while( *p != '(' )
            ADVANCE( p );
        ADVANCE( p );

#ifdef IMR
            int ndim = 24;
#else
            int ndim = idx_builder.size();
#endif
            p += 4;

	    array::skip_blank_lines( p, end );

	    do {
		is_sparse |= *p == '{';
		// Create vector speculatively...
		create_vector<vector_type>( vec, p, end, ndim );
		if( !array::read_vector( p, end, vec.back() ) ) {
		    // Oops, no vector after all...
		    // This should happen at most once per file
		    vec.pop_back();
		}
		array::skip_blank_lines( p, end );
	    } while( *p != '\0' );
    } while( 1 );
#undef ADVANCE

END_OF_FILE:

    is_stored_sparse = is_sparse;
    return data_set_type( relation, idx, vec_ptr );


    is_stored_sparse = is_sparse;
    return data_set_type( relation, idx, vec_ptr );
}

#ifndef IMR
// Specialization for dense vectors without ownership. Ownership of the
// vector contents are referred to the data_set for efficiency reasons.
template<typename DataSetTy>
typename std::enable_if<
    std::is_same<typename DataSetTy::vector_type::memory_mgmt_type, mm_no_ownership_policy>::value,
    DataSetTy>::type
arff_read( const std::string & filename, bool &is_stored_sparse ) {
    typedef DataSetTy data_set_type;
    typedef typename data_set_type::vector_type vector_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_type::index_type index_type;
    typedef typename vector_type::value_type value_type;
    typedef typename data_set_type::vector_list_type vector_set_type;

    std::shared_ptr<index_list_type> idx = std::make_shared<index_list_type>();
    word_container_file_builder<index_list_type> idx_builder( filename, *idx );

    const char * relation = "undefined";
    bool is_sparse = false;

    // Now parse the data
    char * p = idx_builder.get_buffer();
    char * end = idx_builder.get_buffer_end();
#define ADVANCE(pp) do { if( *(pp) == '\0' ) goto END_OF_FILE; ++pp; } while( 0 )
    size_t num_points;
    do {
	arff::skip_blank_lines( p, end );
	while( *p != '@' )
	    ADVANCE( p );
	ADVANCE( p );
	if( !strncasecmp( p, "relation ", 9 ) ) {
	    p += 9;
	    if( !(relation = arff::read_relation( p, idx_builder.get_word_list() )) )
		fatal( "Incomplete relation specifier in input file '",
		       filename, "'" );
	} else if( !strncasecmp( p, "attribute ", 10 ) ) {
	    p += 10;
	    if( !arff::read_attribute( p, idx_builder.get_word_list() ) )
		fatal( "Incomplete attribute specifier in input file '",
		       filename, "'" );
	} else if( !strncasecmp( p, "data", 4 ) ) {
	    // From now on everything is data
	    int ndim = idx_builder.size();
	    p += 4;

	    arff::skip_blank_lines( p, end );

	    return [&]() -> data_set_type {
		// Estimate upper bound on space needed to store all vectors.
		// Assumptions:
		//   + At most one vector per line
		//   + Dense vectors, so fixed vector size
		std::pair<size_t,size_t> info = count_values(p, end);
		// size_t max_points = count_lines(p);
		size_t max_points = info.first;
		size_t max_values = info.second;
		std::shared_ptr<vector_set_type> dvs_ptr
		    = std::make_shared<vector_set_type>(
			max_points, is_sparse_vector<vector_type>::value ?
			max_values : ndim );
		vector_set_type & dvs = *dvs_ptr;
		dvs.clear(); // zero-init

		if( !max_points )
		    goto END_OF_FILE;

		num_points = 0;

		do {
		    is_sparse |= *p == '(';

		    assert( num_points < max_points );
		    init_vector<vector_type>( dvs, p, end, ndim );
		    if( !arff::read_vector( p, end, dvs[num_points++] ) ) {
			// Oops, no vector after all...
			// This should happen at most once per file.
			--num_points;
		    }
		    arff::skip_blank_lines( p, end );
		} while( *p != '\0' );
	    END_OF_FILE:
		dvs.trim_number( num_points );
		is_stored_sparse = is_sparse;
		return data_set_type( relation, idx, dvs_ptr );
	    }();
	}
    } while( 1 );
#undef ADVANCE

END_OF_FILE:

    is_stored_sparse = is_sparse;
    return data_set_type( relation, idx, std::make_shared<vector_set_type>( 0, 0 ) );
}
#endif

// Version for IMR input format to k-means, basic array format
// Specialization for dense vectors without ownership. Ownership of the
// vector contents are referred to the data_set for efficiency reasons.
template<typename DataSetTy>
typename std::enable_if<
    std::is_same<typename DataSetTy::vector_type::memory_mgmt_type, mm_no_ownership_policy>::value,
    DataSetTy>::type
array_read( std::string & filename, bool &is_stored_sparse ) {
    typedef DataSetTy data_set_type;
    typedef typename data_set_type::vector_type vector_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_type::index_type index_type;
    typedef typename vector_type::value_type value_type;
    typedef typename data_set_type::vector_list_type vector_set_type;

    std::shared_ptr<index_list_type> idx = std::make_shared<index_list_type>();
    word_container_file_builder<index_list_type> idx_builder( filename, *idx );

    const char * relation = "undefined";
    bool is_sparse = false;

    // Now parse the data
    char * p = idx_builder.get_buffer();
    char * end = idx_builder.get_buffer_end();
#define ADVANCE(pp) do { if( *(pp) == '\0' ) goto END_OF_FILE; ++pp; } while( 0 )
    size_t num_points;
    do {
	array::skip_blank_lines( p, end );
	while( *p != '(' )
	    ADVANCE( p );
	ADVANCE( p );
	    int ndim = idx_builder.size();
	    p += 4;

	    // array::skip_blank_lines( p, end );

	    return [&]() -> data_set_type {
		// Estimate upper bound on space needed to store all vectors.
		// Assumptions:
		//   + At most one vector per line
		//   + Dense vectors, so fixed vector size
		std::pair<size_t,size_t> info = count_values(p, end);
		// size_t max_points = count_lines(p);
		size_t max_points = info.first;
		size_t max_values = info.second;
		std::shared_ptr<vector_set_type> dvs_ptr
		    = std::make_shared<vector_set_type>(
			max_points, is_sparse_vector<vector_type>::value ?
			max_values : ndim );
		vector_set_type & dvs = *dvs_ptr;
		dvs.clear(); // zero-init

		if( !max_points )
		    goto END_OF_FILE;

		num_points = 0;

		do {
		    is_sparse |= *p == '(';

		    assert( num_points < max_points );
		    init_vector<vector_type>( dvs, p, end, ndim );
		    if( !array::read_vector( p, end, dvs[num_points++] ) ) {
			// Oops, no vector after all...
			// This should happen at most once per file.
			--num_points;
		    }
		    array::skip_blank_lines( p, end );
		} while( *p != '\0' );
	    END_OF_FILE:
		dvs.trim_number( num_points );
		is_stored_sparse = is_sparse;
		return data_set_type( relation, idx, dvs_ptr );
	    }();
    } while( 1 );
#undef ADVANCE

END_OF_FILE:

    is_stored_sparse = is_sparse;
    return data_set_type( relation, idx, std::make_shared<vector_set_type>( 0, 0 ) );
}

namespace arff {

template<typename Type>
const char * as_word( const Type & val ) {
    return val;
}

template<typename Type1, typename Type2>
const char * as_word( const std::pair<Type1,Type2> & val ) {
    return val.first;
}

template<typename Type>
const char * get_value( const Type & val ) {
    return "";
}

template<typename Type1, typename Type2>
const Type2 & get_value( const std::pair<Type1,Type2> & val ) {
    return val.second;
}

template<typename Type1, typename Type2>
std::ostream &
operator << ( std::ostream & os, const std::pair<Type1,Type2> & val ) {
    return os << as_word( val );
}

template<size_t N, typename Type2>
std::ostream &
operator << ( std::ostream & os,
	      const std::pair<asap::text::ngram<N>,Type2> & val ) {
    for( size_t i=0; i < N; ++i ) {
	os << as_word( val.first[i] );
	if( i < N-1 )
	    os << '#';
    }
    return os;
}


template<typename VectorIter, typename ColNameIter, typename RowNameIter>
void arff_write( std::ostream & of,
		 const char * const relation_name,
		 VectorIter vI, VectorIter vE,
		 ColNameIter cI, ColNameIter cE,
		 RowNameIter rI, RowNameIter rE ) {
    of << "@relation " << relation_name;

    for( auto I=cI; I != cE; ++I )
	of << "\n\t@attribute " << *I
	   << " numeric % value=" << get_value(*I);

    of << "\n\n@data";

    for( auto I=vI; I != vE; ++I, ++rI )
	of << "\n\t" << *I << " % " << *rI;

    of << std::endl;
}

};

template<typename DataSetTy>
void arff_write( std::ostream & os,
		 const DataSetTy & data_set ) {
    if( data_set.transpose() ) {
	arff::arff_write( os, data_set.get_relation(),
			  data_set.vector_cbegin(), data_set.vector_cend(), 
			  data_set.index2_cbegin(), data_set.index2_cend(),
			  data_set.index_cbegin(), data_set.index_cend() );
    } else {
	arff::arff_write( os, data_set.get_relation(),
			  data_set.vector_cbegin(), data_set.vector_cend(), 
			  data_set.index_cbegin(), data_set.index_cend(),
			  data_set.index2_cbegin(), data_set.index2_cend() );
    }
}

template<typename DataSetTy>
void arff_write( const std::string & filename,
		 const DataSetTy & data_set ) {
    if( !strcmp( filename.c_str(), "-" ) )
	arff_write( std::cout, data_set );
    else {
	std::ofstream of( filename, std::ios_base::out );
	arff_write( of, data_set );
	of.close();
    }
}

}

#endif // INCLUDED_ASAP_ARFF_H
