/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_ARFF_H
#define INCLUDED_ASAP_ARFF_H

#include <stdexcept>
#include <cctype>
#include <cstdlib>

#include "asap/memory.h"
#include "asap/traits.h"
#include "asap/data_set.h"

namespace asap {

struct NZDelim {
    bool operator () ( char c ) const { 
	return ( c == ',' ) | ( c == '}' );
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

namespace arff {

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
    assert( *p != '{' );
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
	while( std::isspace( *p ) && *p != '\n' )
	    ADVANCE( p );
	if( *p == ',' )
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

    assert( *p == '{' );
    ++p;
    while( std::isspace( *p ) )
	++p;
    do {
	typename vector_type::index_type i = std::strtoul( p, &p, 10 );
	while( std::isspace( *p ) )
	    ++p;
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
	if( *p == '}' )
	    break;
	++p;
	while( std::isspace( *p ) )
	    ++p;
    } while( *p != '}' ); // && *p != '\n' );
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
	if( *p == '}' )
	    break;
	++p;
	++nvalues;
	prevp = p;
    }
    if( std::find_if( prevp, p, IsAlnum() ) != p )
	++nvalues;
    return nvalues;
}

#if 0
std::pair<size_t,size_t> count_values( char * p ) {
    size_t npoints = 0;
    size_t nvalues = 0;
    while( *p ) {
	bool seen = false;
	if( *p == '{' )
	    ++p; // skip opening {
	while( *p != '}' && *p != '\n' ) {
	    if( *p == '\0' )
		return std::make_pair( 0, 0 );
	    if( !std::isspace(*p) ) {
		if( seen && *p == ',' ) {
		    ++nvalues;
		    seen = false;
		} else
		    seen = true;
	    }
	    ++p;
	}
	while( *p != '\n' && *p )
	    ++p;
	// a comma between every two values, provided one seen since last comma
	nvalues += seen;
	++npoints;

	arff::skip_blank_lines( p );
    }
    return std::make_pair( npoints, nvalues );
}
#else
std::pair<size_t,size_t> count_values( const char * p, const char * end ) {
    size_t npoints = 0;
    size_t nvalues = 0;

    const char * prevp = p;
    while( (p=std::find_if( p, end, NZDelim() )) != end ) {
	if( *p == ',' ) {
	    ++nvalues;
	}
	else if( *p == '}' ) {
	    ++npoints;
	    if( std::find_if( prevp, p, IsAlnum() ) != p )
		++nvalues;
	}
	++p;
	prevp = p;
    }

    return std::make_pair( npoints, nvalues );
}
#endif


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
arff_read( const std::string & filename, bool &is_stored_sparse ) {
    typedef DataSetTy data_set_type;
    typedef typename data_set_type::vector_type vector_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename vector_type::index_type index_type;
    typedef typename vector_type::value_type value_type;

    std::shared_ptr<index_list_type> idx = std::make_shared<index_list_type>();
    word_container_file_builder<index_list_type> idx_builder( filename, *idx );

    std::shared_ptr<std::vector<vector_type>> vec_ptr
	= std::make_shared<std::vector<vector_type>>();
    std::vector<vector_type> & vec = *vec_ptr;

    const char * relation = "undefined";
    bool is_sparse = false;

    // Now parse the data
    char * p = idx_builder.get_buffer();
    char * end = idx_builder.get_buffer_end();
#define ADVANCE(pp) do { if( *(pp) == '\0' ) goto END_OF_FILE; ++pp; } while( 0 )
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

	    do {
		is_sparse |= *p == '{';
		// Create vector speculatively...
		create_vector<vector_type>( vec, p, end, ndim );
		if( !arff::read_vector( p, end, vec.back() ) ) {
		    // Oops, no vector after all...
		    // This should happen at most once per file
		    vec.pop_back();
		}
		arff::skip_blank_lines( p, end );
	    } while( *p != '\0' );
	}
    } while( 1 );
#undef ADVANCE

END_OF_FILE:

    is_stored_sparse = is_sparse;
    return data_set_type( relation, idx, vec_ptr );
}

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

		size_t num_points = 0;

		do {
		    is_sparse |= *p == '{';

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

}

#endif // INCLUDED_ASAP_ARFF_H
