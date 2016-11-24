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


#ifndef INCLUDED_ASAP_NORMALIZE_H
#define INCLUDED_ASAP_NORMALIZE_H

#include <vector>
#include <limits>

#include "asap/data_set.h"

namespace asap {

template<typename VectorTy>
typename std::enable_if<is_dense_vector<VectorTy>::value>::type
extrema( const VectorTy & vec,
	 std::vector<std::pair<typename VectorTy::value_type,
	 typename VectorTy::value_type>> & mmv ) {
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::index_type index_type;
    std::pair<value_type, value_type> * mm = mmv.data();
    for( index_type i=0, e=vec.length(); i < e; ++i ) {
	if( mm[i].first > vec[i] )
	    mm[i].first = vec[i];
	if( mm[i].second < vec[i] )
	    mm[i].second = vec[i];
    }
}

template<typename VectorTy>
typename std::enable_if<is_sparse_vector<VectorTy>::value>::type
extrema( const VectorTy & vec,
	 std::vector<std::pair<typename VectorTy::value_type,
	 typename VectorTy::value_type>> & mmv ) {
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::index_type index_type;
    std::pair<value_type, value_type> * mm = mmv.data();
    for( index_type i=0, e=vec.nonzeros(); i < e; ++i ) {
	value_type v;
	index_type c;
	vec.get( i, v, c );
	if( mm[c].first > v )
	    mm[c].first = v;
	if( mm[c].second < v )
	    mm[c].second = v;
    }
}

template<typename DataSet>
std::vector<std::pair<typename DataSet::value_type,
		      typename DataSet::value_type>>
    extrema( const DataSet & data ) {
    typedef typename DataSet::value_type value_type;
    typedef typename DataSet::index_type index_type;
    typedef typename DataSet::vector_type vector_type;
    std::vector<std::pair<value_type, value_type>> mm;
    size_t d = data.get_dimensions();
    mm.resize( d );

    // TODO: tune grainsize
    cilk_for( index_type i=0; i < d; ++i ) {
	// TODO: consider vectorization
	mm[i] = std::make_pair(
	    std::numeric_limits<value_type>::max(),
	    -std::numeric_limits<value_type>::max() );
    }

    // Calculate minimum and maximum value
    // TODO: Consider parallelization using reducer on vector
    //       Every new instance of the reduction variable will need to
    //       be initialized with the loop above (min and max set to worst case).
    //       For dense vectors we could use a different strategy and
    //       parallelize by dimensions rather than by vectors.
    for( typename DataSet::const_vector_iterator
	     I=data.vector_cbegin(), E=data.vector_cend(); I != E; ++I ) {
	extrema( *I, mm );
    }

    // Correct for sparse vectors: if minimum still at initialized value,
    // then dimension was always zero in the data set, i.e., it did not appear.
    if( is_sparse_vector<vector_type>::value ) {
	cilk_for( size_t i=0; i < d; ++i ) {
	    // TODO: consider vectorization
	    if( mm[i].first == std::numeric_limits<value_type>::max() )
		mm[i].first = mm[i].second = value_type(0);
	}
    }

    return mm;
}

namespace internal {
template<typename VectorTy>
struct Scale {
    typedef VectorTy vector_type;
    typedef typename vector_type::value_type value_type;
    typedef typename vector_type::index_type index_type;

private:
    const std::vector<std::pair<value_type, value_type>> & mm;
    
public:
    Scale( const std::vector<std::pair<value_type, value_type>> & mm_ )
	: mm( mm_ ) { }

    void operator() ( index_type i, value_type & v ) {
	if( mm[i].first != mm[i].second )
	    v = (v - mm[i].first) / (mm[i].second - mm[i].first+1);
	else
	    v = value_type(1);
    }
};

template<typename VectorTy>
struct Unscale {
    typedef VectorTy vector_type;
    typedef typename vector_type::value_type value_type;
    typedef typename vector_type::index_type index_type;

private:
    const std::vector<std::pair<value_type, value_type>> & mm;
    
public:
    Unscale( const std::vector<std::pair<value_type, value_type>> & mm_ )
	: mm( mm_ ) { }

    void operator() ( index_type i, value_type & v ) {
	if( mm[i].first != mm[i].second )
	    v = v * (mm[i].second - mm[i].first+1) + mm[i].first;
	else if( v != value_type(0) )
	    v = mm[i].first;
    }
};

} // namespace internal

template<typename DataSet>
std::vector<std::pair<typename DataSet::value_type,
		      typename DataSet::value_type>>
    normalize( DataSet & data ) {
    typedef typename DataSet::value_type value_type;
    typedef typename DataSet::index_type index_type;
    typedef typename DataSet::vector_type vector_type;

    // Calculate the extreme values per dimension
    std::vector<std::pair<value_type, value_type>> mm = extrema( data );

    // Scale data
    internal::Scale<vector_type> scale( mm );
    typename DataSet::vector_iterator E=data.vector_end();
    cilk_for( typename DataSet::vector_iterator
	      I=data.vector_begin(); I != E; ++I ) {
	I->map( scale );
    }

    return mm;
}

template<typename DataSet>
void denormalize( const std::vector<std::pair<typename DataSet::value_type,
		  typename DataSet::value_type>> & extrema,
		  DataSet & data ) {
    typedef typename DataSet::value_type value_type;
    typedef typename DataSet::index_type index_type;
    typedef typename DataSet::vector_type vector_type;

    // Unscale data
    internal::Unscale<vector_type> unscale( extrema );
    typename DataSet::vector_iterator E=data.vector_end();
    cilk_for( typename DataSet::vector_iterator
	     I=data.vector_begin(); I != E; ++I ) {
	I->map( unscale );
    }
}

}

#endif // INCLUDED_ASAP_NORMALIZE_H
