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


#ifndef INCLUDED_ASAP_VECTOR_OPS_H
#define INCLUDED_ASAP_VECTOR_OPS_H

namespace asap {

// Dense vector operations
template<typename IndexTy, typename ValueTy, bool IsVectorized = false>
struct dense_vector_operations {
    typedef IndexTy index_type;
    typedef ValueTy value_type;
    static const bool is_vectorized = false;

    static void
    set( value_type *src, index_type length, value_type val ) {
	std::fill( src, src+length, val );
    }
    static void
    copy( value_type const *src_begin, value_type const *src_end, value_type *dst ) {
	std::copy( src_begin, src_end, dst );
    }
    static void
    copy( value_type const *src, index_type length, value_type *dst ) {
	std::copy( src, src+length, dst );
    }
    static void
    scale( value_type *src, index_type length, value_type alpha ) {
	for( value_type *I=src, *E=src+length; I != E; ++I )
	    *I *= alpha;
    }
    static void
    add( value_type *a, index_type length, value_type const *IB ) {
	for( value_type *IA=a, *EA=a+length; IA != EA; ++IA, ++IB )
	    *IA += *IB;
    }

    static value_type
    square_euclidean_distance(
	value_type const *a, index_type length, value_type const *IB ) {
	value_type sq_dist = 0;
	for( value_type const *IA=a, *EA=a+length; IA != EA; ++IA, ++IB ) {
	    value_type diff = *IA - *IB;
	    sq_dist += diff * diff;
	}
	return sq_dist;
    }
};

#ifdef __INTEL_COMPILER
// TODO: or GCC with Cilkplus support
// Dense vector operations with support for vectorization
template<typename IndexTy, typename ValueTy>
struct dense_vector_operations<IndexTy,ValueTy,true> {
    typedef IndexTy index_type;
    typedef ValueTy value_type;

    static void
    set( value_type *src, index_type length, value_type val ) {
	src[0:length] = (value_type)val;
    }
    static void
    copy( value_type const *src_begin, value_type const *src_end, value_type *dst ) {
	index_type length = src_end - src_begin;
	dst[0:length] = src_begin[0:length];
    }
    static void
    copy( value_type const *src, index_type length, value_type *dst ) {
	dst[0:length] = src[0:length];
    }
    static void
    scale( value_type *a, index_type length, value_type alpha ) {
	a[0:length] *= alpha;
    }
    static void
    add( value_type *a, index_type length, value_type const *b ) {
	a[0:length] += b[0:length];
    }

    static value_type
    square_euclidean_distance(
	value_type const *a, index_type length, value_type const *b ) {
	return __sec_reduce_add( esqd( a[0:length], b[0:length] ) );
    }

private:
    static value_type esqd( value_type a, value_type b ) {
	value_type diff = a - b;
	return diff * diff;
    }
};
#endif

// Sparse vector operations
template<typename IndexTy, typename ValueTy, bool IsVectorized = false>
struct sparse_vector_operations {
    typedef IndexTy index_type;
    typedef ValueTy value_type;
    static const bool is_vectorized = false;

    static void
    set( value_type *v, index_type *c, index_type length,
	 value_type val, index_type idx ) {
	std::fill( v, v+length, val );
	std::fill( c, c+length, idx );
    }
    static void
    copy( value_type const *src_v, index_type const *src_c, index_type length,
	  value_type *dst_v, index_type *dst_c ) {
	std::copy( src_v, src_v+length, dst_v );
	std::copy( src_c, src_c+length, dst_c );
    }
    static void
    scale( value_type *src_v, index_type length, value_type alpha ) {
	for( value_type *I=src_v, *E=src_v+length; I != E; ++I )
	    *I *= alpha;
    }
};

#ifdef __INTEL_COMPILER
// TODO: or GCC with Cilkplus support
// Sparse vector operations with support for vectorization
template<typename IndexTy, typename ValueTy>
struct sparse_vector_operations<IndexTy,ValueTy,true> {
    typedef IndexTy index_type;
    typedef ValueTy value_type;

    static void
    set( value_type *v, index_type *c, index_type length,
	 value_type val, index_type idx ) {
	src_v[0:length] = (value_type)val;
	src_c[0:length] = (value_type)idx;
    }
    static void
    copy( value_type const *src_v, index_type const *src_c, index_type length,
	  value_type *dst_v, index_type *dst_c ) {
	dst_v[0:length] = src_v[0:length];
	dst_c[0:length] = src_c[0:length];
    }
    static void
    scale( value_type *a, index_type length, value_type alpha ) {
	a[0:length] *= alpha;
    }
};
#endif

// Note: there are currently no overloads for the vectorized variants of
//       the operations on a sparse and a dense vector.
template<typename IndexTy, typename ValueTy, bool IsVectorized = false>
struct sparse_dense_vector_operations {
    typedef IndexTy index_type;
    typedef ValueTy value_type;
    static const bool is_vectorized = false;
    typedef dense_vector_operations<index_type, value_type, is_vectorized> dense_ops;

    static void
    copy( value_type const *src_v, index_type const *src_c,
	  index_type src_length, value_type *dst, index_type dst_length ) {
	dense_ops::set( dst, dst_length, value_type(0) );
	for( index_type i=0; i < src_length; ++i )
	    dst[src_c[i]] = src_v[i];
    }

    static void
    add( value_type *dst_v, index_type dst_length,
	 value_type const *src_v, index_type const *src_c,
	 index_type src_length ) {
	for( index_type i=0; i < src_length; ++i )
	    dst_v[src_c[i]] += src_v[i];
    }

    static value_type
    square_euclidean_distance(
	value_type const *a_v, index_type const *a_c, index_type a_length,
	value_type const *d, index_type d_length ) {
	// This code assumes that a_c is sorted in increasing size
	value_type sum = 0;
	for( index_type i=0, j=0; i < d_length; ++i ) {
	    value_type diff;
	    if( j < a_length && i == a_c[j] )
		diff = d[i] - a_v[j++];
	    else
		diff = d[i];
	    sum += diff * diff;
	}
	return sum;
    }

    static value_type
    square_euclidean_distance(
	value_type const *a_v, index_type const *a_c, index_type a_length,
	value_type const *d, index_type d_length, value_type d_sqnorm ) {
	value_type sum = 0;
	for( index_type j=0; j < a_length; ++j )
	    sum += a_v[j] * ( a_v[j] - value_type(2) * d[a_c[j]] );
	return sum + d_sqnorm;
    }

#if 0
    // Attempt to vectorize. Not noticably faster than non-vectorized code
    static value_type
    square_euclidean_distance(
	value_type const *a_v, index_type const *a_c, index_type a_length,
	value_type const *d, index_type d_length, value_type d_sqnorm ) {
	value_type dd[a_length];
	for( index_type j=0; j < a_length; ++j )
	    dd[j] = d[a_c[j]];
	value_type sum
	    = __sec_reduce_add( sqadd( a_v[0:a_length], dd[0:a_length] ) );
	return sum + d_sqnorm;
    }

private:
    static value_type sqadd( value_type a, value_type b ) {
	return a * ( a - value_type(2) * b );
    }
#endif
};

}

#endif // INCLUDED_ASAP_VECTOR_OPS_H
