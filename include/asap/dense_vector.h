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

#ifndef INCLUDED_ASAP_DENSE_VECTOR_H
#define INCLUDED_ASAP_DENSE_VECTOR_H

#include <iostream>
#include <memory>
#include <type_traits>
#include <limits>
#include <cilk/cilk.h>
#include <cilk/reducer.h>

#include "asap/traits.h"
#include "asap/vector_ops.h"
#include "asap/memory.h"

namespace asap {

/** @brief A dense vector
 *
 * @tparam IndexTy The type used to index the vector, typically an integer type
 * @tparam ValueTy The type of the elements stored in the vector
 * @tparam IsVectorized Whether to use vector (SIMD) operations
 * @tparam MemoryMgmt Whether vectors own the associated heap memory
 * @tparam Allocator The memory allocator used by this class
 */
template<typename IndexTy, typename ValueTy, bool IsVectorized,
	 typename MemoryMgmt = mm_ownership_policy,
	 typename Allocator = std::allocator<ValueTy>>
class dense_vector
{
public:
    /** Whether to use vector (SIMD) operations */
    static const bool is_vectorized = IsVectorized;
    /** The type used to index the vector, typically an integer type */
    typedef IndexTy index_type;
    /** The type of the elements stored in the vector */
    typedef ValueTy value_type;
    /** Whether vectors own the associated heap memory */
    typedef MemoryMgmt memory_mgmt_type;
    /** The memory allocator used by this class */
    typedef Allocator allocator_type;
    /** The class holding the vector operations for this class */
    typedef dense_vector_operations<index_type, value_type, is_vectorized> vector_ops;

    /** A tag class describing the characteristics of this class */
    struct _asap_tag : tag_dense, tag_vector { };
    /** A dummy method definition to implement type inspection */
    void asap_decl(void);

private:
    /** Pointer to a dense array of values */
    value_type *m_value;
    /** The length of the vector */
    index_type m_length;

private:
    /** The default constructor is disabled.
     * @details Length cannot be changed for a vector
     */
    dense_vector() = delete; // : m_value(nullptr), m_length(0) { }
public:
    /** Constructor for an empty vector with specified length.
     *
     * @details
     * Disallow this constructor in case vector does not do its own
     * memory management.
     *
     * @tparam MMT Whether the vector owns the heap memory
     * @param length_ The length of the vector
     */
    template<typename MMT = memory_mgmt_type,
	     typename = typename std::enable_if<
		 std::is_same<MMT, memory_mgmt_type>::value &&
		 MMT::has_ownership>::type>
    dense_vector(index_type length_) : m_length(length_) {
	if( memory_mgmt_type::allocate )
	    m_value = allocator_type().allocate( m_length );
	else
	    m_value = nullptr;
    }
    /** Assignment constructor
     *
     * @param value_ An array of values
     * @param length_ The length of the vector
     */
    dense_vector(value_type *value_, index_type length_) : m_length(length_) {
	if( memory_mgmt_type::assign ) {
	    m_value = allocator_type().allocate( m_length );
	    vector_ops::copy( value_, length_, m_value );
	} else
	    m_value = value_;
    }
    /** Move constructor
     *
     * @param pt The dense vector to move
     */
    dense_vector(dense_vector &&pt) : m_length( pt.m_length ) {
	if( memory_mgmt_type::assign_rvalue ) {
	    m_value = allocator_type().allocate( m_length );
	    vector_ops::copy( pt.m_value, pt.m_length, m_value );
	} else {
	    m_value = pt.m_value;
	    pt.m_value = nullptr; // necessary?
	}
    }
    /** Copy constructor
     *
     * @param pt The dense vector to copy
     */
    dense_vector(const dense_vector &pt) : m_length( pt.m_length ) {
	if( memory_mgmt_type::assign ) {
	    m_value = allocator_type().allocate( m_length );
	    vector_ops::copy( pt.m_value, pt.m_length, m_value );
	} else
	    m_value = pt.m_value;
    }
    /** Destructor */
    ~dense_vector() {
	if( m_value && memory_mgmt_type::deallocate )
	    allocator_type().deallocate( m_value, m_length );
    }

    /** Copy-assignment operator */
    dense_vector & operator = ( const dense_vector & dv ) {
	// Cleanup
	if( m_value && memory_mgmt_type::deallocate )
	    allocator_type().deallocate( m_value, m_length );

	// New value
	m_length = dv.m_length;
	if( memory_mgmt_type::assign ) {
	    m_value = allocator_type().allocate( m_length );
	    vector_ops::copy( dv.m_value, dv.m_length, m_value );
	} else
	    m_value = dv.m_value;
	return *this;
    }

    /** Move-assignment operator */
    dense_vector & operator = ( dense_vector && dv ) {
	if( m_value && memory_mgmt_type::deallocate )
	    allocator_type().deallocate( m_value, m_length );
	m_value = std::move(dv.m_value);
	dv.m_value = 0;
	m_length = std::move(dv.m_length);
	dv.m_length = 0;
	return *this;
    }

    /** Return the length of the vector */
    index_type length() const { return m_length; }
    /** Return the dense array of values */
    const value_type * get_value() const { return m_value; }

    /** Return a reference to specific element */
    value_type & operator[]( index_type idx ) {
	return m_value[idx]; // unchecked
    }
    /** Return a specific element
     * @details Assuming trivial types, copy is better than by reference
     */
    value_type operator[]( index_type idx ) const {
	return m_value[idx]; // unchecked
    }

    /** Apply a functor to every element of the vector
     * @param fn Functor to apply. Takes two arguments: the index and the value
     */
    template<typename Fn>
    void map( Fn & fn ) {
	for( index_type i=0; i < m_length; ++i )
	    fn( i, m_value[i] );
    }
    
    /** Normalize, i.e., scale by length of vector */
    void normalize(value_type n) {
	vector_ops::scale( m_value, m_length, value_type(1)/n );
    }
    /** Scale by the specific value */
    void scale(value_type alpha) {
	vector_ops::scale( m_value, m_length, alpha );
    }

    /** Set all elements to zero */
    void clear() {
	vector_ops::set( m_value, m_length, value_type(0) );
	clear_attributes();
    }
    /** Clear the attributes of the vector (generic function) */
    void clear_attributes() {
    }
    
    /** Return the square of Euclidean distance to another dense vector
     *  with elements of the same type
     */
    template<typename OtherVectorTy>
    typename std::enable_if<is_dense_vector<OtherVectorTy>::value, value_type>::type
    sq_dist(OtherVectorTy const& p) const {
	return vector_ops::square_euclidean_distance( m_value, m_length, p.get_value() );
    }
    
    /** Return the square of Euclidean distance of the vector to itself */
    value_type sq_norm() const {
	return vector_ops::square_norm( m_value, m_length );
    }
    
    /** Element-wise vector addition with vector of same type
     * Used in reduction of centre computations */
    template<typename OtherVectorTy>
    const typename std::enable_if<is_dense_vector<OtherVectorTy>::value, dense_vector>::type &
    operator += ( const OtherVectorTy & pt ) {
	vector_ops::add( m_value, m_length, pt.get_value() );
	return *this;
    }

    /** Element-wise vector addition with sparse vector of same-type elements */
    template<typename OtherVectorTy>
    const typename std::enable_if<is_sparse_vector<OtherVectorTy>::value, dense_vector>::type &
    operator += ( const OtherVectorTy & pt ) {
        for(int j = 0; j < pt.nonzeros(); j++) {
	    value_type v;
	    index_type c;
	    pt.get( j, v, c );
	    m_value[c] += v;
	}
	return *this;
    }

    /** Copy vector attributes, if any */
    template<typename OtherVectorTy>
    typename std::enable_if<is_dense_vector<OtherVectorTy>::value>::type
    copy_attributes( const OtherVectorTy & pt ) { }
};


/** @brief A set of dense vectors.
 * 
 * @detail
 * A dense vector set with memory allocation optimized such that memory
 * is allocated only once for all vectors. This requires the use of
 * dense vectors without ownership of the vector data.
 *
 * @tparam VectorTy The type of dense vectors stored
 */
template<typename VectorTy>
class dense_vector_set
{
public:
    typedef typename VectorTy::index_type index_type;
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::memory_mgmt_type memory_mgmt_type;
    typedef typename VectorTy::allocator_type allocator_type;
    typedef VectorTy vector_type;

    typedef const vector_type	* const_iterator;
    typedef vector_type		* iterator;

protected:
    vector_type *m_vectors;
    value_type  *m_alloc;
    size_t m_number;
    size_t m_length;

public:
    // Constructor intended only for use by reducers
    dense_vector_set() : m_vectors(nullptr), m_alloc(nullptr),
			 m_number(0), m_length(0) {
	static_assert( is_dense_vector<VectorTy>::value,
		       "vector_type must be dense" );
	static_assert( !memory_mgmt_type::has_ownership,
		       "vector_type must not have data ownership" );
    }

    // Proper constructor
    dense_vector_set(size_t number, size_t length)
	: m_number(number), m_length( length ) {
	size_t aligned_length = length; // TODO: round up to SIMD vector length
	                                // and guarantee alignment
	m_alloc = allocator_type().allocate( m_number*aligned_length );
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	m_vectors = dv_alloc.allocate( m_number );
	value_type *p = m_alloc;
	for( size_t i=0; i < m_number; ++i ) {
	    dv_alloc.construct( &m_vectors[i], p, length );
	    p += aligned_length;
	}
    }
    dense_vector_set(const dense_vector_set & dvs)
	: dense_vector_set(dvs.m_number,
			   dvs.m_vectors ? dvs.m_vectors[0].length() : 0) {
	std::cerr << "DVS copy construct\n";
	assert( m_length == dvs.m_length );
	for( size_t i=0; i < m_number; ++i )
	    m_vectors[i].copy_attributes( dvs.m_vectors[i] );
	size_t aligned_length = m_length; // TODO
	std::copy( &dvs.m_alloc[0], &dvs.m_alloc[m_number*aligned_length],
		   &m_alloc[0] );
    }
    dense_vector_set(dense_vector_set && dvs)
	: m_vectors(dvs.m_vectors), m_alloc(dvs.m_alloc),
	  m_number(dvs.m_number), m_length(dvs.m_length) {
	std::cerr << "DVS move construct\n";
	dvs.m_vectors = 0;
	dvs.m_alloc = 0;
	dvs.m_number = 0;
	dvs.m_length = 0;
    }
    ~dense_vector_set() {
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	for( size_t i=0; i < m_number; ++i )
	    dv_alloc.destroy( &m_vectors[i] );
	dv_alloc.deallocate( m_vectors, m_number );
	size_t aligned_length = m_length; // TODO
	allocator_type().deallocate( m_alloc, m_number*aligned_length );
    }

    bool check_init( size_t number, size_t length ) {
	if( m_vectors == nullptr ) {
	    new (this) dense_vector_set( number, length ); // initialize
	    return true;
	} else
	    return false;
    }

    void swap( dense_vector_set & dvs ) {
	std::swap( m_vectors, dvs.m_vectors );
	std::swap( m_alloc, dvs.m_alloc );
	std::swap( m_number, dvs.m_number );
	std::swap( m_length, dvs.m_length );
    }

    size_t number() const { return m_number; }
    size_t size() const { return m_number; }
    size_t length() const { return m_length; }
    void   trim_number( size_t n ) { if( n < m_number ) m_number = n; }

    void fill( value_type val ) {
	size_t aligned_length = m_length; // TODO
	std::fill( &m_alloc[0], &m_alloc[m_number*aligned_length], val );
    }
    void clear() {
	size_t aligned_length = m_length; // TODO
	// TODO: vectorize
	std::fill( &m_alloc[0], &m_alloc[m_number*aligned_length],
		   value_type(0) );
	for( size_t i=0; i < m_number; ++i )
	    m_vectors[i].clear_attributes();
    }

    const vector_type & operator[] ( size_t idx ) const {
	return m_vectors[idx];
    }
    vector_type & operator[] ( size_t idx ) {
	return m_vectors[idx];
    }

    iterator begin() { return &m_vectors[0]; }
    iterator end() { return &m_vectors[m_number]; }
    const_iterator cbegin() const { return &m_vectors[0]; }
    const_iterator cend() const { return &m_vectors[m_number]; }

    dense_vector_set & operator += ( const dense_vector_set & dvs ) {
	assert( m_number == dvs.m_number );
	assert( m_length == dvs.m_length );
	for( size_t i=0; i < m_number; ++i )
	    m_vectors[i] += dvs.m_vectors[i];
	return *this;
    }
};

template<typename VectorTy>
typename std::enable_if<asap::is_dense_vector<VectorTy>::value,
			std::ostream &>::type
& operator << ( std::ostream & os, const VectorTy & dv ) {
    os << '{';
    for( int i=0, e=dv.length(); i != e; ++i ) {
	os << dv[i];
	if( i+1 < e )
	    os << ", ";
    }
    os << '}';
    return os;
}

template<typename VectorTy>
typename std::enable_if<asap::is_sparse_vector<VectorTy>::value,
			std::ostream &>::type
operator << ( std::ostream & os, const VectorTy & sv ) {
    os << '{';
    for( int i=0, e=sv.nonzeros(); i != e; ++i ) {
	typename VectorTy::value_type v;
	typename VectorTy::index_type c;
	sv.get( i, v, c );
	os << c << " " << v;
	if( i+1 < e )
	    os << ", ";
    }
    os << '}';
    return os;
}

}

#endif // INCLUDED_ASAP_DENSE_VECTOR_H
