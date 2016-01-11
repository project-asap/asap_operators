/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_SPARSE_VECTOR_H
#define INCLUDED_ASAP_SPARSE_VECTOR_H

#include <iostream>
#include <memory>
#include <type_traits>
#include <limits>

#include "asap/traits.h"
#include "asap/vector_ops.h"

namespace asap {

template<typename VectorTy>
class sparse_vector_set;

template<typename IndexTy, typename ValueTy, bool IsVectorized,
	 typename MemoryMgmt = mm_ownership_policy,
	 typename Allocator = std::allocator<ValueTy>>
class sparse_vector
{
public:
    static const bool is_vectorized = IsVectorized;
    typedef IndexTy index_type;
    typedef ValueTy value_type;
    typedef MemoryMgmt memory_mgmt_type;
    typedef Allocator allocator_type;
    typedef typename Allocator::template rebind<value_type>::other value_allocator_type;
    typedef typename Allocator::template rebind<index_type>::other index_allocator_type;
    typedef sparse_vector_operations<index_type, value_type, is_vectorized> vector_ops;
    typedef sparse_dense_vector_operations<index_type, value_type, is_vectorized> mix_vector_ops;

    struct _asap_tag : tag_sparse, tag_vector { };
    void asap_decl(void);

    friend class sparse_vector_set<sparse_vector>;

private:
    value_type *m_value;
    index_type *m_coord;
    index_type m_length;
    index_type m_nonzeros;

public:
    // length cannot be changed...
    sparse_vector() : m_value(nullptr), m_coord(nullptr), m_length(0), m_nonzeros(0) { }
private:
    sparse_vector(value_type *value_, index_type *coord_, index_type length_,
		  index_type nonzeros_, bool copy) : m_length(length_),
						     m_nonzeros(nonzeros_) {
	if( copy ) {
	    m_value = value_allocator_type().allocate( m_nonzeros );
	    m_coord = index_allocator_type().allocate( m_nonzeros );
	    vector_ops::copy( value_, coord_, nonzeros_, m_value, m_coord );
	} else {
	    m_value = value_;
	    m_coord = coord_;
	}
    }
public:
    template<typename MMT = memory_mgmt_type,
	     typename = typename std::enable_if<
		 std::is_same<MMT, memory_mgmt_type>::value &&
		 MMT::has_ownership>::type>
    sparse_vector(index_type length_, index_type nonzeros_)
	: m_length(length_), m_nonzeros(nonzeros_) {
	if( memory_mgmt_type::allocate ) {
	    m_value = value_allocator_type().allocate( m_nonzeros );
	    m_coord = index_allocator_type().allocate( m_nonzeros );
	} else {
	    m_value = nullptr;
	    m_coord = nullptr;
	}
    }
    sparse_vector(value_type *value_, index_type *coord_,
		  index_type length_, index_type nonzeros_)
	: sparse_vector( value_, coord_, length_, nonzeros_,
			 memory_mgmt_type::assign ) { }
    sparse_vector(const sparse_vector &pt)
	: sparse_vector( pt.m_value, pt.m_coord, pt.m_length, pt.m_nonzeros,
			 memory_mgmt_type::assign ) { }
    sparse_vector(sparse_vector &&pt)
	: sparse_vector( pt.m_value, pt.m_coord, pt.m_length, pt.m_nonzeros,
			 memory_mgmt_type::assign_rvalue ) {
	if( !memory_mgmt_type::assign_rvalue ) {
	    pt.m_value = nullptr;
	    pt.m_coord = nullptr;
	}
    }
    ~sparse_vector() {
	if( m_value && memory_mgmt_type::deallocate )
	    value_allocator_type().deallocate( m_value, m_nonzeros );
	if( m_coord && memory_mgmt_type::deallocate )
	    index_allocator_type().deallocate( m_coord, m_nonzeros );
    }

    void swap( sparse_vector &sv ) {
	std::swap( m_value, sv.m_value );
	std::swap( m_coord, sv.m_coord );
	std::swap( m_length, sv.m_length );
	std::swap( m_nonzeros, sv.m_nonzeros );
    }

    index_type length() const { return m_length; }
    index_type nonzeros() const { return m_nonzeros; }
    const value_type * get_value() const { return m_value; }
    const index_type * get_coord() const { return m_coord; }

    void set( index_type pos, value_type v, index_type c ) {
	assert( 0 <= pos && pos < m_nonzeros );
	m_value[pos] = v;
	m_coord[pos] = c;
    }
    void get( index_type pos, value_type &v, index_type &c ) const {
	v = m_value[pos];
	c = m_coord[pos];
    }

    template<typename Fn>
    void map( Fn & fn ) {
	for( index_type i=0; i < m_nonzeros; ++i )
	    fn( m_coord[i], m_value[i] );
    }
    


#if 0
    value_type & operator[]( index_type idx ) {
	return m_value[idx]; // unchecked
    }
    // Assuming trivial types, copy is better than by reference
    value_type operator[]( index_type idx ) const {
	return m_value[idx]; // unchecked
    }
#endif
    
    void scale(value_type alpha) {
	vector_ops::scale( m_value, m_coord, m_nonzeros, alpha );
    }

    void clear() {
	// Vectorizable
	std::fill( &m_value[0], &m_value[m_nonzeros], value_type(0) );
    }
    void clear_attributes() { }
    
    // Square of Euclidean distance
    template<typename VectorTy>
    typename std::enable_if<is_dense_vector<VectorTy>::value && !is_vector_with_sqnorm_cache<VectorTy>::value, value_type>::type
    sq_dist( VectorTy const &p ) const {
	return mix_vector_ops::square_euclidean_distance(
	    m_value, m_coord, m_nonzeros, p.get_value(), p.length() );
    }
    // Square of Euclidean distance, optimized with precalculated sqnorm
    template<typename VectorTy>
    typename std::enable_if<is_dense_vector<VectorTy>::value && is_vector_with_sqnorm_cache<VectorTy>::value, value_type>::type
    sq_dist( VectorTy const &p ) const {
	return mix_vector_ops::square_euclidean_distance(
	    m_value, m_coord, m_nonzeros, p.get_value(), p.length(), p.get_sqnorm() );
    }
};

// A sparse vector set with memory allocation optimized such that memory
// is allocated only once for all vectors. This requires the use of
// sparse vectors without ownership of the vector data.
template<typename VectorTy>
class sparse_vector_set
{
public:
    typedef typename VectorTy::index_type index_type;
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::memory_mgmt_type memory_mgmt_type;
    typedef typename VectorTy::allocator_type allocator_type;
    typedef typename VectorTy::index_allocator_type index_allocator_type;
    typedef typename VectorTy::value_allocator_type value_allocator_type;
    typedef VectorTy vector_type;

    typedef const vector_type	* const_iterator;
    typedef vector_type		* iterator;

protected:
    vector_type *m_vectors;
    value_type  *m_alloc_v;
    index_type  *m_alloc_i;
    size_t m_number;
    size_t m_capacity;
    size_t m_total_length;

public:
    // Constructor intended only for use by reducers
    sparse_vector_set() : m_vectors(nullptr), m_alloc_v(nullptr),
			  m_alloc_i(nullptr), m_number(0), m_capacity(0),
			  m_total_length(0) {
	static_assert( is_sparse_vector<VectorTy>::value,
		       "vector_type must be sparse" );
	static_assert( !memory_mgmt_type::has_ownership,
		       "vector_type must not have data ownership" );
    }

    // Proper constructor
    sparse_vector_set(size_t capacity, size_t total_length)
	: m_number(0), m_capacity(capacity) {
	m_total_length = total_length; // TODO: round up to vector length
	m_alloc_v = value_allocator_type().allocate( m_total_length );
	m_alloc_i = index_allocator_type().allocate( m_total_length );
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	m_vectors = dv_alloc.allocate( m_capacity );
/* It is impossible to construct the vectors...
	value_type *pv = m_alloc_v;
	index_type *pi = m_alloc_i;
	for( size_t i=0; i < m_capacity; ++i ) {
	    dv_alloc.construct( &m_vectors[i], pv, pi, length );
	    pv += m_total_length;
	    pi += m_total_length;
	}
*/
    }
    sparse_vector_set(const sparse_vector_set & dvs)
	: sparse_vector_set(dvs.m_capacity, dvs.m_total_length) {
	std::cerr << "SVS copy construct\n";
	assert( m_total_length == dvs.m_total_length );
	m_number = dvs.m_number;
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	value_type *pv = m_alloc_v;
	index_type *pi = m_alloc_i;
	for( size_t i=0; i < m_number; ++i ) {
	    size_t length = dvs.m_vectors[i].length();
	    size_t nonzeros = dvs.m_vectors[i].nonzeros();
	    dv_alloc.construct( &m_vectors[i], pv, pi, length, nonzeros );
	    pv += nonzeros;
	    pi += nonzeros;
	}
	std::copy( &dvs.m_alloc_v[0], &dvs.m_alloc_v[m_total_length],
		   &m_alloc_v[0] );
	std::copy( &dvs.m_alloc_i[0], &dvs.m_alloc_i[m_total_length],
		   &m_alloc_i[0] );
    }
    sparse_vector_set(sparse_vector_set && dvs)
	: m_vectors(dvs.m_vectors), m_alloc(dvs.m_alloc),
	  m_number(dvs.m_number), m_capacity(dvs.m_capacity),
	  m_total_length(dvs.m_total_length) {
	std::cerr << "SVS move construct\n";
	dvs.m_vectors = 0;
	dvs.m_alloc = 0;
	dvs.m_number = 0;
	dvs.m_capacity = 0;
	dvs.m_total_length = 0;
    }
    ~sparse_vector_set() {
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	// What if we haven't constructed the vectors yet?
	for( size_t i=0; i < m_number; ++i )
	    dv_alloc.destroy( &m_vectors[i] );
	dv_alloc.deallocate( m_vectors, m_capacity );
	value_allocator_type().deallocate( m_alloc_v, m_total_length );
	index_allocator_type().deallocate( m_alloc_i, m_total_length );
    }

    bool check_init( size_t capacity, size_t total_length ) {
	if( m_vectors == nullptr ) {
	    new (this) sparse_vector_set( capacity, total_length ); // initialize
	    return true;
	} else
	    return false;
    }

    void swap( sparse_vector_set & dvs ) {
	std::swap( m_vectors, dvs.m_vectors );
	std::swap( m_alloc_v, dvs.m_alloc_v );
	std::swap( m_alloc_i, dvs.m_alloc_i );
	std::swap( m_number, dvs.m_number );
	std::swap( m_capacity, dvs.m_capacity );
	std::swap( m_total_length, dvs.m_total_length );
    }

    size_t number() const { return m_number; }
    size_t size() const { return m_number; }
    void   trim_number( size_t n ) { if( n < m_number ) m_number = n; }

    void fill( value_type val ) {
	std::fill( &m_alloc_v[0], &m_alloc_v[m_total_length], val );
    }
    void clear() {
	// TODO: vectorize
	std::fill( &m_alloc_v[0], &m_alloc_v[m_total_length],
		   value_type(0) );
	for( size_t i=0; i < m_number; ++i )
	    m_vectors[i].clear_attributes();
    }

    void emplace_back( size_t length, size_t nonzeros ) {
	assert( m_number < m_capacity );
	value_type * pv;
	index_type * pi;
	if( m_number == 0 ) {
	    pv = m_alloc_v;
	    pi = m_alloc_i;
	} else {
	    size_t prev_len = m_vectors[m_number-1].nonzeros();
	    pv = m_vectors[m_number-1].m_value + prev_len;
	    pi = m_vectors[m_number-1].m_coord + prev_len;
	    assert( pv - m_alloc_v <= m_total_length );
	    assert( pi - m_alloc_i <= m_total_length );
	}
	typename allocator_type::template rebind<vector_type>::
	    other dv_alloc;
	dv_alloc.construct( &m_vectors[m_number++], pv, pi, length, nonzeros );
    }

    const vector_type & operator[] ( size_t idx ) const {
	assert( idx < m_number );
	return m_vectors[idx];
    }
    vector_type & operator[] ( size_t idx ) {
	assert( idx < m_number );
	return m_vectors[idx];
    }

    value_type * get_alloc_v() { return m_alloc_v; }
    index_type * get_alloc_i() { return m_alloc_i; }

    // TODO: work out iterators
    iterator begin() { return &m_vectors[0]; }
    iterator end() { return &m_vectors[m_number]; }
    const_iterator cbegin() const { return &m_vectors[0]; }
    const_iterator cend() const { return &m_vectors[m_number]; }

#if 0
    sparse_vector_set & operator += ( const sparse_vector_set & dvs ) {
	assert( m_number == dvs.m_number );
	assert( m_total_length == dvs.m_total_length );
	for( size_t i=0; i < m_number; ++i )
	    m_vectors[i] += dvs.m_vectors[i];
	return *this;
    }
#endif
};

}

#endif // INCLUDED_ASAP_SPARSE_VECTOR_H
