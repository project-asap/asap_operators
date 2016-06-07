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


#ifndef INCLUDED_ASAP_ATTRIBUTES_H
#define INCLUDED_ASAP_ATTRIBUTES_H

#include "asap/traits.h"

namespace asap {

// Extend a vector (dense or sparse) to hold the value of the square of
// its norm. Note: value must be calculate explicitly by user, it is not
// automatically updated as the vector is changed.
template<typename VectorTy,
	 typename = typename std::enable_if<is_vector<VectorTy>::value>::type>
class vector_with_sqnorm_cache : public VectorTy
{
public:
    static const bool is_vectorized = VectorTy::is_vectorized;
    typedef typename VectorTy::index_type index_type;
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::memory_mgmt_type memory_mgmt_type;
    typedef typename VectorTy::allocator_type allocator_type;
    typedef VectorTy vector_type;

    struct _asap_tag : public tag_sqnorm_cache, vector_type::_asap_tag { };
    void asap_decl(void);

private:
    value_type m_sqnorm;

public:
    template<typename MMT = memory_mgmt_type,
	     typename = typename std::enable_if<
		 std::is_same<MMT, memory_mgmt_type>::value &&
		 MMT::has_ownership>::type>
    vector_with_sqnorm_cache(index_type length_)
	: vector_type(length_), m_sqnorm(0) { }
    vector_with_sqnorm_cache(value_type *value_, index_type length_,
			     value_type sqnorm = 0)
	: vector_type(value_, length_), m_sqnorm(sqnorm) { }

    template<typename OtherVectorTy>
    vector_with_sqnorm_cache(
	const OtherVectorTy &pt,
	typename std::enable_if<
	is_vector_with_sqnorm_cache<OtherVectorTy>::value>::type * = nullptr)
	: vector_type(pt), m_sqnorm(pt.m_sqnorm) { }

    template<typename OtherVectorTy>
    vector_with_sqnorm_cache(
	OtherVectorTy &&pt,
	typename std::enable_if<
	is_vector_with_sqnorm_cache<OtherVectorTy>::value>::type * = nullptr)
	: vector_type(std::move(pt)), m_sqnorm(std::move(pt.m_sqnorm)) { }

/*
    vector_with_sqnorm_cache(const vector_with_sqnorm_cache &pt)
	: vector_type(pt), m_sqnorm(pt.m_sqnorm) { }
    vector_with_sqnorm_cache(vector_with_sqnorm_cache &&pt)
	: vector_type(std::move(pt)), m_sqnorm(std::move(pt.m_sqnorm)) { }
*/
    ~vector_with_sqnorm_cache() { }

    void update_sqnorm() {
	// A more efficient solution is possible for sparse vectors,
	// making use of the fact that the arguments to sq_dist() are the same.
	m_sqnorm = vector_type::sq_norm();
    }
    value_type get_sqnorm() const { return m_sqnorm; }

    template<typename OtherVectorTy>
    const typename std::enable_if<is_vector_with_sqnorm_cache<OtherVectorTy>::value, vector_with_sqnorm_cache>::type &
    operator = ( const OtherVectorTy & pt ) {
	m_sqnorm = pt.m_sqnorm;
	vector_type::operator = ( pt );
	return *this;
    }

    template<typename OtherVectorTy>
    typename std::enable_if<is_vector_with_sqnorm_cache<OtherVectorTy>::value>::type
    copy_attributes( const OtherVectorTy & pt ) {
	m_sqnorm = pt.m_sqnorm;
	vector_type::copy_attributes( pt );
    }

};

// Extend a vector (sparse or dense) with an additive counter. The counter
// can be incremented/decremented by the user. The counter is updated by
// operator += (other operators not implemented yet).
template<typename VectorTy, typename CounterTy = std::size_t,
	 typename = typename std::enable_if<is_vector<VectorTy>::value>::type>
class vector_with_add_counter : public VectorTy
{
public:
    static const bool is_vectorized = VectorTy::is_vectorized;
    typedef typename VectorTy::index_type index_type;
    typedef typename VectorTy::value_type value_type;
    typedef typename VectorTy::memory_mgmt_type memory_mgmt_type;
    typedef VectorTy vector_type;
    typedef CounterTy counter_type;

    struct _asap_tag : public tag_add_counter, vector_type::_asap_tag { };
    void asap_decl(void);

private:
    counter_type m_count;

public:
    template<typename MMT = memory_mgmt_type,
	     typename = typename std::enable_if<
		 std::is_same<MMT, memory_mgmt_type>::value &&
		 MMT::has_ownership>::type>
    vector_with_add_counter(index_type length_)
	: vector_type(length_), m_count(0) { }
    vector_with_add_counter(value_type *value_, index_type length_,
			    counter_type count = 0)
	: vector_type(value_, length_), m_count(count) { }

    template<typename OtherVectorTy>
    vector_with_add_counter(
	const OtherVectorTy &pt,
	typename std::enable_if<
	is_vector_with_add_counter<OtherVectorTy>::value>::type * = nullptr )
	: vector_type(pt), m_count(pt.m_count) { }

    template<typename OtherVectorTy>
    vector_with_add_counter(
	OtherVectorTy &&pt,
	typename std::enable_if<
	is_vector_with_add_counter<OtherVectorTy>::value>::type * = nullptr )
	: vector_type(std::move(pt)), m_count(std::move(pt.m_count)) { }

    ~vector_with_add_counter() { }

    void clear() {
	vector_type::clear();
	clear_attributes();
    }
    void clear_attributes() {
	m_count = 0;
    }

    void inc_count() { ++m_count; }
    void dec_count() { --m_count; }
    counter_type get_count() const { return m_count; }

    // Why is the base class operator += not eligible for the case covered here
    // when the other overridden operator += is not applicable?
    template<typename OtherVectorTy>
    const typename std::enable_if<!is_vector_with_add_counter<OtherVectorTy>::value, vector_with_add_counter>::type &
    operator += ( const OtherVectorTy & v ) {
	this->vector_type::operator += ( v );
	return *this;
    }

    template<typename OtherVectorTy>
    const typename std::enable_if<is_vector_with_add_counter<OtherVectorTy>::value, vector_with_add_counter>::type &
    operator += ( const OtherVectorTy & v ) {
	this->vector_type::operator += ( v );
	m_count += v.m_count;
	return *this;
    }

    template<typename OtherVectorTy>
    const typename std::enable_if<is_vector_with_add_counter<OtherVectorTy>::value, vector_with_add_counter>::type &
    operator = ( const OtherVectorTy & pt ) {
	m_count = pt.m_count;
	vector_type::operator = ( pt );
	return *this;
    }

    template<typename OtherVectorTy>
    typename std::enable_if<is_vector_with_sqnorm_cache<OtherVectorTy>::value>::type
    copy_attributes( const OtherVectorTy & pt ) {
	m_count = pt.m_count;
	vector_type::copy_attributes( pt );
    }
};

}

#endif // INCLUDED_ASAP_ATTRIBUTES_H
