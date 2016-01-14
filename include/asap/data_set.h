/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_DATA_SET_H
#define INCLUDED_ASAP_DATA_SET_H

#include <vector>
#include "asap/traits.h"
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/word_bank.h"

namespace asap {

namespace internal {

template<typename vector_type, typename = void>
struct select_vector_set_type {
    typedef std::vector<vector_type> type;
};

template<typename vector_type>
struct select_vector_set_type<vector_type,
			      typename std::enable_if<is_dense_vector<vector_type>::value && std::is_same<typename vector_type::memory_mgmt_type, mm_no_ownership_policy>::value>::type> {
    typedef dense_vector_set<vector_type> type;
};

template<typename vector_type>
struct select_vector_set_type<vector_type,
			      typename std::enable_if<is_sparse_vector<vector_type>::value && std::is_same<typename vector_type::memory_mgmt_type, mm_no_ownership_policy>::value>::type> {
    typedef sparse_vector_set<vector_type> type;
};

}

// A data set holding a list of vectors of type VectorTy and a list of names
// identfying the coordinates/dimensions of the vectors.
template<typename VectorTy, typename WordContTy>
class data_set {
public:
    typedef VectorTy 	 	 	 	 	vector_type;
    typedef WordContTy 	 	 	 		word_container_type;

    typedef typename VectorTy::index_type	 	index_type;
    typedef typename VectorTy::value_type	 	value_type;
    typedef typename VectorTy::memory_mgmt_type	 	memory_mgmt_type;
    typedef typename VectorTy::allocator_type	 	allocator_type;

    typedef word_container_type  		 	index_list_type;
    typedef typename
    internal::select_vector_set_type<vector_type>::type	vector_list_type;

    typedef typename index_list_type::const_iterator	const_index_iterator;
    typedef typename vector_list_type::const_iterator	const_vector_iterator;
    typedef typename vector_list_type::iterator		vector_iterator;

private:
    // TODO: use VectorTy's allocator_type in both vectors?
    const char            		* m_relation;
    std::shared_ptr<index_list_type>	  m_idx_names;
    std::shared_ptr<vector_list_type>	  m_vectors;

public:
    data_set() : m_relation( nullptr ) { }
    data_set( const char * relation,
	      const std::shared_ptr<index_list_type> & idx_names,
	      const std::shared_ptr<vector_list_type> & vectors )
	: m_relation( relation ), m_idx_names( idx_names ),
	  m_vectors( vectors ) { }
    data_set( const char * relation,
	      std::shared_ptr<index_list_type> && idx_names,
	      std::shared_ptr<vector_list_type> && vectors )
	: m_relation( relation ),
	  m_idx_names( std::move( idx_names ) ),
	  m_vectors( std::move( vectors ) ) { }
    data_set( const data_set & ds ) = delete;
    data_set( data_set && ds ) 
	: m_relation( ds.m_relation ),
	  m_idx_names( std::move( ds.m_idx_names ) ),
	  m_vectors( std::move( ds.m_vectors ) ) { }

    ~data_set() { }

    size_t get_dimensions() const {
	return m_idx_names->size();
    }
    size_t get_num_points() const {
	return m_vectors->size();
    }
    const char * get_relation() const {
	return m_relation;
    }

    const std::shared_ptr<index_list_type> & get_index_ptr() const {
	return m_idx_names;
    }
    const index_list_type & get_index() const { return *m_idx_names; }
    const vector_list_type & get_vectors() const { return *m_vectors; }
    
    const_index_iterator index_cbegin() const { return m_idx_names->cbegin(); }
    const_index_iterator index_cend() const { return m_idx_names->cend(); }

    const char * get_index( size_t n ) const {
	return (*m_idx_names)[n];
    }

    const_vector_iterator vector_cbegin() const { return m_vectors->cbegin(); }
    const_vector_iterator vector_cend() const { return m_vectors->cend(); }
    vector_iterator vector_begin() { return m_vectors->begin(); }
    vector_iterator vector_end() { return m_vectors->end(); }
};

}

#endif // INCLUDED_ASAP_DATA_SET_H
