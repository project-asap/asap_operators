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

#ifndef INCLUDED_ASAP_KMEANS_H
#define INCLUDED_ASAP_KMEANS_H

#include <memory>
#include <cassert>
#include <iomanip>
#include <cilk/reducer_opadd.h>

#include "asap/dense_vector.h"
#include "asap/attributes.h"
#include "asap/data_set.h"

// TODO: kmeans clustering using cosine similarity as a distance metric.
//       would be good for TF/IDF scores: x*y / ||x|| ||y||
//       meaning: put a ||.|| cache on each vector
//
// TODO: Spherical k-means

namespace asap {

template<typename VectorTy, typename WordContTy, typename VectorNameTy = void,
	 typename = typename std::enable_if<std::is_same<typename VectorTy::memory_mgmt_type,
							 mm_no_ownership_policy>::value>::type>
class kmeans_data_set : public data_set<VectorTy,WordContTy,VectorNameTy> {
    typedef data_set<VectorTy,WordContTy> base_type;

private:
    typename base_type::value_type	m_sse;
    size_t				m_num_iters;

public:
    kmeans_data_set( typename base_type::value_type sse, size_t num_iters, 
		     const char * relation,
		     const std::shared_ptr<typename base_type::index_list_type> & idx_names,
		     const std::shared_ptr<typename base_type::vector_list_type> & vectors )
	: base_type( relation, idx_names, vectors ),
	  m_sse( sse ), m_num_iters( num_iters ) { }

    kmeans_data_set( kmeans_data_set && ds ) : base_type(std::move(*static_cast<base_type*>(&ds))),
	m_sse(std::move(ds.m_sse)), m_num_iters(ds.m_num_iters) { }

    size_t num_iterations() const { return m_num_iters; }
    size_t num_clusters() const { return centres().size(); }
    typename base_type::value_type within_sse() const { return m_sse; }

    const typename base_type::vector_list_type & centres() const { return base_type::get_vectors(); }

    // This is much faster with C stdio
    void output( std::ostream & os ) {
	// No flushing
	os << std::nounitbuf;
	os.tie(0);

	os << "                                 Cluster#\n"
	   << "       Attribute     Full Data";
	for( size_t i=0; i < num_clusters(); ++i )
	    os << std::setw(15) << i;
	os << "\n";

	size_t npoints = 0;
	for( size_t i=0; i < num_clusters(); ++i )
	    npoints += centres()[i].get_count();

	char buf[32];
	snprintf( buf, sizeof(buf), "(%d)", npoints );
	os << std::setw(30) << buf;
	for( size_t i=0; i < num_clusters(); ++i ) {
	    snprintf( buf, sizeof(buf), "(%d)", centres()[i].get_count() );
	    os << std::setw(15) << buf; 
	}
	os << "\n";

	{
	    const char lnbuf[] = "==============================";
	    const size_t lnlen = sizeof(lnbuf)-1;

	    os << &lnbuf[lnlen-30];
	    for( size_t i=0; i < num_clusters(); ++i )
		os << &lnbuf[lnlen-15];
	    os << '\n';
	}

	std::streamsize old_prec = os.precision(8);
	os << std::fixed;
	size_t ndim = base_type::get_dimensions();
	size_t ncentres = num_clusters();
	auto centres_ =  &centres()[0];
	// Use iterator for efficiency (in case lookup is slow), but need
	// additional modelling of word_bank in case container stores tripples
	// auto WI = base_type::index_cbegin();
	for( typename base_type::index_type i=0; i < ndim; ++i ){ // , ++WI ) {
	    os << std::setw(16) << std::left << base_type::get_index(i) //  *WI 
	       << std::right;
	    typename base_type::value_type s = 0;
	    for( size_t k=0; k < ncentres; ++k )
		s += centres_[k][i] * centres_[k].get_count();
	    s /= (typename base_type::value_type)npoints;
	    os << std::setw(14) << s;
	    for( size_t k=0; k < ncentres; ++k )
		os << std::setw(15) << centres_[k][i];
	    os << '\n';
	}
	os.precision(old_prec);
	
	// Flushing
	os << std::unitbuf;
    }
};

template<typename IndexTy, typename ValueTy, bool IsVectorized,
	 typename Allocator = std::allocator<ValueTy>>
class kmeans_operator {
public:
    static const bool is_vectorized = IsVectorized;
    typedef IndexTy index_type;
    typedef ValueTy value_type;
    typedef Allocator allocator_type;
    typedef vector_with_sqnorm_cache<vector_with_add_counter<
	dense_vector<index_type, value_type, is_vectorized,
		     mm_no_ownership_policy, allocator_type>,
	size_t>> centre_vector_type;
    typedef dense_vector_set<centre_vector_type> kmeans_dense_vector_set;

private:
    class dense_vector_set_monoid : public cilk::monoid_base<kmeans_dense_vector_set>
    {
	typedef centre_vector_type vector_type;
	typedef kmeans_dense_vector_set vector_set_type;

    public:
	static void reduce( vector_set_type *left, vector_set_type *right ) {
	    assert( left->number() == right->number() );
	    for( size_t i=0; i < left->number(); ++i )
		// Avoid adding zero-vectors, indicated by zero count in K-Means
		if( (*right)[i].get_count() > 0 )
		    (*left)[i] += (*right)[i];
	}
    };

private:
    kmeans_dense_vector_set m_centres;
    size_t m_num_clusters;
    const size_t m_vector_length;
    size_t m_num_iters;
    value_type m_sse;

public:
    kmeans_operator(size_t num_clusters, size_t vector_length)
	: m_centres( num_clusters, vector_length),
	  m_num_clusters( num_clusters ), m_vector_length( vector_length ),
	  m_num_iters( 0 ), m_sse( 0 ) { }
    ~kmeans_operator() { }

private:
    template<typename InputIterator>
    void kmeansPP_init(InputIterator I, InputIterator E, size_t *cluster_asgn ) {
	size_t num_points = std::distance(I, E);
	size_t c = 0;
	size_t pt;
	{ // First point
	    pt = rand() % num_points;
	    cluster_asgn[pt] = c;
	    InputIterator II = I;
	    std::advance( II, pt );
	    m_centres[c] += *II;
	    m_centres[c].inc_count(); // will inc to 1 only
	    if( is_sparse_vector<decltype(*I)>::value )
		m_centres[c].update_sqnorm();
	    c++;
	    // std::cerr << "PP: pt " << c << " is " << pt << "\n";
	}
	
	value_type * D = new value_type[num_points]; // zero-init
	
	cilk::reducer< cilk::op_add<value_type> > sum( 0 );
	cilk_for( InputIterator II=I; II != E; ++II ) {
	    size_t pos = std::distance(I, II);
	    value_type distance = II->sq_dist( m_centres[c-1] );
	    D[pos] = distance;
	    *sum += distance;
	    // std::cerr << "sq distance " << distance << " / " << D[pos] << "\n";
	    assert( distance >= 0 );
	}
	D[pt] = 0; // zero probability
	// std::cerr << "sum=" << sum.get_value() << "\n";

	while( c < m_num_clusters ) {
	    double r = (double)rand() / (double)RAND_MAX;
	    double cum = 0;
	    r *= sum.get_value();
	    size_t pt = 0;
	    for( InputIterator II=I; II != E; ++II, ++pt ) {
		cum += D[pt];
		// std::cerr << "r=" << r << " cum=" << cum << " at pt=" << pt << " D[pt]=" << D[pt] << "\n";
		if( cum >= r ) {
		    m_centres[c] += *II;
		    m_centres[c].inc_count(); // will inc to 1 only
		    if( is_sparse_vector<decltype(*I)>::value )
			m_centres[c].update_sqnorm();
		    c++;
		    D[pt] = 0;
		    // std::cerr << "PP: pt " << c << " is " << pt << "\n";
		    break;
		}
	    }
	    if( c >= m_num_clusters )
		break;

	    sum.set_value( 0 );
	    cilk_for( InputIterator II=I; II != E; ++II ) {
		size_t pos = std::distance(I, II);
		value_type distance = II->sq_dist( m_centres[c-1] );
		if( D[pos] > distance )
		    D[pos] = distance;
		// if( distance == 0 ) std::cerr << "set D[" << pos << "] to zero\n";
		*sum += D[pos];
	    }
/*
	    if( sum.get_value() == 0 ) {
		std::cerr << "NO MORE DISTANCES!\n";
		// for( int i=0; i < num_points; ++i )
		// std::cerr << "D[" << i << "]=" << D[i] << "\n";
	    }
*/
	}

	delete[] D;
    }
    
public:
    // A range of vectors representing points to cluster. The vectors
    // must be compatible with the IndexTy and ValueTy provided to the
    // class template.
    // The InputIterator must be a RandomAccessIterator
    template<typename InputIterator>
    size_t cluster(InputIterator I, InputIterator E, size_t max_iters = 0) {
	size_t num_points = std::distance(I, E);

	// size_t cluster_asgn[num_points];
	size_t * cluster_asgn = new size_t[num_points];

	// Set all centres and their associated counters to 0.
	m_centres.clear();
	// Initialize centres by mapping inputs randomly to centres
	kmeansPP_init(I, E, cluster_asgn );
/*
	size_t pt=0;
	for( InputIterator II=I; II != E; ++II, ++pt ) {
	    size_t c = rand() % m_num_clusters;
	    cluster_asgn[pt] = c;
	    m_centres[c] += *II;
	    m_centres[c].inc_count();
	}
*/
	normalize( m_centres );

	// Iterate K-Means loop up to max_iters times
	size_t num_iters = 1;
	while( kmeans_iterate( I, E, cluster_asgn ) )
	    if( ++num_iters >= max_iters && max_iters > 0 )
		break;
        delete[] cluster_asgn;

	return m_num_iters = num_iters;
    }

    value_type within_sse() const { return m_sse; }
    size_t num_iterations() const { return m_num_iters; }
    const kmeans_dense_vector_set &centres() const {
	return m_centres;
    }
    kmeans_dense_vector_set &centres() {
	return m_centres;
    }

private:
    template<typename InputIterator>
    bool kmeans_iterate( InputIterator I, InputIterator E,
			 size_t cluster_asgn[] ) {
	bool modified = false;

	std::cerr << "***** ITER ***** " << m_sse << "\n";

	// Use a Cilk reducer to assign points to clusters
	cilk::reducer<dense_vector_set_monoid>
	    new_centres( m_num_clusters, m_vector_length );
	// Set vectors and cluster sizes to 0
	new_centres->clear();

	// Pre-calculate square norms for the centres
	if( is_sparse_vector<decltype(*I)>::value ) {
	    for( size_t c=0; c < m_num_clusters; ++c )
		m_centres[c].update_sqnorm();
	}

	cilk::reducer< cilk::op_add<value_type> > sse( 0 );

	cilk_for( InputIterator II=I; II != E; ++II ) {
	    size_t pt = std::distance( I, II );
	    // Possibly a fresh view has been served. Check for initialization.
	    // This counter-acts a short-coming of Cilk reducers: it is not
	    // possible to initialize the views with parameters specific to
	    // the problem instance.
	    if( new_centres->check_init( m_num_clusters, m_vector_length ) )
		new_centres->clear();

	    // Assign points to cluster.
	    value_type smallest_distance
		= std::numeric_limits<value_type>::max();
	    size_t new_cluster_id = m_num_clusters; // invalid value
	    for(size_t j = 0; j < m_num_clusters; j++) {
		// assign point to cluster with smallest total squared distance
		value_type distance = II->sq_dist( m_centres[j] );
		if( !(distance >= 0) )
		    std::cerr << "distance is " << distance << " for "
			      << *II << " and " << m_centres[j] << " sqnorm "
			      << m_centres[j].get_sqnorm() << "\n";
		assert( distance >= 0 );
		if( distance < smallest_distance ) {
		    smallest_distance = distance;
		    new_cluster_id = j;
		}
	    }
	    assert( new_cluster_id < m_num_clusters
		    && "Some cluster must be the closest for any point" );

	    // if new cluster then update modified flag
	    if( new_cluster_id != cluster_asgn[pt] ) {
		// benign race; works well. Alternative: reduction(|: modified)
		modified = true;
		cluster_asgn[pt] = new_cluster_id;

	    }

	    (*new_centres)[new_cluster_id] += *II;
	    (*new_centres)[new_cluster_id].inc_count();
	    *sse += smallest_distance; // add up squared distances
	}

	normalize( *new_centres );

	// Alternative way of assessing convergence
	if( std::is_floating_point<value_type>::value && modified ) {
	    value_type epsilon = 1e-4;
	    modified = false;
	    // Note: we have a sqnorm cache on m_centres, not on new_centres
	    for( int i=0; i < m_num_clusters; ++i ) {
		value_type d = (*new_centres)[i].sq_dist( m_centres[i] );
		std::cerr << "centre " << i << " moves over " << d << "\n"; 
		if( d >= epsilon*epsilon ) {
		    modified = true;
		    break;
		}
	    }
	}

	new_centres->swap( m_centres );
	m_sse = sse.get_value();
	assert( m_sse >= 0 );
	return modified;
    }
    void normalize( kmeans_dense_vector_set & centres ) {;
	// TODO: cilk_for. if range large enough... How much is large enough?
	//       depends on #length (#clusters very small)
	/*cilk_*/
	for( size_t c=0; c < m_num_clusters; ++c ) {
	    size_t cnt = centres[c].get_count();
	    if( cnt > 0 ) // cluster must be non-empty to scale
		centres[c].scale( value_type(1)/value_type(cnt) );
	    else
		std::cerr << "WARN: cluster " << c << " is empty\n";
	}
    }
};

template<typename DataSetTy>
struct kmeans_data_set_type_creator {
    typedef typename DataSetTy::vector_type vector_type;
    typedef typename DataSetTy::value_type value_type;
    typedef typename DataSetTy::index_type index_type;
    typedef typename DataSetTy::word_container_type word_container_type;
    typedef typename DataSetTy::allocator_type allocator_type;
    static const bool is_vectorized = vector_type::is_vectorized;

    typedef typename
    kmeans_operator<index_type, value_type, is_vectorized,
		    allocator_type>::centre_vector_type centre_vector_type;

    typedef kmeans_data_set<centre_vector_type, word_container_type> data_set_type;
};

template<typename DataSetTy>
typename kmeans_data_set_type_creator<DataSetTy>::data_set_type
kmeans( const DataSetTy & data_set, size_t num_clusters,
	size_t max_iters = 0 ) {
    typedef typename DataSetTy::vector_type vector_type;
    typedef typename DataSetTy::value_type value_type;
    typedef typename DataSetTy::index_type index_type;
    typedef typename DataSetTy::allocator_type allocator_type;
    static const bool is_vectorized = vector_type::is_vectorized;

    typedef kmeans_operator<index_type, value_type, is_vectorized,
			    allocator_type> kmeans_type;
    typedef typename kmeans_type::kmeans_dense_vector_set kmeans_vector_set;

    kmeans_type op( num_clusters, data_set.get_dimensions() );
    op.cluster( data_set.vector_cbegin(), data_set.vector_cend(), max_iters );

    typedef typename kmeans_data_set_type_creator<DataSetTy>::data_set_type
	data_set_type;
    typedef typename data_set_type::index_list_type index_list_type;
    typedef typename data_set_type::vector_list_type vector_list_type;

    std::shared_ptr<kmeans_vector_set> centres
	= std::make_shared<kmeans_vector_set>( std::move(op.centres()) );

    return data_set_type( op.within_sse(), op.num_iterations(), "kmeans",
			  data_set.get_index_ptr(), centres );
}


}

#endif // INCLUDED_ASAP_KMEANS_H
