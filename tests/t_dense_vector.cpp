#include <iostream>
#include <vector>
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"
#include "asap/kmeans.h"

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
	ValueTy v;
	IndexTy c;
	sv.get( i, v, c );
	os << c << ": " << v;
	if( i+1 < e )
	    os << ", ";
    }
    os << '}';
    return os;
}


template<typename VectorTy>
typename std::enable_if<VectorTy::memory_mgmt_type::has_ownership, VectorTy>::type
create( typename VectorTy::index_type length ) {
    return VectorTy( length );
}

template<typename VectorTy>
typename std::enable_if<!VectorTy::memory_mgmt_type::has_ownership, VectorTy>::type
create( typename VectorTy::index_type length ) {
    return VectorTy( new typename VectorTy::value_type[length], length );
}

template<typename IndexTy, typename ValueTy, bool IsVectorized,
	 typename Ownership>
void test( IndexTy length, char const *msg ) {
    typedef asap::dense_vector<IndexTy, ValueTy, IsVectorized, Ownership>
	dv_type;
    
    dv_type dv = create<dv_type>(10);
    for( int i=0; i < 10; ++i )
	dv[i] = 0.2 + i;
    std::cout << msg << " dv init: " << dv << ", " << dv.get_value() << std::endl;
    
    dv_type dv2 = dv;
    std::cout << msg << " dv copy: " << dv2 << ", " << dv2.get_value() << std::endl;

    dv_type dv3 = std::move(dv);
    std::cout << msg << " dv move: " << dv3 << ", " << dv3.get_value() << std::endl;
}

int main( int argc, char *argv[] ) {
    test<int, float, false, asap::mm_ownership_type>(
	10, "float[int], not vectorized, owned:" );
    test<int, float, false, asap::mm_no_ownership_type>(
	10, "float[int], not vectorized, not owned:" );
    test<int, float, true, asap::mm_ownership_type>(
	10, "float[int], vectorized, owned:" );
    test<int, float, true, asap::mm_no_ownership_type>(
	10, "float[int], vectorized, not owned:" );

    asap::dense_vector_set<asap::dense_vector<int, float, false, asap::mm_no_ownership_type>> dvs( 30, 10 );
    for( auto I=dvs.begin(), E=dvs.end(); I != E; ++I )
	for( size_t i=0; i < 10; ++i )
	    (*I)[i] = rand() % 100;

    asap::dense_vector_set<asap::vector_with_sqnorm_cache<asap::dense_vector<int, float, false, asap::mm_no_ownership_type>>> dvs_sq( 4, 10 );
    dvs_sq[3];

    asap::kmeans_operator<int, float, false> kmeans_op(8, 100);
    kmeans_op.cluster( dvs.begin(), dvs.end() );

    std::cout << "clusters:\n";
    auto & centres = kmeans_op.centres();
    size_t pt = 0;
    for( auto I=centres.begin(), E=centres.end(); I != E; ++I, ++pt )
	std::cout << pt << "\t" << *I << std::endl;
    std::cout << "Iterations: " << kmeans_op.num_iterations() << std::endl;
    std::cout << "Within cluster SSE: " << kmeans_op.within_sse() << std::endl;

    std::cout << "Create sparse vectors\n";
    std::vector<
	asap::sparse_vector<int, float, false,
			    asap::mm_ownership_type>> svs;
    svs.reserve(200);
    for( size_t i=0; i < 200; ++i ) {
	size_t len = 1 + rand() % 10;
	svs.emplace_back( 40, len );
	size_t minj = 0;
	for( size_t j=0; j < len; ++j ) {
	    size_t v = minj + ( rand()%(40-minj-(len-j)) );
	    svs[i].set( j, rand()%100, minj );
	    minj = v;
	}
	// std::cout << i << "\t: " << svs[i] << std::endl;
    }

    {
	std::cout << "sparse vector k-means\n";
	asap::kmeans_operator<int, float, false> kmeans_op(4, 40);
	kmeans_op.cluster( svs.begin(), svs.end() );

	std::cout << "clusters:\n";
	auto & centres = kmeans_op.centres();
	size_t pt = 0;
	for( auto I=centres.begin(), E=centres.end(); I != E; ++I, ++pt )
	    std::cout << pt << "\t" << *I << std::endl;
	std::cout << "Iterations: " << kmeans_op.num_iterations() << std::endl;
	std::cout << "Within cluster SSE: " << kmeans_op.within_sse() << std::endl;
    }


    return 0;
}
