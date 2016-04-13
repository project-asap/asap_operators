#include <iterator>

#include "asap/utils.h"
#include "asap/arff.h"
#include "asap/dense_vector.h"
#include "asap/sparse_vector.h"

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
	os << c << ": " << v;
	if( i+1 < e )
	    os << ", ";
    }
    os << '}';
    return os;
}


template<typename vector_type>
void test( const char * filename ) {
    typedef asap::data_set<vector_type> data_set_type;

    bool is_stored_sparse;
    data_set_type data
	= asap::arff_read<data_set_type>( std::string(filename), is_stored_sparse );

    std::cout << "Relation: " << data.get_relation() << std::endl;
    std::cout << "Stored sparsely: " << is_stored_sparse << std::endl;
    std::cout << "Dimensions: " << data.get_dimensions() << std::endl;
    for( typename data_set_type::const_index_iterator
	     I=data.index_cbegin(), E=data.index_cend(); I != E; ++I )
	std::cout << " '" << *I << '\'';
    std::cout << std::endl;
    std::cout << "Points: " << data.get_num_points() << std::endl;
    for( typename data_set_type::const_vector_iterator
	     I=data.vector_cbegin(), E=data.vector_cend(); I != E; ++I )
	std::cout << '\t' << *I << '\n';
    std::cout << std::endl;
}

int main( int argc, char *argv[] ) {
    if( argc < 2 )
	fatal( "usage: t_arff_read input-filename" );

    const char * filename = argv[1];

    std::cout << "==== Reading ARFF file: dense vectors with ownership\n";
    test<asap::dense_vector<size_t, float, false, asap::mm_ownership_type>>(
	filename );
    
    std::cout << "==== Reading ARFF file: dense vectors without ownership\n";
    test<asap::dense_vector<size_t, float, false, asap::mm_no_ownership_type>>(
	filename );
    
    std::cout << "==== Reading ARFF file: sparse vectors with ownership\n";
    test<asap::sparse_vector<size_t, float, false, asap::mm_ownership_type>>(
	filename );
    
    std::cout << "==== Reading ARFF file: sparse vectors without ownership\n";
    test<asap::sparse_vector<size_t, float, false, asap::mm_no_ownership_type>>(
	filename );

    return 0;
}
