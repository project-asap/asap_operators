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


#ifndef INCLUDED_ASAP_NGRAM_BANK_H
#define INCLUDED_ASAP_NGRAM_BANK_H

#include "asap/word_bank.h"

namespace asap {

template<typename IndexTy, typename WordBankTy, size_t N_>
class ngram_container {
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;
    static const size_t N = N_;

protected:
    index_type		m_words;
    word_bank_type	m_storage;

public:
    ngram_container() { }
    // Try to avoid use of the copy constructor
    ngram_container( const ngram_container & wc )
	: m_words( wc.m_words ), m_storage( wc.m_storage ) { }
    ngram_container( ngram_container && wc )
	: m_words( std::move(wc.m_words) ),
	  m_storage( std::move(wc.m_storage) ) { }

    // template<typename... Args>
    // ngram_container( Args... args ) : m_storage( args... ) { }

    const word_bank_type & storage() const { return m_storage; }

    size_t size() const		{ return m_words.size(); }
    bool empty() const		{ return m_words.empty(); }

    void clear()		{ m_words.clear(); m_storage.clear(); } 
    void swap( ngram_container & wb ) {
	m_words.swap( wb.m_words );
	m_storage.swap( wb.m_storage );
    }

    void enregister( std::shared_ptr<char> & buf ) {
	m_storage.enregister( buf );
    }

    // Build up a string
    const char * append( const char * start, const char * str, size_t len ) {
	return m_storage.append( start, str, len );
    }
    void erase( const char * w ) {
	m_storage.erase( w );
    }
};

template<size_t N>
struct value_cmp<text::ngram<N>> {
    bool operator () ( const text::ngram<N> & v1, const text::ngram<N> & v2 ) const {
	return text::ngram_cmp()( v1, v2 );
    }
};

template<typename IndexTy, typename WordBankTy, size_t N_>
class ngram_map;

// IndexTy is sequential container such std::vector, std::deque, std::list
// Assumed is that IndexTy::value_type is const char *
template<typename IndexTy, typename WordBankTy, size_t N_>
class ngram_kv_list : public ngram_container<IndexTy,WordBankTy,N_> {
    typedef word_container<IndexTy,WordBankTy> base_type;
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;

    typedef typename index_type::value_type	value_type;
    typedef typename value_type::first_type	key_type;
    typedef typename value_type::second_type	mapped_type;

    typedef typename index_type::const_iterator	const_iterator;
    typedef typename index_type::iterator	iterator;

    static const bool is_managed = word_bank_type::is_managed;
    static const bool can_sort = true;
    static const bool always_sorted = false;

    static const size_t N = N_;

private:
    template<typename OtherIndexTy>
    struct is_compatible
	: std::integral_constant<
	bool,
	std::is_same<typename OtherIndexTy::key_type, key_type>::value
	&&
	std::is_same<typename OtherIndexTy::mapped_type, mapped_type>::value> {
    };

public:
    ngram_kv_list() { }
    template<typename... Args>
    ngram_kv_list( Args... args ) : base_type( args... ) { }

    void set_growth( size_t w, size_t b ) { }

    // Memorize the word, but do not store it in the word list
    const char * memorize( char * p, size_t len ) {
	return this->m_storage.store( p, len );
    }

    // Memorize the word and store it in the word list as well
    const char * index( char * p, size_t len ) {
	const char * w = this->m_storage.store( p, len );
	this->m_words.push_back( w );
	return w;
    }

    // Only store in index
    void index_only( const char * w ) {
	this->m_words.push_back( w );
    }

    void resize( size_t sz ) { this->m_words.resize( sz ); }
    void reserve( size_t sz ) { this->m_words.reserve( sz ); }

    // Retrieve n-th word item in the container.
    const value_type & operator[] ( size_t n ) const {
	return this->m_words[n];
    }

    iterator begin() { return this->m_words.begin(); }
    iterator end() { return this->m_words.end(); }

    const_iterator cbegin() const { return this->m_words.cbegin(); }
    const_iterator cend() const { return this->m_words.cend(); }

    const_iterator find( const text::ngram<N> & w ) const {
	value_type val
	    = std::make_pair( w, typename value_type::second_type() );
	pair_cmp<value_type,value_type> cmp;
	for( const_iterator I=cbegin(), E=cend(); I != E; ++I )
	    if( !cmp( *I, val ) && !cmp( val, *I ) )
		return I;
	return cend();
    }

    const_iterator binary_search( const text::ngram<N> & w ) const {
	value_type val
	    = std::make_pair( w, typename value_type::second_type() );
	// val.first = w; // only if std::pair
	std::pair<const_iterator,bool> ret
	    = binary_search( cbegin(), cend(), this->size(), val,
			     pair_cmp<value_type,value_type>() );
	return ret.second ? ret.first : cend();
    }

    // TODO: this is imprecise:
    // + Not clear if range [I,E) is all of wb, or only part of it
    // + As such, copying over all of wb may be too much
    // + Ideally want to translate all strings into existing word bank
    // At the moment, this is used only with *this initially empty, so this
    // is ok.
    template<typename InputIterator>
    void insert( InputIterator I, InputIterator E, const word_bank_base & wb ) {
	assert( this->m_words.empty() );
	std::for_each( I, E, [&]( typename index_type::value_type & val ) { this->m_words.push_back( val ); } );
	// this->m_words.insert( I, E );
	this->m_storage.copy( wb );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( const ngram_map<OtherIndexTy,OtherWordBankTy,N> & wc ) {
	assert( this->m_words.empty() );
	this->m_words.insert( this->m_words.end(), wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( ngram_map<OtherIndexTy,OtherWordBankTy,N> && wc ) {
	assert( this->m_words.empty() );
	this->m_words.insert( this->m_words.end(),
			      wc.begin(),
			      wc.end() );
	// std::make_move_iterator(wc.begin()),
	// std::make_move_iterator(wc.end()) );
	this->m_storage.copy( std::move(wc.storage()) );
	wc.clear();
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_map<OtherIndexTy,OtherWordBankTy,N> & rhs ) {
	typedef typename ngram_map<OtherIndexTy,OtherWordBankTy,N>::value_type
	    other_value_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.size(),
		     pair_cmp<value_type,value_type>(),
		     pair_nonzero_reducer<value_type,other_value_type>() );
	this->m_storage.copy( rhs.storage() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_kv_list<OtherIndexTy,OtherWordBankTy,N> & rhs ) {
	typedef typename ngram_kv_list<OtherIndexTy,OtherWordBankTy,N>::value_type
	    ::second_type other_value_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.size(),
		     pair_cmp<value_type,value_type>(),
		     pair_nonzero_reducer<value_type,other_value_type>() );
	this->m_storage.copy( rhs.storage() );
    }


    // Add in all contents from rhs into lhs (*this) and clear rhs
    // Assumes both *this and rhs are sorted by key (whatever sorting function
    // is used ...)
    void reduce( ngram_kv_list & rhs ) {
	// TODO: consider parallel merge (std::experimental::parallel_merge)
	// TODO: Better with move iterators?
	core_reduce( rhs.begin(), rhs.end(), rhs.size(),
		     pair_cmp<value_type,value_type>(),
		     pair_add_reducer<value_type,value_type>() );
	this->m_storage.copy( std::move(rhs.storage()) );
	rhs.clear();
    }

private:
    template<class InputIt, class Compare, class Reduce>
    void core_reduce(InputIt first2, InputIt last2, size_t size2,
		     Compare cmp, Reduce reduce) {
	index_type joint;
	joint.reserve( this->size() + size2 ); // worst case
	core_merge( this->begin(), this->end(), 
		    first2, last2, std::back_inserter(joint),
		    pair_cmp<value_type,value_type>(),
		    pair_add_reducer<value_type,value_type>() );
	this->m_words.swap( joint );
    }
    template<class InputIt, class OutputIt, class Compare, class Reduce>
    OutputIt core_merge(iterator first1, iterator last1,
			 InputIt first2, InputIt last2,
			 OutputIt d_first, Compare cmp, Reduce reduce) {
	for (; first1 != last1; ++d_first) {
	    if (first2 == last2) {
		return std::copy(first1, last1, d_first);
	    }
	    if( !cmp(*first1, *first2) ) {
		if( !cmp(*first2, *first1) ) { // equal
		    auto val = *first1;
		    reduce( val, *first2 );
		    *d_first = val;
		    ++first1;
		} else {
		    *d_first = *first2;
		}
		++first2;
	    } else {
		*d_first = *first1;
		++first1;
	    }
	}
	return std::copy(first2, last2, d_first);
    }

    // iterator is a RandomAccess iterator
    // n == std::distance( I, E );
    template<typename InputIt, typename Compare>
    std::pair<InputIt,bool>
    binary_search( InputIt I, InputIt E, size_t n, const value_type & val,
		   Compare cmp ) const {
	if( n == 0 )
	    return std::make_pair( E, false );
	else if( n == 1 )
	    return std::make_pair( I, !cmp( *I, val ) && !cmp( val, *I ) );

	size_t l = n/2;
	InputIt M = std::next( I, l );

	if( cmp( *M, val ) ) // *M < val, search right sub-range
	    return binary_search( M, E, n-l, val, cmp );
	else if( cmp( val, *M ) ) // val < *M search left sub-range
	    return binary_search( I, M, l, val, cmp );
	else // val == *M
	    return std::make_pair( M, true );
    }
};

// IndexTy is a map such as std::map, std::unordered_map where the mapped_type
// is an integral type, or any type supporting:
//  - default constructor
//  - operator ++ ()
//  - operator += ()
template<typename IndexTy, typename WordBankTy, size_t N_>
class ngram_map : public ngram_container<IndexTy,WordBankTy,N_> {
    typedef ngram_container<IndexTy,WordBankTy,N_> base_type;
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;

    typedef typename index_type::key_type	key_type;
    typedef typename index_type::mapped_type	mapped_type;
    typedef typename index_type::value_type	value_type;
    typedef typename index_type::const_iterator	const_iterator;
    typedef typename index_type::iterator	iterator;

    static const bool is_managed = word_bank_type::is_managed;
    static const bool can_sort = false;
    static const bool always_sorted
	= is_specialization_of<std::map, index_type>::value;

    static const size_t N = N_;

    template<typename OtherIndexTy, typename OtherWordBankTy, size_t OtherN>
    friend class ngram_map;

private:
    template<typename OtherIndexTy>
    struct is_compatible
	: std::integral_constant<
	bool,
	std::is_same<typename OtherIndexTy::key_type, key_type>::value
	&&
	std::is_same<typename OtherIndexTy::mapped_type, mapped_type>::value> {
    };

    template<typename OtherIndexTy>
    struct is_compatible_kv
	: std::integral_constant<
	bool,
	std::is_same<typename OtherIndexTy::value_type::first_type, key_type>::value
	&&
	std::is_same<typename OtherIndexTy::value_type::second_type, mapped_type>::value> {
    };

public:
    ngram_map() { static_assert( !is_managed, "Require managed word_bank" ); }
    ngram_map( const ngram_map & wm )
	: base_type( *static_cast<const base_type *>( &wm ) ) { }
    ngram_map( ngram_map && wm ) : base_type( std::move(wm) ) { }
    // template<typename... Args>
    // ngram_map( Args... args ) : base_type( args... ) { }

    void set_growth( size_t w, size_t b ) {
	this->m_words.set_growth( w, b );
    }

    // Memorize the word, but do not store it in the word list
    const char * memorize( char * p, size_t len ) {
	return this->m_storage.store( p, len );
    }

    // Memorize the word and store it in the word list as well
    const char * store( char * p, size_t len ) {
	return this->m_storage.store( p, len );
    }

    void index( const asap::text::ngram<N> & ng ) {
	++this->m_words[ng];
    }

    iterator begin() { return this->m_words.begin(); }
    iterator end() { return this->m_words.end(); }

    const_iterator cbegin() const { return this->m_words.cbegin(); }
    const_iterator cend() const { return this->m_words.cend(); }

    void reserve( size_t n ) { reserve_space( this->m_words, n ); }

    iterator find( const key_type & w ) {
	return this->m_words.find( w );
    }
    const_iterator find( const key_type & w ) const {
	return this->m_words.find( w );
    }
    // For reference and ease of substituting types in templates
    iterator binary_search( const key_type & w ) {
	return find( w );
    }
    const_iterator binary_search( const key_type & w ) const {
	return find( w );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_map<OtherIndexTy,OtherWordBankTy,N_> & rhs ) {
	typedef typename ngram_map<OtherIndexTy,OtherWordBankTy,N_>::mapped_type
	    other_mapped_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     mapped_nonzero_reducer<mapped_type,other_mapped_type>() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_kv_list<OtherIndexTy,OtherWordBankTy,N> & rhs ) {
	typedef typename ngram_kv_list<OtherIndexTy,OtherWordBankTy,N>
	    ::value_type::second_type other_mapped_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     mapped_nonzero_reducer<mapped_type,other_mapped_type>() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( const ngram_map<OtherIndexTy,OtherWordBankTy,N_> & wc ) {
	this->m_words.insert( wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( ngram_map<OtherIndexTy,OtherWordBankTy,N_> && wc ) {
	// std::make_move_iterator on wc.begin() and end() segfaults
	// if wc is a asap::hashtable
	this->m_words.insert( wc.begin(), wc.end() );
	this->m_storage.copy( std::move(wc.storage()) );
	wc.clear();
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible_kv<OtherIndexTy>::value>::type
    insert( const ngram_kv_list<OtherIndexTy,OtherWordBankTy,N> & wc ) {
	this->m_words.insert( wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible_kv<OtherIndexTy>::value>::type
    insert( ngram_kv_list<OtherIndexTy,OtherWordBankTy,N> && wc ) {
	this->m_words.insert( wc.begin(), wc.end() );
	this->m_storage.copy( std::move(wc.storage()) );
	wc.clear();
    }


    // Add in all contents from rhs into lhs and clear rhs
    void reduce( ngram_map & rhs ) {
	// Assumes Reducer is commutative
	if( this->size() < rhs.size() )
	    this->swap( rhs );
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     mapped_add_reducer<mapped_type,mapped_type>() );
	rhs.clear();
    }
private:
    // Copy in all contents from rhs into *this.
    // Retains rhs. Shares word storage.
    template<typename InputIterator, typename Reducer>
    void core_reduce( InputIterator Is, InputIterator E,
		      const word_bank_base & storage, Reducer reducer_fn ) {
	bool any_word_new = false;
#if 1
	// This version is good for the pre_alloc word bank, but not for
	// the managed one as we struggle with translating strings.
	for( InputIterator I=Is; I != E; ++I ) {
	    // Speculatively map word into our own word bank. This is necessary
	    // only if the word was not present yet. In case of pre_alloc
	    // word bank, this is unnecessary anyway (no-op).
	    // The reason hereto is that we cannot overwrite the key
	    // once the element has been inserted. Unless if we force it
	    // with a const_cast...
	    key_type w = I->first;
/*
	    if( word_bank_type::is_managed ) { // record new copy of ngram
		size_t len = strlen( I->first );
		w = memorize( (char*)I->first, len );
	    }
*/

	    // Note: reconstruct value_type from key and mapped_type as
	    //       we may call this function with different mapped_types
	    value_type keyval( w, mapped_type(0) );
	    reducer_fn( keyval.second, I->second );

	    // Lookup translated word and 
	    std::pair<iterator,bool> ret = this->m_words.insert( keyval );
	    if( ret.second ) {
		// Value was freshly inserted. Now remap the string.
		// Unseen words need to be stored into container of LHS
		// unless if we retain all files in memory, then do it
		// en bloc and skip the storage step (no-op but for strlen).
		any_word_new = true;

	    } else {
		// Not inserted - key already occurred
		reducer_fn( ret.first->second, I->second );
/*
		if( word_bank_type::is_managed ) // erase redundant copy of word
		    this->erase( w );
*/

		// TODO: consider dropping the old word and setting I->first
		//       to ret.first->first; also need to push the appropriate
		//       parts of storage onto I's storage. This is hard
		//       (appropriate parts) and we currently don't have the
		//       container. Reason to do this is so we can reduce
		//       memory footprint.
	    }
	}
#elif 0
	iterator hint = this->m_words.begin();
	typename index_type::key_compare cmp = this->m_words.key_comp();

	for( other_const_iterator I=rhs.cbegin(), E=rhs.cend(); I != E; ++I ) {
	    iterator L = this->m_words.lower_bound( I->first );
	    // Found with equal keys?
	    if( L != this->m_words.end()
		&& !cmp( L->first, I->first ) && !cmp( I->first, L->first ) )
		reducer_fn( L->second, I->second );
	    else {
		iterator where = L;
		if( where == this->m_words.end() )
		    where = hint;

		// Unseen words need to be stored into container of LHS
		// unless if we retain all files in memory, then do it
		// en bloc and skip the storage step (no-op but for strlen).
		any_word_new = true;
		const text::ngram<N> & w = I->first;
/*
		if( word_bank_type::is_managed ) // record new copy of word
		    w = memorize( (char*)w, strlen( w ) );
*/
		// ++this->m_words[w]; // count word (set to 1)
		value_type keyval( w, mapped_type(0) );
		reducer_fn( keyval.second, I->second );
		hint = this->m_words.insert( where, keyval );
	    }
	}
#else
	for( other_const_iterator I=rhs.cbegin(), E=rhs.cend(); I != E; ++I ) {
	    iterator L = this->m_words.find( I->first );
	    if( L != this->m_words.end() ) {
		reducer_fn( L->second, I->second );
	    } else {
		// Unseen words need to be stored into container of LHS
		// unless if we retain all files in memory, then do it
		// en bloc and skip the storage step (no-op but for strlen).
		any_word_new = true;
		const text::ngram<N> & w = I->first;
/*
		if( word_bank_type::is_managed ) // record new copy of word
		    w = memorize( (char*)w, strlen( w ) );
*/
		// ++this->m_words[w]; // count word (set to 1)
		value_type keyval( w, mapped_type(0) );
		reducer_fn( keyval.second, I->second );
		this->m_words.insert( keyval );
	    }
	}
#endif
	// TODO: measure how frequently this happens and its impact on
	//       overall memory footprint
	// if( !any_word_new )
	// std::cerr << "All words in right argument also in left argument\n";
	if( /*!word_bank_type::is_managed && */ any_word_new )
	    this->m_storage.copy( storage );
    }
};


template<typename WordContainerTy>
class ngram_container_reducer {
    typedef WordContainerTy type;
    static const size_t N = type::N;

    struct Monoid : cilk::monoid_base<type> {
	static void reduce( type * left, type * right ) {
	    left->reduce( *right );
	}
	static void identity( type * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) type();
	    p->set_growth( 1, 2 );
	    // TODO: p->private_reserve(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    ngram_container_reducer() : imp_() { }

    void swap( type & c ) {
	imp_.view().swap( c );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_map<OtherIndexTy,OtherWordBankTy,N> & rhs ) {
	imp_.view().count_presence( rhs );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const ngram_kv_list<OtherIndexTy,OtherWordBankTy,N> & rhs ) {
	imp_.view().count_presence( rhs );
    }

    type & get_value() { return imp_.view(); }
};


} // namespace asap

#endif // INCLUDED_ASAP_NGRAM_BANK_H
