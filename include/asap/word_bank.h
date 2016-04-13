/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_WORD_BANK_H
#define INCLUDED_ASAP_WORD_BANK_H

#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <list>
#include <utility>
#include <memory>

namespace asap {

class word_bank_base {
    // list of all chunks of text
    std::list<std::shared_ptr<char>> m_store;

public:
    word_bank_base() { }
    word_bank_base( const word_bank_base & wb ) : m_store( wb.m_store ) { }
    word_bank_base( word_bank_base && wb )
	: m_store( std::move(wb.m_store) ) { }
    word_bank_base & operator = ( const word_bank_base & wb ) {
	m_store = wb.m_store;
	return *this;
    }
    word_bank_base & operator = ( word_bank_base && wb ) {
	m_store = std::move( wb.m_store );
	return *this;
    }
    ~word_bank_base() { clear(); }

    void swap( word_bank_base & wb ) {
	m_store.swap( wb.m_store );
    }

    void clear() {
	// Delete chunks of data held in word_bank
	m_store.clear();
    }

    void copy( const word_bank_base & rhs ) {
	// This creates shared_ptr<>'s pointing to the same blocks
	// as the rhs container
	m_store.insert( m_store.end(),
			rhs.m_store.cbegin(), rhs.m_store.cend() );
    }

    void copy( word_bank_base && rhs ) {
	// This creates shared_ptr<>'s pointing to the same blocks
	// as the rhs container
	m_store.insert( m_store.end(),
			std::make_move_iterator(rhs.m_store.begin()),
			std::make_move_iterator(rhs.m_store.end()) );
    }

    void reduce( word_bank_base & rhs ) {
	// Move all chunks over from rhs to lhs
	// Using a list will be more efficient for this operation.
	m_store.splice( m_store.end(), rhs.m_store );
    }

    void enregister( std::shared_ptr<char> && buf ) {
	m_store.push_back( std::move(buf) );
    }

protected:
    char * push_chunk( size_t size ) {
	return push_chunk( new char[size] );
    }
    char * push_chunk( char * chunk ) {
	std::shared_ptr<char> sp( chunk, std::default_delete<char[]>() );
	m_store.push_back( sp );

	return chunk;
    }
};

class word_bank_managed : public word_bank_base {
public:
    static const bool is_managed = true;
    
private:
    const size_t	  m_chunk;
    size_t		  m_avail;
    char		* m_next;

public:
    word_bank_managed( size_t chunk = 4096 )
	: m_chunk( chunk ), m_avail( 0 ), m_next( nullptr ) { }
    word_bank_managed( const word_bank_managed & wb )
	: word_bank_base( wb ), m_chunk( wb.m_chunk ), m_avail( 0 ),
	  m_next( nullptr ) { }
    word_bank_managed( word_bank_managed && wb )
	: word_bank_base( std::move((word_bank_base&&)wb) ),
	  m_chunk( std::move(wb.m_chunk) ), m_avail( std::move(wb.m_avail) ),
	  m_next( std::move(wb.m_next) ) {
	wb.m_avail = 0;
	wb.m_next = nullptr;
    }
    word_bank_managed & operator = ( const word_bank_managed & wb ) {
	word_bank_base::operator = ( wb );
	return *this;
    }
    word_bank_managed & operator = ( word_bank_managed && wb ) {
	word_bank_base::operator = ( std::move(wb) );
	return *this;
    }
    ~word_bank_managed() { clear(); }

    void clear() {
	word_bank_base::clear();
	m_avail = 0;
	m_next = nullptr;
    }

    // Push len characters starting at p and '\0' delimit it
    const char * store( const char * p, size_t len ) {
	// Doesn't deal with fragmentation
	if( m_avail < len+1 )
	    push_chunk( std::max( len+1, m_chunk ) );
	char * where = m_next;
	strncpy( where, p, len );
	where[len] = '\0';
	m_avail -= len + 1;
	m_next += len + 1;
	return where;
    }

    // Build up a string
    const char * append( const char * start, const char * str, size_t len ) {
	// Erase prior string terminator
	if( start ) {
	    --m_next;
	    ++m_avail;
	}

	// Make sure there is enough space
	// Doesn't deal with fragmentation
	if( m_avail < len+1 ) {
	    char * was_next = m_next;
	    push_chunk( std::max( start ? was_next-start+len+1 : len+1,
				  m_chunk ) );
	    // copy what we had so we can extend it
	    if( start ) {
		start = store( start, was_next - start );
		--m_next;
		++m_avail;
	    }
	}

	// Store the string but return original pointer, unless if it is the
	// start of the word
	const char * where = store( str, len );
	return start ? start : where;
    }

    // Note: this is dangerous to use...
    void erase( const char * w ) {
	char * ww = const_cast<char *>( w );
	m_avail += m_next - ww;
	m_next = ww;
    }

private:
    void push_chunk( size_t len ) {
	m_next = word_bank_base::push_chunk( len );
	m_avail = len;
    }
};

// A word bank with all words taken from a pre-defined block of text.
// The block of text is modified with '\0' to indicate end of string.
class word_bank_pre_alloc : public word_bank_base {
public:
    static const bool is_managed = false;
    
public:
    word_bank_pre_alloc( char * store = 0 ) {
	if( store )
	    push_chunk( store );
    }
    word_bank_pre_alloc( const word_bank_pre_alloc & wb )
	: word_bank_base( wb ) { }
    word_bank_pre_alloc( word_bank_pre_alloc && wb )
	: word_bank_base( std::move( (word_bank_base&&)wb ) ) { }
    word_bank_pre_alloc & operator = ( const word_bank_pre_alloc & wb ) {
	word_bank_base::operator = ( wb );
	return *this;
    }
    word_bank_pre_alloc & operator = ( word_bank_pre_alloc && ) = delete;
    ~word_bank_pre_alloc() { clear(); }

    void clear() {
	// Parent class has ownership over m_store chunk
	word_bank_base::clear();
    }

    // Push len characters starting at p and '\0' delimit it
    const char * store( char * p, size_t len ) {
	p[len] = '\0';
	return p;
    }

    // Build up a string
    const char * append( const char * start, const char * str, size_t len ) {
	assert( 0 );
	return 0;
    }
    void erase( const char * w ) { }
};

template<typename IndexTy, typename WordBankTy>
class word_map;

template<typename IndexTy, typename WordBankTy>
class word_container {
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;

protected:
    index_type		m_words;
    word_bank_type	m_storage;

public:
    word_container() { }
    // Try to avoid use of the copy constructor
    word_container( const word_container & wc )
	: m_words( wc.m_words ), m_storage( wc.m_storage ) { }
    word_container( word_container && wc )
	: m_words( std::move(wc.m_words) ),
	  m_storage( std::move(wc.m_storage) ) { }

    // template<typename... Args>
    // word_container( Args... args ) : m_storage( args... ) { }

    const word_bank_type & storage() const { return m_storage; }

    size_t size() const		{ return m_words.size(); }
    bool empty() const		{ return m_words.empty(); }

    void clear()		{ m_words.clear(); m_storage.clear(); } 
    void swap( word_container & wb ) {
	m_words.swap( wb.m_words );
	m_storage.swap( wb.m_storage );
    }

    void enregister( std::shared_ptr<char> && buf ) {
	m_storage.enregister( std::move(buf) );
    }

    // Build up a string
    const char * append( const char * start, const char * str, size_t len ) {
	return m_storage.append( start, str, len );
    }
    void erase( const char * w ) {
	m_storage.erase( w );
    }
};

// TODO: make parameter for word_list/merge/reduce operator
template<typename ValueTyL, typename ValueTyR>
struct pair_cmp {
    bool operator () ( const ValueTyL & v1, const ValueTyR & v2 ) {
	return strcmp( v1.first, v2.first ) < 0;
    }
};

template<typename ValueTyL, typename ValueTyR>
struct pair_add_reducer {
    void operator () ( ValueTyL & v1, const ValueTyR & v2 ) {
	v1.second += v2.second;
    }
};

template<typename ValueTyL, typename ValueTyR>
struct pair_nonzero_reducer {
    void operator () ( ValueTyL & v1, const ValueTyR & v2 ) {
	v1.second += ( v2.second > 0 );
    }
};

// IndexTy is sequential container such std::vector, std::deque, std::list
// Assumed is that IndexTy::value_type is const char *
template<typename IndexTy, typename WordBankTy>
class word_list : public word_container<IndexTy,WordBankTy> {
    typedef word_container<IndexTy,WordBankTy> base_type;
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;

    typedef typename index_type::value_type	value_type;

    typedef typename index_type::const_iterator	const_iterator;
    typedef typename index_type::iterator	iterator;

    static const bool is_managed = word_bank_type::is_managed;

private:
    template<typename OtherIndexTy>
    struct is_compatible
	: std::integral_constant<
	bool,
	std::is_same<typename OtherIndexTy::key_type,
		     typename value_type::first_type>::value
	&&
	std::is_same<typename OtherIndexTy::mapped_type,
		     typename value_type::second_type>::value> {
    };

public:
    word_list() { }
    template<typename... Args>
    word_list( Args... args ) : base_type( args... ) { }

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

    // Retrieve n-th word.
    // Depends on pure list (like in directory list - single word)
    // or used as associative container (key-value pairs).
    // Need to differentiate word_list from word_value_list...
    auto operator[] ( size_t n ) const -> decltype(this->m_words[n]) {
	return this->m_words[n];
    }

    iterator begin() { return this->m_words.begin(); }
    iterator end() { return this->m_words.end(); }

    const_iterator cbegin() const { return this->m_words.cbegin(); }
    const_iterator cend() const { return this->m_words.cend(); }

    const_iterator find( const char * w ) const {
	value_type val
	    = std::make_pair( w, typename value_type::second_type() );
	pair_cmp<value_type,value_type> cmp;
	for( const_iterator I=cbegin(), E=cend(); I != E; ++I )
	    if( !cmp( *I, val ) && !cmp( val, *I ) )
		return I;
	return cend();
    }

    const_iterator binary_search( const char * w ) const {
	value_type val
	    = std::make_pair( w, typename value_type::second_type() );
	// val.first = w; // only if std::pair
	std::pair<const_iterator,bool> ret
	    = binary_search( cbegin(), cend(), this->size(), val,
			     pair_cmp<value_type,value_type>() );
	return ret.second ? ret.first : cend();
    }

#if 0
    // TODO: this is imprecise:
    // + Not clear if range [I,E) is all of wb, or only part of it
    // + As such, copying over all of wb may be too much
    // + Ideally want to translate all strings into existing word bank
    // At the moment, this is used only with *this initially empty, so this
    // is ok.
    template<typename InputIterator>
    void insert( InputIterator I, InputIterator E, const word_bank_base & wb ) {
	std::for_each( I, E, [&]( typename index_type::value_type & val ) { this->m_words.push_back( val ); } );
	// this->m_words.insert( I, E );
	this->m_storage.copy( wb );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( const word_map<OtherIndexTy,OtherWordBankTy> & wc ) {
	this->m_words.insert( this->m_words.end(), wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( word_map<OtherIndexTy,OtherWordBankTy> && wc ) {
	this->m_words.insert( this->m_words.end(),
			      std::make_move_iterator(wc.begin()),
			      std::make_move_iterator(wc.end()) );
	this->m_storage.copy( std::move(wc.storage()) );
	wc.clear();
    }
#endif

    // Add in all contents from rhs into lhs (*this) and clear rhs
    // Assumes both *this and rhs are sorted by key (whatever sorting function
    // is used ...)
    void reduce( word_list & rhs ) {
	// TODO: consider parallel merge (std::experimental::parallel_merge)
	// TODO: Better with move iterators?
	core_reduce( rhs.begin(), rhs.end(), rhs.size(), rhs.storage(),
		     pair_cmp<value_type,value_type>(),
		     pair_add_reducer<value_type,value_type>() );
	rhs.clear();
    }

private:
    template<class InputIt, class Compare, class Reduce>
    void core_reduce(InputIt first2, InputIt last2, size_t size2,
		     const word_bank_base & storage,
		     Compare cmp, Reduce reduce) {
	index_type joint;
	joint.reserve( this->size() + size2 ); // worst case
	core_merge( this->begin(), this->end(), 
		    first2, last2, std::back_inserter(joint),
		    pair_cmp<value_type,value_type>(),
		    pair_add_reducer<value_type,value_type>() );
	this->m_words.swap( joint );
	this->m_storage.copy( std::move(storage) );
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

// IndexTy is sequential container such std::vector, std::deque, std::list
// Assumed is that IndexTy::value_type is const char *
template<typename IndexTy, typename WordBankTy>
class kv_list : public word_container<IndexTy,WordBankTy> {
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
    kv_list() { }
    template<typename... Args>
    kv_list( Args... args ) : base_type( args... ) { }

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

    const_iterator find( const char * w ) const {
	value_type val
	    = std::make_pair( w, typename value_type::second_type() );
	pair_cmp<value_type,value_type> cmp;
	for( const_iterator I=cbegin(), E=cend(); I != E; ++I )
	    if( !cmp( *I, val ) && !cmp( val, *I ) )
		return I;
	return cend();
    }

    const_iterator binary_search( const char * w ) const {
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
    insert( const word_map<OtherIndexTy,OtherWordBankTy> & wc ) {
	assert( this->m_words.empty() );
	this->m_words.insert( this->m_words.end(), wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( word_map<OtherIndexTy,OtherWordBankTy> && wc ) {
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
    void count_presence( const word_map<OtherIndexTy,OtherWordBankTy> & rhs ) {
	typedef typename word_map<OtherIndexTy,OtherWordBankTy>::value_type
	    other_value_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     pair_nonzero_reducer<value_type,other_value_type>() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const kv_list<OtherIndexTy,OtherWordBankTy> & rhs ) {
	typedef typename kv_list<OtherIndexTy,OtherWordBankTy>::value_type
	    ::second_type other_value_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.size(), rhs.storage(),
		     pair_cmp<value_type,value_type>(),
		     pair_nonzero_reducer<value_type,other_value_type>() );
    }


    // Add in all contents from rhs into lhs (*this) and clear rhs
    // Assumes both *this and rhs are sorted by key (whatever sorting function
    // is used ...)
    void reduce( kv_list & rhs ) {
	// TODO: consider parallel merge (std::experimental::parallel_merge)
	// TODO: Better with move iterators?
	core_reduce( rhs.begin(), rhs.end(), rhs.size(), rhs.storage(),
		     pair_cmp<value_type,value_type>(),
		     pair_add_reducer<value_type,value_type>() );
	rhs.clear();
    }

private:
    template<class InputIt, class Compare, class Reduce>
    void core_reduce(InputIt first2, InputIt last2, size_t size2,
		     const word_bank_base & storage,
		     Compare cmp, Reduce reduce) {
	index_type joint;
	joint.reserve( this->size() + size2 ); // worst case
	core_merge( this->begin(), this->end(), 
		    first2, last2, std::back_inserter(joint),
		    pair_cmp<value_type,value_type>(),
		    pair_add_reducer<value_type,value_type>() );
	this->m_words.swap( joint );
	this->m_storage.copy( std::move(storage) );
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


template<typename ValueTyL, typename ValueTyR>
struct mapped_add_reducer {
    void operator () ( ValueTyL & v1, const ValueTyR & v2 ) {
	v1 += v2;
    }
};

template<typename ValueTyL, typename ValueTyR>
struct mapped_nonzero_reducer {
    void operator () ( ValueTyL & v1, const ValueTyR & v2 ) {
	v1 += ( v2 > 0 );
    }
};

// IndexTy is a map such as std::map, std::unordered_map where the mapped_type
// is an integral type, or any type supporting:
//  - default constructor
//  - operator ++ ()
//  - operator += ()
template<typename IndexTy, typename WordBankTy>
class word_map : public word_container<IndexTy,WordBankTy> {
    typedef word_container<IndexTy,WordBankTy> base_type;
public:
    typedef IndexTy	  index_type;
    typedef WordBankTy	  word_bank_type;

    typedef typename index_type::key_type	key_type;
    typedef typename index_type::mapped_type	mapped_type;
    typedef typename index_type::value_type	value_type;
    typedef typename index_type::const_iterator	const_iterator;
    typedef typename index_type::iterator	iterator;

    static const bool is_managed = word_bank_type::is_managed;

    template<typename OtherIndexTy, typename OtherWordBankTy>
    friend class word_map;

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
    word_map() { }
    word_map( const word_map & wm )
	: base_type( *static_cast<const base_type *>( &wm ) ) { }
    word_map( word_map && wm ) : base_type( std::move(wm) ) { }
    // template<typename... Args>
    // word_map( Args... args ) : base_type( args... ) { }

    // Memorize the word, but do not store it in the word list
    const char * memorize( char * p, size_t len ) {
	return this->m_storage.store( p, len );
    }

    // Memorize the word and store it in the word list as well
    const char * index( char * p, size_t len ) {
	const char * w = this->m_storage.store( p, len );
	++ (this->m_words[w]);
	return w;
    }

    // Build up a string
    std::pair<const char *, char *>
    build( const std::pair<const char *, char *> & where, const char * str ) {
	return this->m_storage.build( str );
    }

    // Retrieve the n-th word. Linear time complexity
    // Note: used in output.
    const char * operator[] ( size_t n ) const {
	return std::next( cbegin(), n )->first;
    }

    iterator begin() { return this->m_words.begin(); }
    iterator end() { return this->m_words.end(); }

    const_iterator cbegin() const { return this->m_words.cbegin(); }
    const_iterator cend() const { return this->m_words.cend(); }

    iterator find( const char * w ) {
	return this->m_words.find( w );
    }
    const_iterator find( const char * w ) const {
	return this->m_words.find( w );
    }
    // For reference and ease of substituting types in templates
    iterator binary_search( const char * w ) {
	return find( w );
    }
    const_iterator binary_search( const char * w ) const {
	return find( w );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const word_map<OtherIndexTy,OtherWordBankTy> & rhs ) {
	typedef typename word_map<OtherIndexTy,OtherWordBankTy>::mapped_type
	    other_mapped_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     mapped_nonzero_reducer<mapped_type,other_mapped_type>() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const kv_list<OtherIndexTy,OtherWordBankTy> & rhs ) {
	typedef typename word_list<OtherIndexTy,OtherWordBankTy>::value_type
	    ::second_type other_mapped_type;
	core_reduce( rhs.cbegin(), rhs.cend(), rhs.storage(),
		     mapped_nonzero_reducer<mapped_type,other_mapped_type>() );
    }

    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( const word_map<OtherIndexTy,OtherWordBankTy> & wc ) {
	this->m_words.insert( this->m_words.end(), wc.cbegin(), wc.cend() );
	this->m_storage.copy( wc.storage() );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    typename
    std::enable_if<is_compatible<OtherIndexTy>::value>::type
    insert( word_map<OtherIndexTy,OtherWordBankTy> && wc ) {
	this->m_words.insert( this->m_words.end(),
			      std::make_move_iterator(wc.begin()),
			      std::make_move_iterator(wc.end()) );
	this->m_storage.copy( std::move(wc.storage()) );
	wc.clear();
    }


    // Add in all contents from rhs into lhs and clear rhs
    void reduce( word_map & rhs ) {
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
	    const char * w = I->first;
	    if( word_bank_type::is_managed ) { // record new copy of word
		size_t len = strlen( I->first );
		w = memorize( (char*)I->first, len );
	    }

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
		// const char * w = I->first;
		// if( word_bank_type::is_managed ) // record new copy of word
		    // ret.first->first = memorize( (char*)w, len ); // strlen( w ) );

	    } else {
		// Not inserted - key already occurred
		reducer_fn( ret.first->second, I->second );
		if( word_bank_type::is_managed ) // erase redundant copy of word
		    this->erase( w );

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
		const char * w = I->first;
		if( word_bank_type::is_managed ) // record new copy of word
		    w = memorize( (char*)w, strlen( w ) );
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
		const char * w = I->first;
		if( word_bank_type::is_managed ) // record new copy of word
		    w = memorize( (char*)w, strlen( w ) );
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
	if( !word_bank_type::is_managed && any_word_new )
	    this->m_storage.copy( storage );
    }
};

template<typename WordContainerTy>
class word_container_reducer {
    typedef WordContainerTy type;
    struct Monoid : cilk::monoid_base<type> {
	static void reduce( type * left, type * right ) {
	    left->reduce( *right );
	}
	static void identity( type * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) type();
	    // TODO: p->private_reserve(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    word_container_reducer() : imp_() { }

    void swap( type & c ) {
	imp_.view().swap( c );
    }

/*
    mapped_type & operator [] ( const key_type & key ) {
	return imp_.view()[key];
    }
    const mapped_type & operator [] const ( const key_type & key ) {
	return imp_.view()[key];
    }
*/
    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const word_map<OtherIndexTy,OtherWordBankTy> & rhs ) {
	imp_.view().count_presence( rhs );
    }
    template<typename OtherIndexTy, typename OtherWordBankTy>
    void count_presence( const kv_list<OtherIndexTy,OtherWordBankTy> & rhs ) {
	imp_.view().count_presence( rhs );
    }

    type & get_value() { return imp_.view(); }
};


template<typename WordContainerTy>
class word_container_file_builder {
public:
    typedef WordContainerTy word_container_type;

private:
    word_container_type	& 	 	  m_container; // word container, with storage
    std::shared_ptr<char>  		  m_buf;	// file contents
    size_t				  m_size;	// file size

public:
    word_container_file_builder( const std::string & filename,
				 word_container_type & container )
	: m_container( container ) {
	open_file( filename );
    }
    ~word_container_file_builder() { }

    void swap( word_container_file_builder & w ) {
	std::swap( m_container, w.m_container );
	std::swap( m_buf, w.m_buf );
	std::swap( m_size, w.m_size );
    }

    size_t size() const { return m_container.size(); }
    char * get_buffer() const { return m_buf.get(); }
    char * get_buffer_end() const { return m_buf.get()+m_size; }
    const word_container_type & get_word_list() const { return m_container; }
    word_container_type & get_word_list() { return m_container; }

    const char * memorize( char * p, size_t len ) {
	return m_container.record( p, len );
    }
    const char * index( char * p, size_t len ) {
	return m_container.push_back( p, len );
    }

private:
    // Read the file into memory.
    // TODO: replace mmap( PROT_READ | PROT_WRITE, MAP_PRIVATE );
    void open_file( const std::string & filename ) {
	struct stat finfo;
	int fd;

	const char * fname = filename.c_str();

	if( (fd = open( fname, O_RDONLY )) < 0 )
	    fatale( "open", fname );
	if( fstat( fd, &finfo ) < 0 )
	    fatale( "fstat", fname );

	uint64_t r = 0;
	char * buf = new char[finfo.st_size+1];
	while( r < (uint64_t)finfo.st_size )
	    r += pread( fd, buf + r, finfo.st_size, r );
	buf[finfo.st_size] = '\0';

	close( fd );

	m_size = finfo.st_size;

	std::shared_ptr<char> sp( buf, std::default_delete<char[]>() );
	m_buf = sp;
	if( !word_container_type::is_managed )
	    m_container.enregister( std::move(sp) );
    }
};

} // namespace asap

#endif // INCLUDED_ASAP_MEMORY_H
