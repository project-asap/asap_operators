// -*- c++ -*-
/* Copyright (c) 2016, Queen's University Belfast.
 */

#ifndef ASAP_HASHINDEX_H
#define ASAP_HASHINDEX_H

#include <unordered_map>
#include <vector>
#include <deque>
#include <functional>
#include <memory>
#include <cassert>

namespace asap {

template<typename Key, typename T, class Hash=std::hash<Key>, 
	 class KeyEqual = std::equal_to<Key>,
	 class Allocator = std::allocator<std::pair<Key,T>>>
class hash_index
{
public:
    typedef Key				key_type;
    typedef T				mapped_type;
    typedef std::pair<Key, T>		value_type;
    typedef std::size_t 		size_type;
    typedef std::ptrdiff_t 		difference_type;
    typedef Hash 			hasher;
    typedef KeyEqual 			key_equal;
    typedef Allocator 			allocator_type;
    typedef value_type& 		reference;
    typedef const value_type& 		const_reference;
    // typedef typename std::allocator_traits<Allocator>::pointer pointer;
    // typedef typename std::allocator_traits<Allocator>::const_pointer const_pointer;
    // typedef iterator;
    // typedef const_iterator;

private:
    typedef hash_index<Key, T, Hash, KeyEqual, Allocator> self_type;
private:
    typedef std::pair<size_t,value_type>	serial_type;
    typedef size_t				index_type;

// Alternative to constexpr function (Intel compiler fails)
#define unused_key (~size_t(0))

    typedef typename
    allocator_type::template rebind<serial_type>::other serial_allocator_type;
    typedef typename
    allocator_type::template rebind<index_type>::other index_allocator_type;

    typedef typename std::deque<serial_type, serial_allocator_type>::iterator
    storage_iterator;
    typedef typename std::deque<serial_type, serial_allocator_type>::const_iterator
    const_storage_iterator;

    std::deque<serial_type, serial_allocator_type>	storage;
    std::vector<index_type, index_allocator_type>	table;
    hasher kh;
    key_equal keql;
    size_type msize;
    size_type load;
public:
    hash_index(size_type init_size = 256) : msize(0), load(0) {
	assert( (init_size & (init_size-size_type(1))) == 0 );
        rehash(init_size);
    }
    
    ~hash_index() { } // unimplemented, assuming no destructor for Key, T

    hasher hash_function() const { return kh; }
    key_equal key_eq() const { return keql; }

    void swap( self_type & h ) {
	table.swap( h.table );
	storage.swap( h.storage );
	std::swap( kh, h.kh );
	std::swap( msize, h.msize );
	std::swap( load, h.load );
    }

    void clear() {
	table.clear();
	storage.clear();
	msize = 0;
	load = 0;
    }

    size_t size() const { return load; }
    
    void rehash(size_type newsize) {
	assert( (newsize & (newsize-size_type(1))) == 0 );

	// New storage
        decltype(table) newtable( newsize, unused_key );

	// Move over contents
	for( storage_iterator
		 I=storage.begin(), E=storage.end(); I != E; ++I ) {
	    const Key & key = I->second.first;
	    size_type index = kh( key ) & (newsize-1);
	    while( newtable[index] != unused_key )
		index = (index+1) & (newsize-1);
	    I->first = index;
	    newtable[index] = I - storage.begin();
        }
        newtable.swap(table);
        msize = newsize;
    }

    size_t capacity() const { return msize; }

    mapped_type& operator[] (Key const& key) 
    {
        size_type index = kh(key) & (msize-1);
        while( table[index] != unused_key
	      && !keql( storage[table[index]].second.first, key ) ) {
            index = (index+1) & (msize-1);
        }

        if( table[index] != unused_key ) {
            return storage[table[index]].second.second;
	} else {
            load++;
            if(load >= msize>>1) {
                rehash(msize<<1);
                index = kh(key) & (msize-1);
                while( table[index] != unused_key
		       && !keql( storage[table[index]].second.first, key ) ) {
                    index = (index+1) & (msize-1);
                }
            }
	    table[index] = storage.size();
	    storage.push_back(
		std::make_pair( index, std::make_pair( key, mapped_type() ) ) );
            return storage.back().second.second;
        }
    }

    class const_iterator
	: public std::iterator<std::random_access_iterator_tag, value_type>
    {
	const_storage_iterator I;
	    
    public:
        const_iterator( const_storage_iterator _I ) : I( _I ) { }

        bool operator == (const_iterator const& other) const {
	    return I == other.I;
	}
        bool operator != (const_iterator const& other) const {
            return I != other.I;
        }
        const_iterator& operator++() {
	    ++I;
            return *this;
        }
        const_reference operator*() { return I->second; }
	value_type const* operator->() { return &I->second; }
    };

    class iterator
	: public std::iterator<std::random_access_iterator_tag, value_type> {
	storage_iterator I;
    public:
        iterator( storage_iterator _I ) : I( _I ) { }

	operator const_iterator () const {
	    return const_iterator( (const_storage_iterator)I );
	}

        bool operator != ( iterator const& other ) const {
	    return I != other.I;
	}
	bool operator == ( iterator const& other ) const {
	    return I == other.I;
	}
        iterator& operator++() { ++I; return *this; }
        value_type & operator*() { return I->second; }
	value_type * operator->() { return &I->second; }
        value_type & operator*() const { return I->second; }
	value_type * operator->() const { return &I->second; }
    };


    std::pair<iterator,bool> insert( const value_type & kv ) {
	const key_type & key = kv.first;
        size_type index = kh(key) & (msize-1);
        while( table[index] != unused_key
	       && !keql(storage[table[index]].second.first, key ) ) {
            index = (index+1) & (msize-1);
        }

        if( table[index] != unused_key )
            return std::make_pair( iterator( storage.begin()+table[index] ),
				   false );
        else {
            load++;
            if(load >= msize>>1) {
                rehash(msize<<1);
                index = kh(key) & (msize-1);
                while( table[index] != unused_key
		       && !keql( storage[table[index]].second.first, key ) ) {
                    index = (index+1) & (msize-1);
                }
            }
            table[index] = storage.size();
	    storage.push_back( std::make_pair( index, kv ) );
            return std::make_pair( iterator( storage.begin()+table[index] ),
				   true );
        }
    }

    template<typename Iterator>
    void insert( Iterator from, Iterator to ) {
/*
	// If it is cheap to calculate the number of elements that will be
	// inserted, do so and estimate that half of them are new.
	if( std::is_same<typename std::iterator_traits<Iterator>::iterator_tag,
	    std::random_access_iterator_tag>::value ) {
	    size_t n = std::distance( from, to ); // O(1)
	    if( load + n/2 >= msize>>1 )
		rehash(msize<<1);
	}
*/

	while( from != to ) {
	    insert( *from );
	    ++from;
	}
    }

    const_iterator find( const key_type &key ) const {
        size_type index = kh(key) & (msize-1);
        while( table[index] != unused_key
	       && !keql( storage[table[index]].second.first, key ) ) {
            index = (index+1) & (msize-1);
        }

        if( table[index] != unused_key )
            return const_iterator( storage.cbegin() + table[index] );
        else
	    return cend();
    }

    iterator find( const key_type &key ) {
        size_type index = kh(key) & (msize-1);
        while( table[index] != unused_key
	       && !keql( storage[table[index]].second.first, key ) ) {
            index = (index+1) & (msize-1);
        }

        if( table[index] != unused_key )
            return iterator( storage.begin() + table[index] );
        else
	    return end();
    }

    iterator begin() { return iterator( storage.begin() ); }
    iterator end() { return iterator( storage.end() ); }

    const_iterator begin() const { return const_iterator( storage.cbegin() ); }
    const_iterator end() const { return const_iterator( storage.cend() ); }

    const_iterator cbegin() const { return const_iterator( storage.cbegin() ); }
    const_iterator cend() const { return const_iterator( storage.cend() ); }
};

#undef unused_key

} // namespace asap

#endif // ASAP_HASHINDEX_H
