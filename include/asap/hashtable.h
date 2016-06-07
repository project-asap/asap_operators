// -*- c++ -*-
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
 *
 * This is based on code from Pheonix++ Version 1.0, license below applies.
*/

/* Copyright (c) 2016, Queen's University Belfast.
 */
/* Based on:
* Copyright (c) 2007-2011, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#ifndef ASAP_HASHTABLE_H
#define ASAP_HASHTABLE_H

#include <unordered_map>
#include <vector>
#include <functional>
#include <memory>
#include <cassert>

namespace asap {

// storage for flexible cardinality keys
template<typename Key, typename T, class Hash=std::hash<Key>, 
	 class KeyEqual = std::equal_to<Key>,
	 class Allocator = std::allocator<std::pair<Key,T>>>
class hash_table
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
    typedef typename allocator_type::template rebind<bool>::other bool_allocator_type;
    typedef hash_table<Key, T, Hash, KeyEqual, Allocator> self_type;
private:
    std::vector<value_type, allocator_type> table;
    std::vector<bool, bool_allocator_type> occupied;
    hasher kh;
    key_equal keql;
    size_type msize;
    size_type load;
    size_type log_grow;
    size_type log_grow_by;
public:
    hash_table(size_type init_size = 256,
	       size_type log_grow_ = 1,
	       size_type log_grow_by_ = 1)
	: msize(0), load(0), log_grow(log_grow_), log_grow_by(log_grow_by_) {
	assert( (init_size & (init_size-size_type(1))) == 0 );
        rehash(init_size);
    }

    void set_growth( size_type log_grow_ = 1, size_type log_grow_by_ = 1 ) {
	log_grow = log_grow_;
	log_grow_by = log_grow_by_;
    }
    
    ~hash_table() { } // unimplemented, assuming no destructor for Key, T

    hasher hash_function() const { return kh; }
    key_equal key_eq() const { return keql; }

    void swap( self_type & h ) {
	table.swap( h.table );
	occupied.swap( h.occupied );
	std::swap( kh, h.kh );
	std::swap( msize, h.msize );
	std::swap( load, h.load );
    }

    void clear() {
	table.clear();
	occupied.clear();
	msize = 0;
	load = 0;
    }

    size_t size() const { return load; }
    
    void rehash(size_type newsize) {
	assert( (newsize & (newsize-size_type(1))) == 0 );

	// New storage
        decltype(table) newtable(newsize);
        decltype(occupied) newoccupied(newsize, false);

	// Move over contents
        for(size_type i = 0; i < msize; i++) {
            if(occupied[i]) {
                size_type index = kh(table[i].first) & (newsize-1);
                while(newoccupied[index])
                    index = (index+1) & (newsize-1);
		// std::swap( newtable[index], table[i] );
		newtable[index] = table[i];
                newoccupied[index] = true;
            }
        }
        newtable.swap(table);
        newoccupied.swap(occupied);
        msize = newsize;
    }

    size_t capacity() const { return msize; }

    mapped_type& operator[] (Key const& key) 
    {
        size_type index = kh(key) & (msize-1);
        while(occupied[index] && !keql(table[index].first, key)) {
            index = (index+1) & (msize-1);
        }

        if(occupied[index])
            return table[index].second;
        else {
            load++;
            if(load >= msize>>log_grow) {
                rehash(msize<<log_grow_by);
                index = kh(key) & (msize-1);
                while(occupied[index] && !keql(table[index].first, key)) {
                    index = (index+1) & (msize-1);
                }
            }
            table[index].first = key;
            occupied[index] = true;
            return table[index].second;
        }
    }

    class const_iterator : public std::iterator<
	std::input_iterator_tag,
	value_type>
	{
        hash_table const* a;
        size_type index;
    public:
        const_iterator(hash_table const& a, int index)
        {
            this->a = &a;
            this->index = index;
            
            while(this->index < this->a->msize && 
                !this->a->occupied[this->index]) {
                this->index++;
            }
	    // assert( this->index <= this->a->msize );
        }
        bool operator == (const_iterator const& other) const {
	    return a == other.a && index == other.index;
	}
        bool operator != (const_iterator const& other) const {
            return index != other.index;
        }
        const_iterator& operator++() {
            if(index < a->msize) {
                index++;
                while(index < a->msize && !a->occupied[index]) {
                    index++;
                }
		// assert( index <= a->msize );
            }
            return *this;
        }
        const_reference operator*() {
            return a->table[index];
        }
	value_type const* operator->() {
            return &a->table[index];
	}
    };

    class iterator : public std::iterator<
	std::forward_iterator_tag,
	value_type> {
        hash_table * a;
        size_type index;
    public:
        iterator(hash_table & a, int index)
        {
            this->a = &a;
            this->index = index;
            
            while(this->index < this->a->msize && 
                !this->a->occupied[this->index]) {
                this->index++;
            }
	    // assert( this->index <= this->a->msize );
        }

	operator const_iterator () const {
	    return const_iterator( *a, index );
	}

        bool operator != (iterator const& other) const {
            return index != other.index;
        }
        iterator& operator++() {
            if(index < a->msize) {
                index++;
                while(index < a->msize && !a->occupied[index]) {
                    index++;
                }
            }
	    // assert( index <= a->msize );
            return *this;
        }
        value_type & operator*() { // key in entry should be const
            return a->table[index];
        }
	value_type * operator->() { // key in entry should be const
            return &a->table[index];
	}
        value_type & operator*() const { // key in entry should be const
            return a->table[index];
        }
	value_type * operator->() const { // key in entry should be const
            return &a->table[index];
	}

	bool operator == ( const iterator & I ) const {
	    return &a == &I.a && index == I.index;
	}
    };


    std::pair<iterator,bool> insert( const value_type & kv ) {
	const key_type & key = kv.first;
        size_type index = kh(key) & (msize-1);
        while(occupied[index] && !keql(table[index].first, key)) {
            index = (index+1) & (msize-1);
        }

        if(occupied[index])
            return std::make_pair( iterator( *this, index ), false );
        else {
            load++;
            if(load >= msize>>log_grow) {
                rehash(msize<<log_grow_by);
                index = kh(key) & (msize-1);
                while(occupied[index] && !keql(table[index].first, key)) {
                    index = (index+1) & (msize-1);
                }
            }
            table[index] = kv;
            occupied[index] = true;
            return std::make_pair( iterator( *this, index ), true );
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
		rehash(msize<<1); // to incorporate n/2 possibly more than x2
	}
*/

	while( from != to ) {
	    insert( *from );
	    ++from;
	}
    }

    const_iterator find( const key_type &key ) const {
        size_type index = kh(key) & (msize-1);
        while(occupied[index] && !keql(table[index].first, key)) {
            index = (index+1) & (msize-1);
        }

        if(occupied[index])
            return const_iterator( *this, index );
        else
	    return cend();
    }

    iterator find( const key_type &key ) {
        size_type index = kh(key) & (msize-1);
        while(occupied[index] && !keql(table[index].first, key)) {
            index = (index+1) & (msize-1);
        }

        if(occupied[index])
            return iterator( *this, index );
        else
	    return end();
    }

    iterator begin() {
        return iterator(*this, 0);
    }

    iterator end() {
        return iterator(*this, msize); 
    }

    const_iterator begin() const {
        return const_iterator(*this, 0);
    }

    const_iterator end() const {
        return const_iterator(*this, msize); 
    }

    const_iterator cbegin() const {
        return const_iterator(*this, 0);
    }

    const_iterator cend() const {
        return const_iterator(*this, msize); 
    }
};

} // namespace asap

#endif // ASAP_HASHTABLE_H
