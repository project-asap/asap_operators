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


#ifndef INCLUDED_ASAP_TRAITS_H
#define INCLUDED_ASAP_TRAITS_H

#include <type_traits>

namespace asap {

template<template<typename...> class Template, typename T>
struct is_specialization_of : std::false_type { };

template<template<typename...> class Template, typename... Params>
struct is_specialization_of<Template, Template<Params...>> : std::true_type { };

namespace internal {

// Auxiliary types
typedef char small_type;
struct large_type {
    small_type dummy[2];
};

// This code checks whether the type T is a class with a member
// public: void asap_decl();
template<class T>
struct class_has_asap_decl {
    template<void (T::*)()> struct tester;

    template<typename U>
    static small_type has_matching_member( tester<&U::asap_decl>*);
    template<typename U>
    static large_type has_matching_member(...);

    static const bool value =
	sizeof(has_matching_member<T>(0))==sizeof(small_type);
};
    
// Default case is no match (struct has bool value member = false).
template<typename T,bool is_class_type=std::is_class<T>::value>
struct is_asap_class : std::false_type { };

// Case for structs where type matching with class_has_asap_decl succeeds
template<typename T>
struct is_asap_class<T,true>
    : std::integral_constant<bool, class_has_asap_decl<T>::value> { };

// Helper -- only call it when T is a class type
template<typename T,typename Tag,bool hod=is_asap_class<T>::value>
struct is_asap_class_with_tag : std::false_type { };

template<typename T, typename Tag>
struct is_asap_class_with_tag<T,Tag,true>
    : std::is_base_of<Tag, typename T::_asap_tag> { };

template<typename T, typename... Tags>
struct is_asap_class_with_all_tags;

template<typename T, typename Tag>
struct is_asap_class_with_all_tags<T,Tag> : is_asap_class_with_tag<T,Tag> { };

template<typename T, typename Tag, typename... Tags>
struct is_asap_class_with_all_tags<T,Tag,Tags...>
    : std::integral_constant<bool, is_asap_class_with_tag<T,Tag>::value
			     && is_asap_class_with_all_tags<T,Tags...>::value> { };

template<typename T, typename... Tags>
struct is_asap_class_with_any_tags;

template<typename T, typename Tag>
struct is_asap_class_with_any_tags<T,Tag> : is_asap_class_with_tag<T,Tag> { };

template<typename T, typename Tag, typename... Tags>
struct is_asap_class_with_any_tags<T,Tag,Tags...>
    : std::integral_constant<bool, is_asap_class_with_tag<T,Tag>::value
			     || is_asap_class_with_any_tags<T,Tags...>::value> { };

template<typename T>
struct strip {
    typedef typename std::remove_cv<
	typename std::remove_reference<T>::type>::type type;
};

}

// These types represent properties of data structures. They are used
// as if they are binary flags.
struct tag_vector { };
struct tag_dense { };
struct tag_sparse { };
struct tag_sqnorm_cache { };
struct tag_add_counter { };

// Check if type T represents a vector
template<typename T>
struct is_vector : internal::is_asap_class_with_tag<
    typename internal::strip<T>::type, tag_vector> { };

// Check if type T represents a dense vector
template<typename T>
struct is_dense_vector
    : internal::is_asap_class_with_all_tags<
    typename internal::strip<T>::type, tag_dense, tag_vector> { };

// Check if type T represents a sparse vector
template<typename T>
struct is_sparse_vector
    : internal::is_asap_class_with_all_tags<
    typename internal::strip<T>::type, tag_sparse, tag_vector> { };

// Check if type T represents a vector extended with an additive counter
template<typename T>
struct is_vector_with_add_counter
    : internal::is_asap_class_with_tag<
    typename internal::strip<T>::type, tag_add_counter> { };

// Check if type T represents a vector extended with a cache holding the
// square of its (Euclidean) norm.
template<typename T>
struct is_vector_with_sqnorm_cache
    : internal::is_asap_class_with_tag<
    typename internal::strip<T>::type, tag_sqnorm_cache> { };

// Check if type T has any attributes
template<typename T>
struct has_attributes
    : internal::is_asap_class_with_any_tags<
    typename internal::strip<T>::type, tag_sqnorm_cache, tag_add_counter> { };

namespace internal {

// This code checks whether the type T is a class with a member
// public: void reserve(size_t);
template<class T>
struct class_is_sizeable {
    template<void (T::*)()> struct tester;

    template<typename U>
    static small_type has_matching_member( tester<&U::reserve>*);
    template<typename U>
    static large_type has_matching_member(...);

    static const bool value =
	sizeof(has_matching_member<T>(0))==sizeof(small_type);
};
    
// Default case is no match (struct has bool value member = false).
template<typename T,bool is_class_type=std::is_class<T>::value>
struct is_sizeable : std::false_type { };

// Case for structs where type matching with class_has_asap_decl succeeds
template<typename T>
struct is_sizeable<T,true>
    : std::integral_constant<bool, class_is_sizeable<T>::value> { };

} // namespace internal

// Check if type T has a reserve() method
template<typename T>
struct is_sizeable : internal::is_sizeable<T> { };

// Call the reserve(size_t) method on a class, if it has one
template<typename T, bool = internal::is_sizeable<T>::value>
struct sizeable_methods {
    static void resize( T &, size_t )		{ }
    static void reserve( T &, size_t )		{ }
};

template<typename T>
struct sizeable_methods<T,true> {
    static void resize( T & obj , size_t sz )	{ obj.resize( sz ); }
    static void reserve( T & obj , size_t sz )	{ obj.reserve( sz ); }
};

} // namespace asap

#endif // INCLUDED_ASAP_TRAITS_H
