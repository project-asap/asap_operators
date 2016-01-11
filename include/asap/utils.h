/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_UTILS_H
#define INCLUDED_ASAP_UTILS_H

#include <cerrno>
#include <cstring>
#include <iostream>

namespace asap {

#define fatale(fn,...)	asap::fatale_fn((fn),__FILE__,__LINE__,__VA_ARGS__)
#define fatal(...)	asap::fatal_fn(__FILE__,__LINE__,__VA_ARGS__)

void print_args( std::ostream & os ) {
}

template<typename T, typename... Tn>
void print_args( std::ostream & os, T arg, Tn... args) {
    os << arg;
    print_args( os, args... );
}

template<typename... Tn>
void __attribute__((noreturn, noinline))
fatale_fn( const char * fn, const char * srcfile, unsigned lineno,
	   Tn... args ) {
    const char * es = strerror( errno );
    std::cerr << srcfile << ':' << lineno << ": " << fn
	      << ": " << es << ": ";
    print_args( std::cerr, args... );
    std::cerr << std::endl;
    exit( 1 );
}

template<typename... Tn>
void __attribute__((noreturn,noinline))
fatal_fn( const char * srcfile, unsigned lineno, Tn... args ) {
    std::cerr << srcfile << ':' << lineno << ": ";
    print_args( std::cerr, args... );
    std::cerr << std::endl;
    exit( 1 );
}

}

#endif // INCLUDED_ASAP_UTILS_H
