/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_IO_H
#define INCLUDED_ASAP_IO_H

#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#include <memory>
#include <cerrno>

#include "asap/word_bank.h"

namespace asap {

namespace internal {

template<typename ContainerTy>
void getdir( const std::string & dirname, ContainerTy & files ) {
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dirname.c_str())) == NULL)
	fatale( "opendir", dirname );

    while ((dirp = readdir(dp)) != NULL) {
	const char * file = files.append( 0, dirname.c_str(), dirname.length() );
	file = files.append( file, "/", 1 );
	file = files.append( file, dirp->d_name, strlen(dirp->d_name) );

        struct stat buf;
        if( lstat( file, &buf ) < 0 )
	    fatale( "lstat", file );

        if (S_ISREG(buf.st_mode))
            files.index_only(file);
	else
	    files.erase(file);
    }

    closedir(dp);
}

}

template<typename WordListTy>
void get_directory_listing( const std::string & dirname, WordListTy & wl ) {
    // Note: use asap::word_list defined in asap/word_bank.h
    // Note: WordListTy::word_bank_type must be managed
    typedef WordListTy word_list_type;

    static_assert( word_list_type::word_bank_type::is_managed,
		   "Directory listing word_bank must be self-managed" );

    internal::getdir( dirname, wl );
}

}

#endif // INCLUDED_ASAP_IO_H
