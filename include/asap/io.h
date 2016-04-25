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
size_t getdir( const std::string & dirname, ContainerTy & files,
	       bool recursive = true ) {
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dirname.c_str())) == NULL)
	fatale( "opendir", dirname );

    size_t total_size = 0;

    while ((dirp = readdir(dp)) != NULL) {
	const char * file = files.append( 0, dirname.c_str(), dirname.length() );
	file = files.append( file, "/", 1 );
	file = files.append( file, dirp->d_name, strlen(dirp->d_name) );

        struct stat buf;
        if( lstat( file, &buf ) < 0 )
	    fatale( "lstat", file );

        if (S_ISREG(buf.st_mode)) {
            files.index_only(file);
	    total_size += buf.st_size;
	} else if( S_ISLNK(buf.st_mode) ) {
	    char lnk[256];
	    strncpy( lnk, file, sizeof(lnk) );
	    size_t nbp = strlen( file )-1;
	    while( lnk[nbp] != '/' && nbp > 0 )
		--nbp;
	    if( lnk[nbp] == '/' )
		++nbp; // forward from '/'
	    ssize_t nb = readlink(file, &lnk[nbp], sizeof(lnk)-nbp);
	    if( nb < 0 )
		fatale( "readlink", file );

	    lnk[nbp+nb] = '\0';

	    if( lstat( lnk, &buf ) < 0 )
		fatale( "lstat symlink", lnk );
	    if( S_ISREG(buf.st_mode) ) {
		files.erase(file);
		files.index(lnk, nbp+nb);
		total_size += buf.st_size;
	    }
	} else if( S_ISDIR(buf.st_mode) && recursive ) {
	    if( strcmp( dirp->d_name, "." ) && strcmp( dirp->d_name, ".." ) ) {
		std::string subdir = file;
		files.erase(file);
		total_size += getdir( subdir, files, recursive );
	    } else
		files.erase(file);
	} else
	    files.erase(file);
    }

    closedir(dp);

    return total_size;
}

}

template<typename WordListTy>
size_t get_directory_listing( const std::string & dirname, WordListTy & wl ) {
    // Note: use asap::word_list defined in asap/word_bank.h
    // Note: WordListTy::word_bank_type must be managed
    typedef WordListTy word_list_type;

    static_assert( word_list_type::word_bank_type::is_managed,
		   "Directory listing word_bank must be self-managed" );

    return internal::getdir( dirname, wl );
}

}

#endif // INCLUDED_ASAP_IO_H
