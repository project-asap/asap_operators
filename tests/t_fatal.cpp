#include "asap/utils.h"

int main( int argc, char *argv[] ) {
    if( argc == 1 )
	fatal( "argc is 1" );
    if( argc == 2 )
	fatale( "main", "argc is 1" );

    return 0;
}
