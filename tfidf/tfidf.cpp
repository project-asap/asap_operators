/* Copyright (c) 2007-2011, Stanford University
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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <vector>
#include <algorithm>
#include <iostream>
#include <memory>
#ifdef P2_UNORDERED_MAP
#include "p2_unordered_map.h"
#endif // P2_UNORDERED_MAP

#if SEQUENTIAL && PMC
#include <likwid.h>
#endif

#if !SEQUENTIAL
#include <cilk/cilk.h>
#include <cilk/reducer.h>
#include <cilk/reducer_opadd.h>
// #include "cilkpub/sort.h"
#else
#define cilk_sync
#define cilk_spawn
#define cilk_for for
#endif

#if TRACING
#include "tracing/events.h"
#include "tracing/events.cc"
#define TRACE(e)  event_tracer::get().record( event_tracer::e, 0, 0 )
#else
#define TRACE(e)  do { } while( 0 )
#endif

#include "stddefines.h"
#include <tuple>
#include <map>
#include <set>
#include <fstream>
#include <math.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

#define DEFAULT_DISP_NUM 10

// a passage from the text. The input data to the Map-Reduce
struct wc_string {
    char* data;
    uint64_t len;
};

// a single null-terminated word
struct wc_word {
    char* data;
    
    wc_word(char * d = 0) : data( d ) { }
    
    // necessary functions to use this as a key
    bool operator<(wc_word const& other) const {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(wc_word const& other) const {
        return strcmp(data, other.data) == 0;
    }
};


// a hash for the word
struct wc_word_hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(wc_word const& key) const
    {
        char* h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};

struct wc_word_pred
{
    bool operator() ( const wc_word & a, const wc_word & b ) const {
	return !strcmp( a.data, b.data );
    }
};

struct wc_sort_pred_by_first
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return a.first < b.first;
    }
};

struct wc_sort_pred
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return a.second > b.second 
	  || (a.second == b.second && strcmp( a.first.data, b.first.data ) > 0);
    }
};

struct wc_merge_pred
{
    bool operator() ( const std::pair<wc_word, size_t> & a,
		      const std::pair<wc_word, size_t> & b ) const {
	return strcmp( a.first.data, b.first.data ) < 0;
    }
};

// Use inheritance for convenience, should use encapsulation.
static size_t nfiles = 0;
class fileVector : public std::vector<size_t> {
public:
    fileVector() : std::vector<size_t>( nfiles, 0 ) { }
};

#ifdef P2_UNORDERED_MAP
typedef std::p2_unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(STD_UNORDERED_MAP)
#include <unordered_map>
typedef std::unordered_map<wc_word, fileVector, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(PHOENIX_MAP)
#include "container.h"
typedef hash_table<wc_word, fileVector, wc_word_hash> wc_unordered_map;
#else
#include "container.h"
typedef hash_table_stored_hash<wc_word, fileVector, wc_word_hash> wc_unordered_map;

#endif // P2_UNORDERED_MAP

#if !SEQUENTIAL

static double merge_time = 0;

void merge_two_dicts( wc_unordered_map & m1, wc_unordered_map & m2 ) {
    struct timespec begin, end;
    get_time (begin);
    // std::cerr << "merge 2...\n";
    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
	std::vector<size_t> & counts1 =  m1[I->first];
	const std::vector<size_t> & counts2 =  I->second;
	// Vectorized
	size_t * v1 = &counts1.front();
	const size_t * v2 = &counts2.front();
	v1[0:nfiles] += v2[0:nfiles];
    }
    m2.clear();
    get_time (end);
    merge_time += time_diff(end, begin);
    // std::cerr << "merge 2 done...\n";
}

class dictionary_reducer {
    struct Monoid : cilk::monoid_base<wc_unordered_map> {
	static void reduce( wc_unordered_map * left,
			    wc_unordered_map * right ) {
	    TRACE( e_sreduce );
	    merge_two_dicts( *left, *right );
	    TRACE( e_ereduce );
	}
	static void identity( wc_unordered_map * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) wc_unordered_map(1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    dictionary_reducer() : imp_() { }

    void swap( wc_unordered_map & c ) {
	imp_.view().swap( c );
    }

    fileVector & operator []( wc_word idx ) {
	return imp_.view()[idx];
    }

    size_t empty() const {
	return imp_.view().size() == 0;
    }

    typename wc_unordered_map::iterator begin() { return imp_.view().begin(); }
    // typename wc_unordered_map::const_iterator cbegin() { return imp_.view().cbegin(); }
    typename wc_unordered_map::iterator end() { return imp_.view().end(); }
    // typename wc_unordered_map::const_iterator cend() { return imp_.view().cend(); }

/*
#if defined(STD_UNORDERED_MAP)
    wc_unordered_map::hasher hash_function() {
        return imp_.view().hash_function();
    }
#endif
*/

};
#else
typedef wc_unordered_map dictionary_reducer;
#endif

void wc( char * data, uint64_t data_size, uint64_t chunk_size, dictionary_reducer & dict, unsigned int file) {

    uint64_t splitter_pos = 0;
    while( 1 ) {
	TRACE( e_ssplit );

        /* End of data reached, return FALSE. */
        if ((uint64_t)splitter_pos >= data_size) {
	    TRACE( e_esplit );
            break;
	}

        /* Determine the nominal end point. */
        uint64_t end = std::min(splitter_pos + chunk_size, data_size);
        /* Move end point to next word break */
        while(end < data_size && 
            data[end] != ' ' && data[end] != '\t' &&
            data[end] != '\r' && data[end] != '\n')
            end++;
	if( end < data_size )
	    data[end] = '\0';

        /* Set the start of the next data. */
	wc_string s;
        s.data = data + splitter_pos;
        s.len = end - splitter_pos;
        
        splitter_pos = end;
	TRACE( e_esplit );

        /* Continue with map since the s data is valid. */
	cilk_spawn [&] (wc_string s) {
	    TRACE( e_smap );

	    // TODO: is it better for locatiy to move toupper() into the inner loop?
	    for (uint64_t i = 0; i < s.len; i++)
		s.data[i] = toupper(s.data[i]);

	    uint64_t i = 0;
            uint64_t start;
            wc_word word = { s.data+start };
	    while(i < s.len) {            
		while(i < s.len && (s.data[i] < 'A' || s.data[i] > 'Z'))
		    i++;
		start = i;
		/* and can we also vectorize toupper?
		while( i < s.len ) {
		   s.data[i] = toupper(s.data[i]);
		   if(((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
		      i++;
		   else
		      break;
		}
		*/
		// while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
		while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') ))
		    i++;
		if(i > start)
		{
		    s.data[i] = 0;
		    word = { s.data+start };
		    dict[word][file]++;
		}
	    }
	    TRACE( e_emap );
        }( s );
    }
    cilk_sync;

    TRACE( e_synced );
    // std::cout << "final hash table size=" << final_dict.bucket_count() << std::endl;
}

#define NO_MMAP

// vim: ts=8 sw=4 sts=4 smarttab smartindent

using namespace std;

int getdir (std::string dir, std::vector<std::string> &files)
{
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
	std::cerr << "Error(" << errno << ") opening " << dir << std::endl;
        return errno;
    }
 
    while ((dirp = readdir(dp)) != NULL) {
        if (dirp->d_type == DT_REG) {
            string dirFull=(dir);
            std::string relFilePath=dirFull + "/" + dirp->d_name;
            files.push_back(relFilePath);
        }
    }
    closedir(dp);
    return 0;
}


#include <unordered_map>
size_t is_nonzero( size_t s ) {
    return s != 0;
}

int main(int argc, char *argv[]) 
{
    char * fname=0;
    struct timespec begin, end, all_begin;
    std::vector<std::string> files;

    dictionary_reducer dict;

    get_time (begin);
    all_begin = begin;

#if TRACING
    event_tracer::init();
#endif

    bool checkResults=false;
    int c;

    while ( (c = getopt (argc, argv, "cd:")) != -1 )
	switch (c) {
	case 'c':
	    checkResults = 1;
	    break;
	case 'd':
	    fname = optarg;
	    break;
	case '?':
	    if (optopt == 'd')
		fprintf (stderr, "Option -%c requires a directory argument.\n", optopt);
	    else if (isprint (optopt))
		fprintf (stderr, "Unknown option `-%c'.\n", optopt);
	    else
		fprintf (stderr,
			 "Unknown option character `\\x%x'.\n",
			 optopt);
	    return 1;
	default:
	    abort ();
	}

    // Make sure a filename is specified
    if( !fname ) {
        printf("USAGE: %s -d <directory name> [-c]\n", argv[0]);
        exit(1);
    }

    getdir(fname,files);
    nfiles = files.size();
    char * fdata[files.size()];
    struct stat finfo[files.size()];
    int fd[files.size()];
    get_time (end);

#ifdef TIMING
    print_time("initialize", begin, end);
#endif

    cilk_for (unsigned int i = 0;i < files.size();i++) {

        struct timespec beginI, endI, beginWC, endWC;
        get_time(beginI);

        // dict.setReserve(files.size());

        // Read in the file
        fd[i] = open(files[i].c_str(), O_RDONLY);
       
        // Get the file info (for file length)
        fstat(fd[i], &finfo[i]);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
	// Memory map the file
        fdata[i] = (char*)mmap(0, finfo[i].st_size + 1, 
			       PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd[i], 0);
#else
        // Memory map the file
        fdata[i] = (char*)mmap(0, finfo[i].st_size + 1, 
			       PROT_READ, MAP_PRIVATE, fd[i], 0);
#endif
#else
        uint64_t r = 0;

        fdata[i] = (char *)malloc (finfo[i].st_size);
        while(r < (uint64_t)finfo[i].st_size)
            r += pread (fd[i], fdata[i] + r, finfo[i].st_size, r);
#endif    
    
        get_time (endI);

#ifdef TIMING
        print_time("thread file-read", beginI, endI);
#endif

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
#else
#endif
#else
        close(fd[i]);
#endif

        get_time (beginWC);

        wc(fdata[i], finfo[i].st_size, 1024*1024, dict, i);

        get_time (endWC);
#ifdef TIMING
        print_time("thread WC ", beginWC, endWC);
#endif

        TRACE( e_smerge );

    }

    // File initialisation
    string strFilename(fname);
    string arffTextFilename(strFilename + ".arff");
    ofstream resFileTextArff;
    resFileTextArff.open(arffTextFilename, ios::out | ios::trunc );
    
    get_time (begin);

#define SS(str) (str), (sizeof((str))-1)/sizeof((str)[0])
#define XS(str) (str).c_str(), (str).size()
#define ST(i)   ((const char *)&(i)), sizeof((i))

    // Char initialisations
    char nline='\n';
    char space=' ';
    char tab='\t';
    char comma=',';
    char colon=':';
    char lbrace='{';
    char rbrace='}';
    char what;

    //
    // print out arff text format
    //
    string loopStart = "@attribute ";
    string typeStr = "numeric";
    string dataStr = "@data";
    string headerTextArff("@relation tfidf");
    string classTextArff("@attribute @@class@@ {text}");

    resFileTextArff << headerTextArff << "\n" << classTextArff << "\n";;
    resFileTextArff.flush();

    // std::unordered_map retains iteration order of elements unless if
    // the hashtable is resized, e.g., due to insertion/deletion or
    // explicitly requested. Since these won't happen now, we do not need to
    // store the additional idMap.
    // unordered_map<uint64_t, uint64_t> idMap;
    // hash_table<uint64_t, uint64_t, wc_word_hash> idMap;
    // long i=1;
/*
#ifdef STD_UNORDERED_MAP
    wc_unordered_map::hasher fn = dict.hash_function();
#endif
*/
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

        resFileTextArff << "\t";

        const string & str = I->first.data;
/*
#ifndef STD_UNORDERED_MAP
        uint64_t id = I.getIndex();
#else
        uint64_t id = fn(I->first);
#endif
*/

        resFileTextArff << loopStart << str << " " << typeStr << "\n";
        // idMap[id]=i;

        // i++;
    }

    resFileTextArff << "\n\n" << dataStr << "\n\n";
    resFileTextArff.flush();

    //
    // print the data
    //
    for (unsigned int i = 0;i < files.size();i++) {

        string & keyStr = files[i];

	if( dict.empty() )
	    continue;

        resFileTextArff << "\t{";

        // iterate over each word to collect total counts of each word in all files (reducedCount)
        // OR the number of files that contain the work (existsInFilesCount)
	long id=1;
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I, ++id ) {

                size_t tf = I->second[i];
	        if (!tf)
		    continue;

                // todo: workout how the best way to calculate and store each 
                // word total once for all files
#if 0
	        cilk::reducer< cilk::op_add<size_t> > existsInFilesCount(0);
	        cilk_for (int j = 0; j < I->second.size(); ++j) {
		    // *reducedCount += I->second[j];  // Use this if we want to count every occurence
		    if (I->second[j] > 0) *existsInFilesCount += 1;
	        }
	        size_t fcount = existsInFilesCount.get_value();
#else
                // Think this counts total occurences in all files, rather than number of other files it exists in ???
		// No: is_nonzero counts 1 per file with non-zero count
                // Makes it different to sparks tfidf implementation following ?
	        const size_t * v = &I->second.front();
	        // size_t len = I->second.size();
	        size_t fcount = __sec_reduce_add( is_nonzero(v[0:nfiles]) );
#endif

                //     Calculate tfidf  ---   Alternative versions of tfidf:
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) sumOccurencesOfWord + 1.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) numOfOtherDocsWithWord + 2.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) reducedCount.get_value() + 1.0)); 
                // Sparks version;
                double tfidf = (double) tf * log10(((double) files.size() + 1.0) / ((double) fcount + 1.0)); 

/*
#ifndef STD_UNORDERED_MAP
                uint64_t id = I.getIndex();
#else
                uint64_t id = fn(I->first);
#endif
                resFileTextArff << idMap[id] << ' ' << tfidf;
*/

                // Note:
                // If Weka etc doesn't care if there is an extra unnecessary comma at end
                // of a each record then we'd rather avoid the branch test here, so leave comma in
                resFileTextArff << id << ' ' << tfidf << ',';
        }
        resFileTextArff << "}\n";
    }
    resFileTextArff.close();

    get_time (end);
#ifdef TIMING
    print_time("output", begin, end);
    print_time("merge", merge_time);
    print_time("complete time", all_begin, end);
#endif

    get_time (end);

#ifdef TIMING
    // print_time("finalize", begin, end);
#endif

#if TRACING
    event_tracer::destroy();
#endif

    for(int i = 0; i < files.size() ; ++i) {
#ifndef NO_MMAP
        munmap(fdata[i], finfo[i].st_size + 1);
#else
        free (fdata[i]);
#endif
    }

    return 0;
}




// Unused function, code for future reference
void recordOfCodeForALLOutputFormats() {

    //
    // This func is Non-compilable, for future record for cleaner output formats code
    //
#if 0
    string strFilename(fname);
    string txtFilename(strFilename + ".txt");
    string binFilename(strFilename + ".bin");
    string arffTextFilename(strFilename + ".arff");
    string arffBinFilename(strFilename + ".arff.bin");
    ofstream resFile (txtFilename, ios::out | ios::trunc | ios::binary);
    ofstream resFileArff ( arffBinFilename, ios::out | ios::trunc | ios::binary);
    auto name_max = pathconf(fname, _PC_NAME_MAX);
    ofstream resFileTextArff;
    resFileTextArff.open(arffTextFilename, ios::out | ios::trunc );
    
    get_time (begin);

    string headerText("Document Vectors (sequencefile @ hdfs):");
    string headerTextArff("@relation tfidf");
    string classText("Key class: class org.apache.hadoop.io.Text Value Class: class org.apache.mahout.math.VectorWritable");
    string classTextArff("@attribute @@class@@ {text}");

#define SS(str) (str), (sizeof((str))-1)/sizeof((str)[0])
#define XS(str) (str).c_str(), (str).size()
#define ST(i)   ((const char *)&(i)), sizeof((i))

    char nline='\n';
    char space=' ';
    char tab='\t';
    char comma=',';
    char colon=':';
    char lbrace='{';
    char rbrace='}';
    char what;

    // print arff
    string loopStart = "@attribute ";
    string typeStr = "numeric";
    string dataStr = "@data";
    resFileArff.write (headerTextArff.c_str(), headerTextArff.size());
    resFileArff.write ((char *)&nline,1);
    resFileArff.write ((char *)&tab,1);
    resFileArff.write (classTextArff.c_str(), classTextArff.size());
    resFileArff.write ((char *)&nline,1);

    resFileTextArff << headerTextArff << "\n" << classTextArff << "\n";;
    resFileTextArff.flush();
    // uint64_t indices[dict.size()];
    unordered_map<uint64_t, uint64_t> idMap;
    // hash_table<uint64_t, uint64_t, wc_word_hash> idMap;
    int i=1;
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        resFileArff.write((char *)&tab, 1);

        resFileTextArff << "\t";

        uint64_t id = I.getIndex();
        string str = I->first.data;
        resFileArff.write((char *) loopStart.c_str(), loopStart.size());
        resFileArff.write((char *) str.c_str(), str.size());
        resFileArff.write((char *) &space, sizeof(char));
        resFileArff.write((char *) typeStr.c_str(), typeStr.size());
        resFileArff.write((char *) &nline, sizeof(char));

        resFileTextArff << loopStart << str << " " << typeStr << "\n";

        idMap[id]=i;
        i++;
        // cout << "\t" << loopStart << id << "\n";
    }
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) dataStr.c_str(), dataStr.size());
    resFileArff.write((char *) &nline, sizeof(char));
    resFileArff.write((char *) &nline, sizeof(char));

    resFileTextArff << "\n\n" << dataStr << "\n\n";

    // printing mahoot Dictionary
    cout << headerText << nline;
    cout << "\t" << classText << nline;

    // printing mahoot Dictionary
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write ((char *)&nline,1);
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write ((char *)&nline,1);
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        uint64_t id = I.getIndex();
        const string & str = I->first.data;
	resFile.write( SS( "\tKey: " ) )
	    .write( XS( str ) )
	    .write( SS( " Value: " ) )
	    .write( ST( id ) )
	    .write( SS( "\n" ) );
    }

    // printing mahoot output header
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write((char *) &nline, sizeof(char));
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write((char *) &nline, sizeof(char));
    // cout << headerText << nline << "\t" << classText << nline;
    
    resFileTextArff.flush();

    for (unsigned int i = 0;i < files.size();i++) {
        // printing mahoot loop start text including filename twice !
        string & keyStr = files[i];

	if( dict.empty() ) {
	    // string loopStart = "Key: " + keyStr + ": " + "Value: "
	    // + keyStr + ":";
	    // resFile.write ((char *)&tab, 1);
	    // resFile.write (loopStart.c_str(), loopStart.size());
	    // resFile.write ((char *)&rbrace, 1);
	    // resFile.write ((char *)&nline, 1);
	    resFile.write( SS( "\tKey: " ) )
		.write( XS( keyStr ) )
		.write( SS( ": Value: " ) )
		.write( XS( keyStr ) )
		.write( SS( ":{}\n" ) );
	    continue;
	}

	resFile.write( SS( "\tKey: " ) )
	    .write( XS( keyStr ) )
	    .write( SS( ": Value: " ) )
	    .write( XS( keyStr ) )
	    .write( SS( ":{" ) );

        resFileTextArff << "\t{";

        // iterate over each word to collect total counts of each word in all files (reducedCount)
        // OR the number of files that contain the work (existsInFilesCount)
        for( auto I=dict.begin(), E=dict.end(); I != E; ) {

                size_t tf = I->second[i];
	        if (!tf) {
		    ++I;
		    continue;
	        }

                // todo: workout how the best way to calculate and store each 
                // word total once for all files
#if 0
	        cilk::reducer< cilk::op_add<size_t> > existsInFilesCount(0);
	        cilk_for (int j = 0; j < I->second.size(); ++j) {
		    // *reducedCount += I->second[j];  // Use this if we want to count every occurence
		    if (I->second[j] > 0) *existsInFilesCount += 1;
	        }
	        size_t fcount = existsInFilesCount.get_value();
#else
	        const size_t * v = &I->second.front();
	        size_t len = I->second.size();
	        size_t fcount = __sec_reduce_add( is_nonzero(v[0:len]) );
#endif

                //     Calculate tfidf  ---   Alternative versions of tfidf:
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) sumOccurencesOfWord + 1.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) numOfOtherDocsWithWord + 2.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) reducedCount.get_value() + 1.0)); 
                // Sparks version;
                double tfidf = (double) tf * log10(((double) files.size() + 1.0) / ((double) fcount + 1.0)); 

                uint64_t id = I.getIndex();

	        resFile.write( ST(id) )
		    .write( SS(":") )
		    .write( ST( tfidf ) );

                resFileTextArff << idMap[id] << space << tfidf;

	        // if( I != E )
                    resFile.write ((char *) &comma, sizeof(char));

                    resFileArff.write ((char *) &comma, sizeof(char));


                ++I;

                // Note:
                // If Weka etc doesn't care if there is an extra unnecessary comma at end
                // of a each record then we'd rather avoid the branch test here, so leave it in
                    cout << ",";
                    resFileArff.write ((char *) &comma, sizeof(char));
                    resFileTextArff << comma;

        }
        // cout << "\n";
	// What is this? Reverse one character?
	// In order to avoid this, need to count number of words in each
	// file during wc(). Not sure if that pays off...
        long pos = resFile.tellp();
        resFile.seekp (pos-1);

        resFile.write ((char *)&rbrace, 1);
        resFile.write ((char *)&nline, 1);

        pos = resFileArff.tellp();
        resFileArff.seekp (pos-1);
	resFileArff.write( SS( "}\n" ) );

	resFile.write( SS( "}\n" ) );

         resFileTextArff << "}\n";

        cout << "}\n";
    }
    resFileArff.close();
    resFileTextArff.close();
    resFile.close();

    get_time (end);
#ifdef TIMING
    print_time("output", begin, end);
    print_time("all", all_begin, end);
#endif

    // Check on binary file:
    // -c at the command line with try to read results back in from binary file and display 
    // but note there will be odd chars before unsigned int64's as we do simple read of unsigned 
    // int 64 for 'id' Examining the binary file itself shows the binary file has the correct values for 'id'
    if (checkResults) {
        char colon, comma, cbrace, nline, tab;
        uint64_t id;
        double tfidf;
        char checkHeaderText[headerText.size()];
        char checkClassText[classText.size()];

        ifstream inResFile;
        inResFile.open("testRes.txt", ios::binary);

	std::cerr << "\nREADING IN --------------------------------" << "\n" ;
        // reading mahoot Dictionary
        inResFile.read( (char*)&checkHeaderText, headerText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        inResFile.read( (char*)&tab, 1);
        inResFile.read( (char*)&checkClassText, classText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        // checkClassText[classText.size()+1]=0;
	std::cerr << checkHeaderText << nline << tab << checkClassText << nline;
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
            inResFile.read( (char*)&tab, 1);

	    string str =  I->first.data;
            string iterStartCheck = "Key: " + str + " Value: ";
            char preText[iterStartCheck.size() +1 ];
            inResFile.read((char *)&preText, iterStartCheck.size());
            inResFile.read( (char*)&id, sizeof(uint64_t));
            inResFile.read( (char*)&nline, sizeof(char));
	    std::cerr << tab << preText << id << nline;
            // 	std::cerr << tab << preText << id;
        }

	// reading mahoot TFIDF mappings per word per file
	inResFile.read( (char*)checkHeaderText, headerText.size());
	inResFile.read( (char*)&nline, sizeof(char));
	inResFile.read( (char*)&tab, 1);
	inResFile.read( (char*)checkClassText, classText.size());
	inResFile.read( (char*)&nline, sizeof(char));
	std::cerr << checkHeaderText << nline << tab << checkClassText << nline;

	// read for each files
	for (unsigned int i = 0;i < files.size();i++) {
	    string & keyStr = files[i];
	    string loopStart = "Key: " + keyStr + ": " + "Value: " + keyStr + ":" + "{";
	    char preText[loopStart.size() +1];
	    inResFile.read( (char*)&tab, 1);
	    inResFile.read( (char*)&preText, loopStart.size());
	    std::cerr << tab << preText;
                
	    // iterate over each word to collect total counts of each word in all files
	    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

		size_t tf = I->second[i];
		if (!tf) continue;
		inResFile.read( (char*)&id, sizeof(uint64_t));
		inResFile.read( (char*)&colon, sizeof(char));
		inResFile.read( (char*)&tfidf, sizeof(double));
		inResFile.read( (char*)&comma, sizeof(char));
		std::cerr << id << colon << tfidf << comma ;
	    }
	    // inResFile.read((char*)&cbrace, 1);  // comma will contain cbrace after last iteration
	    cbrace=comma;
	    inResFile.read((char*)&nline, 1);
	    std::cerr << nline;
	}
    }
#endif

}
