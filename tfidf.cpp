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
//#include <unordered_map>
#include <iostream>
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
#include "cilkpub/sort.h"
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

#ifdef P2_UNORDERED_MAP
typedef std::p2_unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(STD_UNORDERED_MAP)
typedef std::unordered_map<wc_word, size_t, wc_word_hash, wc_word_pred> wc_unordered_map;
#elif defined(PHOENIX_MAP)
#include "container.h"
typedef hash_table<wc_word, size_t, wc_word_hash> wc_unordered_map;
#else
#include "container.h"
// typedef hash_table_stored_hash<wc_word, size_t, wc_word_hash> wc_unordered_map;
typedef std::vector<size_t> fileVector;
typedef hash_table_stored_hash<wc_word, std::vector<size_t>, wc_word_hash> wc_unordered_map;

 
         // typedef std::pair<size_t, size_t> fileWordIndexType;
         // fileWordIndexType thisIdf(i,j);
         // typedef std::map<fileWordIndexType, double> idfFileWordMap;
 
// typedef std::map<std::pair<size_t, size_t>, double> idfFileWordMap;
typedef std::pair<size_t, size_t> fileWordIndexType;
typedef std::map<fileWordIndexType, double> idfFileWordMap;

#endif // P2_UNORDERED_MAP

#if !SEQUENTIAL

void merge_two_dicts( wc_unordered_map & m1, wc_unordered_map & m2 ) {

    for( auto I=m2.cbegin(), E=m2.cend(); I != E; ++I ) {
	std::vector<size_t> counts =  I->second;
        for( auto J=counts.begin(), JE=counts.end(); J != JE; ++J ) {
	    m1[I->first][std::distance(counts.begin(), J)] += *J;
        }
    }
    m2.clear();
}

#if MASTER
wc_unordered_map master_map;
pthread_mutex_t master_mutex;
#endif
class dictionary_reducer {
    struct Monoid : cilk::monoid_base<wc_unordered_map> {
	static void reduce( wc_unordered_map * left,
			    wc_unordered_map * right ) {
	    TRACE( e_sreduce );
#if MASTER
	    if( pthread_mutex_trylock( &master_mutex ) == 0 ) {
		// Lock successfully acquired.
		merge_two_dicts( master_map, *right );
		pthread_mutex_unlock( &master_mutex );
	    } else {
		merge_two_dicts( *left, *right );
	    }
#else
	    merge_two_dicts( *left, *right );
#endif
	    TRACE( e_ereduce );
	}
	static void identity( wc_unordered_map * p ) {
	    // Initialize to useful default size depending on chunk size
	    new (p) wc_unordered_map( 1<<16);
	}

    };

private:
    cilk::reducer<Monoid> imp_;

public:
    dictionary_reducer() : imp_() { }

    void swap( wc_unordered_map & c ) {
	imp_.view().swap( c );
    }

    // fileVector & operator []() {
	// return imp_.view()[idx];
    // }

    fileVector & operator []( wc_word idx ) {
	return imp_.view()[idx];
    }

#if 0
    fileVector & operator ()( wc_word idx , uint64_t fileReserve) {
        return imp_.view()(idx, fileReserve);
    }
#endif

    size_t empty() const {
	return imp_.view().size() == 0;
    }

    typename wc_unordered_map::iterator begin() { return imp_.view().begin(); }
    // typename wc_unordered_map::const_iterator cbegin() { return imp_.view().cbegin(); }
    typename wc_unordered_map::iterator end() { return imp_.view().end(); }
    // typename wc_unordered_map::const_iterator cend() { return imp_.view().cend(); }

    void setReserve(size_t size) { imp_.view().setReserve(size); }
};
#else
typedef wc_unordered_map dictionary_reducer;
#endif

#if 0
#if TLSREDUCE || ASAP
#if 1
void merge_dicts( wc_unordered_map ** dicts, size_t length ) {
    TRACE( e_sreduce );
    if( length > 2 ) {
	cilk_spawn merge_dicts( dicts, length/2 );
	merge_dicts( &dicts[length/2], length-length/2 );
	cilk_sync;
	merge_two_dicts( *dicts[0], *dicts[length/2] );
    } else if( length > 1 ) {
	merge_two_dicts( *dicts[0], *dicts[1] );
    }
    TRACE( e_ereduce );
}
#else
// Need to do fine-grain striping across dictionaries, creating tasks
// that consist of reducing the n-th slice of each of the dictionaries.
// Only sensible if combining this with serialization to key-value pairs,
// or if we provide a data structure that is a set of (slices of) hash tables
// *and* we distribute keys to identical slices for each thread's hash table.
void merge_dicts( wc_unordered_map ** dicts, size_t length ) {
    size_t nslices = 16; // so many slices
    wc_unordered_map dict[nslices];
    cilk_for( size_t s=0; s < nslices; ++s ) {
	for( size_t i=0; i < length; ++i ) {
	    wc_unordered_map & m1 = dict[s];
	    wc_unordered_map & m2 = *dicts[i];
	    for( auto I=m2.slice_begin(s,nslices), E=m2.slice_end(s,nslices);
		 I != E; ++I )
		m1[I->first] += I->second;
	}
    }
}
#endif if 1
#endif TLS or ASAP
#endif // if 0

void wc( char * data, uint64_t data_size, uint64_t chunk_size, wc_unordered_map & final_dict, unsigned int file,  uint64_t fileReserve ) {
    final_dict.rehash( 256 );
#if TLSREDUCE
    static __thread wc_unordered_map * tls_dict = 0;
    std::vector<wc_unordered_map *> all_dicts;
#else
    // dictionary_reducer dict;
    wc_unordered_map  dict;

    // previous code: size_t & s = dict[w];
#if 0
khere
    std::vector<size_t> & v = dict[w];
    size_t & si = v[i];
    size_t & si = dict[w][i];
    dict[w].begin();
#endif

#endif
    uint64_t splitter_pos = 0;
    while( 1 ) {
	TRACE( e_ssplit );

        dict.setReserve(fileReserve);

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
	data[end] = '\0';

        /* Set the start of the next data. */
	wc_string s;
        s.data = data + splitter_pos;
        s.len = end - splitter_pos;
        
        splitter_pos = end;
	TRACE( e_esplit );

	// ++parts;

        /* Continue with map since the s data is valid. */
	// cilk_spawn [&] (wc_string s) 
	    TRACE( e_smap );
#if TLSREDUCE
	    if( !tls_dict ) {
		tls_dict = new wc_unordered_map;
		all_dicts.push_back( tls_dict );
	    }
	    wc_unordered_map & dict = *tls_dict;
#endif
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
		while(i < s.len && ((s.data[i] >= 'A' && s.data[i] <= 'Z') || s.data[i] == '\''))
		    i++;
		if(i > start)
		{
		    s.data[i] = 0;
		    word = { s.data+start };
		    // dict(word,fileReserve)[file]++;
		    dict[word][file]++;
                    // std::vector<size_t> & v = dict[word];
                    // size_t & si = v[file];
                    // si++;
                    // std::cout << s.data+start << "\n";
		}
	    }
	    TRACE( e_emap );
#if 0
    std::cout << "File " << file << "\n";
    for( auto I=dict.begin(), E=dict.cend(); I != E; ++I ) {
        std::cout << "Word: " << I->first.data << "\n";
        for( auto J=I->second.begin(), JE=I->second.cend(); J != JE; ++J ) {
            // for( auto J=I->second.cbegin(), JE=I->second.cend(); J != JE; ++J ) 
                std::cout << "       second: " << *J << "\n";
         }
    }
#endif
    }
    // cilk_sync;
    // typename wc_unordered_map::const_iterator begin() { return imp_.view().begin(); }
#if 0
    std::cout << "_File " << file << "\n";
    for( auto I=dict.begin(), E=dict.cend(); I != E; ++I ) {
        std::cout << "Word: " << I->first.data << "\n";
        for( auto J=I->second.begin(), JE=I->second.cend(); J != JE; ++J ) {
                std::cout << "       second: " << *J << "\n";
         }
    }
#endif
#if TLSREDUCE
    // for( auto I=all_dicts.begin(), E=all_dicts.end(); I != E; ++I ) {
    merge_dicts( &all_dicts[0], all_dicts.size() );
    wc_unordered_map & dict = *all_dicts[0];
#endif
#if MASTER
    if( !dict.empty() ) {
    }
    master_map.swap( final_dict );
#else
    dict.swap( final_dict );
#endif
    TRACE( e_synced );
    // std::cout << "final hash table size=" << final_dict.bucket_count() << std::endl;
}

#define NO_MMAP

// vim: ts=8 sw=4 sts=4 smarttab smartindent


/* Program to find the frequency of words in a text file */
#include <tuple>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <algorithm>
#include <fstream>
#include "vector"
#include <math.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

using namespace std;
int getdir (std::string dir, std::vector<std::string> &files)
{
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
        cout << "Error(" << errno << ") opening " << dir << endl;
        return errno;
    }
 
    while ((dirp = readdir(dp)) != NULL) {
        if (dirp->d_type == DT_REG) {
            std::string relFilePath=std::string(dir + "/") + dirp->d_name;
            files.push_back(relFilePath);
        }
    }
    closedir(dp);
    return 0;
}



int main(int argc, char *argv[]) 
{
    int fd;
    char * fdata;
    unsigned int disp_num;
    struct stat finfo;
    char * fname, * disp_num_str;
    struct timespec begin, end, ser_begin;
    vector<string> files = vector<string>();
    // typedef std::vector<std::pair<wc_word, size_t> > sorted_result_type;
    // // typedef std::vector< sorted_result_type > dir_sorted_result_type;
    uint64_t dictionaryIndex=0;

    // wc_unordered_map dict;
    dictionary_reducer dict;

    // std::vector<std::vector<std::pair<wc_word, size_t>>> sorted_result;

    get_time (begin);

#if SEQUENTIAL && PMC
    LIKWID_MARKER_INIT;
#endif // SEQUENTIAL && PMC

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

    // fname = argv[1];
    // disp_num_str = argv[2];
    
    // Make sure a filename is specified
    if( argc < 2 ) {
        printf("USAGE: %s <directory name> [Top # of results to display]\n", argv[0]);
        exit(1);
    }

    getdir(fname,files);
    dict.setReserve(files.size());

    // sorted_result_type sorted_result[files.size()];

    wc_unordered_map * asap_dict;
    // /* static */wc_unordered_map * asap_dict[files.size()];
    // std::vector<wc_unordered_map *> all_dicts;

    // printf("Wordcount: Running...\n");

    cilk_for (unsigned int i = 0;i < files.size();i++) {

        // Read in the file
        fd = open(files[i].c_str(), O_RDONLY);
       
        // Get the file info (for file length)
        fstat(fd, &finfo);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
        fdata = (char*)mmap(0, finfo.st_size + 1, 
			PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
#else
        // Memory map the file
        fdata = (char*)mmap(0, finfo.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd, 0);
#endif
#else
        uint64_t r = 0;

        fdata = (char *)malloc (finfo.st_size);
        while(r < (uint64_t)finfo.st_size)
            r += pread (fd, fdata + r, finfo.st_size, r);
#endif    
    
        // Get the number of results to display
        disp_num = (disp_num_str == NULL) ? DEFAULT_DISP_NUM : atoi(disp_num_str);

        get_time (end);

#ifdef TIMING
        print_time("initialize", begin, end);
#endif

#if SEQUENTIAL && PMC
        LIKWID_MARKER_START("mapreduce");
#endif // SEQUENTIAL && PMC
        get_time (begin);

        asap_dict = new wc_unordered_map;
        // all_dicts.push_back( asap_dict[i] );

        wc_unordered_map result = *asap_dict;

        // std::vector<std::pair<wc_word, size_t>> result;
        wc(fdata, finfo.st_size, 1024*1024, result, i, files.size());

        get_time (ser_begin);
#if SEQUENTIAL && PMC
        LIKWID_MARKER_STOP("mapreduce");
#endif // SEQUENTIAL && PMC

#if SEQUENTIAL && PMC
        LIKWID_MARKER_START("serialize");
#endif // SEQUENTIAL && PMC
        TRACE( e_smerge );

        // sorted_result[i].reserve( result.size() );

        TRACE( e_emerge );

        // No important performance difference between the two sort versions
        // for 100MB x4 data set.
        TRACE( e_ssort );
#if SEQUENTIAL
        // std::sort( sorted_result[i]->begin(), sorted_result[i]->end(), wc_sort_pred_by_first() );
#else
        // cilkpub::cilk_sort( sorted_result[i].begin(), sorted_result[i].end(), wc_sort_pred_by_first() );
#endif

        TRACE( e_esort );
        // cilkpub::cilk_sort_in_place( sorted_result[i].begin(), sorted_result[i].end(), wc_sort_pred() );
        get_time (end);
#if SEQUENTIAL && PMC
        LIKWID_MARKER_STOP("serialize");
#endif // SEQUENTIAL && PMC

#ifdef TIMING
        print_time("serialize", ser_begin, end);
        print_time("library", begin, end);
#endif
        get_time (begin);

// End of trace to go
        // printf("parts: %lu\n", parts );

#ifndef NO_MMAP
        munmap(fdata, finfo.st_size + 1);
#else
        // free (fdata);
#endif
        close(fd);

        dict.swap(result);
#if 0
    // Temporary trace: todelete khere
    std::cout << "Single Stage \n\n" << "Single File  " << files[i] << "\n";
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        std::cout << "Word: " << I->first.data << "\n";
        for( auto J=I->second.begin(), JE=I->second.end(); J != JE; ++J ) {
            // for( auto J=I->second.cbegin(), JE=I->second.end(); J != JE; ++J ) {
                std::cout << "       second: " << *J << "\n";
         }
    }
#endif
    }

    // Temporary trace: todelete khere
    std::cout << "Merge Stage \n\n" << "Merged File  " << "\n";
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        std::cout << "Word: " << I->first.data << "\n";
        for( auto J=I->second.begin(), JE=I->second.end(); J != JE; ++J ) {
            // for( auto J=I->second.cbegin(), JE=I->second.end(); J != JE; ++J ) {
                std::cout << "       second: " << *J << "\n";
         }
    }

    ofstream resFile ("testRes.txt", ios::out | ios::trunc | ios::binary);
    string headerText("Document Vectors (sequencefile @ hdfs):");
    string classText("Key class: class org.apache.hadoop.io.Text Value Class: class org.apache.mahout.math.VectorWritable");
    // Print the dictionary
    cout << "Dictionary\n" ;
  
    // printing mahoot Dictionary
    char nline='\n';
    char tab='\t';
    char comma=',';
    char colon=':';
    char rbrace='}';
    char what;
    cout << headerText;
    cout << "\t" << classText;
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write ((char *)&nline,1);
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write ((char *)&nline,1);
    for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
        resFile.write((char *)&tab, 1);
        uint64_t id = I.getIndex();
        string str = I->first.data;
        string loopStart = "Key: " + str + " Value: ";
        resFile.write((char *)loopStart.c_str(), loopStart.size());
        resFile.write((char *) &id, sizeof(uint64_t));
        resFile.write((char *) &nline, sizeof(char));
        // cout << "\t" << loopStart << std::to_string(static_cast<long long>(id)) << "\n";
        cout << "\t" << loopStart << id << "\n";
    }

    // printing mahoot output header
    resFile.write (headerText.c_str(), headerText.size());
    resFile.write((char *) &nline, sizeof(char));
    resFile.write ((char *)&tab,1);
    resFile.write (classText.c_str(), classText.size());
    resFile.write((char *) &nline, sizeof(char));
    cout << headerText << nline << "\t" << classText << nline;
    

    // size_t reducedCount[files.size()];
    for (unsigned int i = 0;i < files.size();i++) {
        // printing mahoot loop start text including filename twice !
        string keyStr = files[i];
        string loopStart = "Key: " + keyStr + ": " + "Value: " + keyStr + ":" + "{";
        resFile.write ((char *)&tab, 1);
        resFile.write (loopStart.c_str(), loopStart.size());
        cout << "\t" << loopStart;
        // resFile.write ((char *)&what, 1);

        // iterate over each word to collect total counts of each word in all files (reducedCount)
        // OR the number of files that contain the work (existsInFilesCount)
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

                size_t tf = I->second[i];
                if (!tf) continue;

                // todo: workout how the best way to calculate and store each 
                // word total once for all files
                cilk::reducer< cilk::op_add<size_t> > existsInFilesCount(0);
                cilk_for (int j = 0; j < I->second.size(); ++j) {
                    // *reducedCount += I->second[j];  // Use this if we want to count every occurence
                    if (I->second[j] > 0) *existsInFilesCount += 1;
                }

                //     Calculate tfidf  ---   Alternative versions of tfidf:
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) sumOccurencesOfWord + 1.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) numOfOtherDocsWithWord + 2.0)); 
                // double tfidf = tf * log10(((double) files.size() + 1.0) / ((double) reducedCount.get_value() + 1.0)); 
                // Sparks version;
                double tfidf = (double) tf * log10(((double) files.size() + 1.0) / ((double) existsInFilesCount.get_value() + 1.0)); 

                uint64_t id = I.getIndex();
                resFile.write ((char *) &id, sizeof(uint64_t));
                resFile.write ((char *) &colon, sizeof(char));
                resFile.write ((char *) &tfidf, sizeof(double));
                resFile.write ((char *) &comma, sizeof(char));
                cout << id << ":" << tfidf;
                if ( I != E ) {
                    cout << ",";
                }

        }
        cout << "\n";
        long pos = resFile.tellp();
        resFile.seekp (pos-1);
        resFile.write ((char *)&rbrace, 1);
        resFile.write ((char *)&nline, 1);
        cout << "}\n";
    }
    resFile.close();


    // Rough check on binary file:
    // -c at the command line with try to read results back in from binary file and display 
    // but note there will be funny chars before unsigned int64's as we do simple read of unsigned 
    // int 64 for 'id' Examining the binary file itself shows the binary file has the correct values for 'id'
    if (checkResults) {
        char colon, comma, cbrace, nline, tab;
        uint64_t id;
        double tfidf;
        char checkHeaderText[headerText.size()];
        char checkClassText[classText.size()];

        ifstream inResFile;
        inResFile.open("testRes.txt", ios::binary);

        cout << "\nREADING IN --------------------------------" << "\n" ;
        // reading mahoot Dictionary
        inResFile.read( (char*)&checkHeaderText, headerText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        inResFile.read( (char*)&tab, 1);
        inResFile.read( (char*)&checkClassText, classText.size());
        inResFile.read( (char*)&nline, sizeof(char));
        // checkClassText[classText.size()+1]=0;
        cout << checkHeaderText << nline << tab << checkClassText << nline;
        for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {
            inResFile.read( (char*)&tab, 1);

	    string str =  I->first.data;
            string iterStartCheck = "Key: " + str + " Value: ";
            char preText[iterStartCheck.size() +1 ];
            inResFile.read((char *)&preText, iterStartCheck.size());
            inResFile.read( (char*)&id, sizeof(uint64_t));
            inResFile.read( (char*)&nline, sizeof(char));
            cout << tab << preText << id << nline;
            // cout << tab << preText << id;
        }

            // reading mahoot TFIDF mappings per word per file
            inResFile.read( (char*)checkHeaderText, headerText.size());
            inResFile.read( (char*)&nline, sizeof(char));
            inResFile.read( (char*)&tab, 1);
            inResFile.read( (char*)checkClassText, classText.size());
            inResFile.read( (char*)&nline, sizeof(char));
            cout << checkHeaderText << nline << tab << checkClassText << nline;

            // read for each files
            for (unsigned int i = 0;i < files.size();i++) {
                string keyStr = files[i];
                string loopStart = "Key: " + keyStr + ": " + "Value: " + keyStr + ":" + "{";
                char preText[loopStart.size() +1];
                inResFile.read( (char*)&tab, 1);
                inResFile.read( (char*)&preText, loopStart.size());
                cout << tab << preText;
                
                // iterate over each word to collect total counts of each word in all files
                for( auto I=dict.begin(), E=dict.end(); I != E; ++I ) {

                    size_t tf = I->second[i];
                    if (!tf) continue;
                    inResFile.read( (char*)&id, sizeof(uint64_t));
                    inResFile.read( (char*)&colon, sizeof(char));
                    inResFile.read( (char*)&tfidf, sizeof(double));
                    inResFile.read( (char*)&comma, sizeof(char));
                    cout << id << colon << tfidf << comma ;
                }
                // inResFile.read((char*)&cbrace, 1);  // comma will contain cbrace after last iteration
                cbrace=comma;
                inResFile.read((char*)&nline, 1);
                cout << nline;
            }
    }

// #endif

    get_time (end);

#ifdef TIMING
    print_time("finalize", begin, end);
#endif

#if TRACING
    event_tracer::destroy();
#endif

#if SEQUENTIAL && PMC
    LIKWID_MARKER_CLOSE;
#endif // SEQUENTIAL && PMC
    
    free (fdata);

    return 0;
}
