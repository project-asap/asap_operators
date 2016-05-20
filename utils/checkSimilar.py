#!/usr/bin/python

import sys, getopt
import re

# outf = open(out, 'w')
# with open(inf) as f:    
    # pattern = re.compile(r"[^-\d]*(-?\d+\.\d+)[^-\d]*")  

def isclose(a, b, rel_tol=1e-03, abs_tol=0.0):
    return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

def populateArray(inf, array):

    with open(inf) as f:
        pattern = re.compile(r"[^-\d]*(-?\d+\.\d+)[^-\d]*(-?\d+\.\d+)[^-\d]*(-?\d+\.\d+)[^-\d]*")  
        for line in f:
            match = pattern.match(line)
            if match:
                # outf.write(match.group(1) + '\n' + match.group(2) + '\n' + match.group(3) + '\n')
	        array.append(match.group(1))
	        array.append(match.group(2))
	        array.append(match.group(3))
	# print "list values are ",array


def main(argv):

	inf1=0
	inf2=0

        try:
            opts, args = getopt.getopt(argv,"hi:j:",["ifile=","jfile="])

        except getopt.GetoptError:
            print 'checkSimilar.py -i <infile1> -j <infile2>'
            sys.exit(2)

    	for opt, arg in opts:
        	if opt == '-h':
            		print './checkSimilar.py -i <infile1> -j <infile2>'
            		sys.exit(2)
        	elif opt in ("-i", "--ifile"):
            		inf1 = arg
        	elif opt in ("-j", "--jfile"):
            		inf2 = arg

	if (not inf1 or not inf2):
	    print "Must specify 2 input files, use command format is:   ./checkSimilar.py -i <infile1> -j <infile2>"
	    sys.exit(2)

	file1values = []
	file2values = []

	populateArray(inf1, file1values)
	populateArray(inf2, file2values)

	for a, b in zip(file1values, file2values):
    	    # print(a, b)
    	    if isclose(float(a),float(b)) is False:
        	    # print "variance detected"
		    sys.exit(1)
	
    	sys.exit(0)



"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
