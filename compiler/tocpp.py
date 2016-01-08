#!/usr/bin/python

import sys, getopt
import elementtree.ElementTree as ET
from elementtree.ElementTree import Element, SubElement
from collections import deque
import re

def func_call(tag, signature, data_op_queue, fileio_op_queue, op_queue):

    # Set DATA_PARAMS first (ie. vars named data* which are workflow parameters)
    paramPtr=1
    currentParam="DATA_PARAM"+`paramPtr` 
    # while data_op_queue:
    while currentParam in signature:
        replaceParam=data_op_queue.popleft()
        signature = signature.replace(currentParam, replaceParam)
        paramPtr+=1
        currentParam="DATA_PARAM"+`paramPtr` 

    # Set OP_PARAMS next (ie. special attributes such as 'num of clusters' to operators
    paramPtr=1
    currentParam="OP_PARAM"+`paramPtr` 
    while currentParam in signature:
        replaceParam=op_queue.popleft()
        signature = signature.replace(currentParam, replaceParam)
        paramPtr+=1
        currentParam="OP_PARAM"+`paramPtr` 

    # Set FILE_PARAMS next (ie. original input and output filename parameters
    # if tag != 'run':
        # return signature
    paramPtr=1
    currentParam="FILE_PARAM"+`paramPtr` 
    while currentParam in signature:
        replaceParam=fileio_op_queue.popleft()
        signature = signature.replace(currentParam, replaceParam)
        paramPtr+=1
        currentParam="PARAM"+`paramPtr` 
    return signature


class tfidf:
    """ Represents options and declaration templates for tfidf """
    declarations = {'input': 'const char * VARIN = VAROUT;', 'output': 'const char * VARIN = VAROUT;',
                    'maxiters': 'const int VARIN = VAROUT;', 'numclusters': 'const int VARIN = VAROUT;' }

    paramQueue=deque([])

    action_map = {'input':'readDir(FILE_PARAM1);', 'run': 'tfidf(DATA_PARAM1, OP_PARAM1, OP_PARAM2);','output':'output(DATA_PARAM1, FILE_PARAM1);'}

    def setInFile(infile):
        self.infile=infile

    def setInFile(outfile):
        self.outfile=outfile



class kmeans:
    """ Represents options and declaration templates for kmeans """
    declarations = {'input': 'const char * VARIN = VAROUT;', 'output': 'const char * VARIN = VAROUT;',
                    'numclusters': 'const int VARIN = VAROUT;'}
    paramQueue=deque([])

    action_map = {'input':'readFile(FILE_PARAM1);', 'run': 'kmeans(DATA_PARAM1, OP_PARAM1);', 'output':'output(DATA_PARAM1, FILE_PARAM1);'}


operator_map = {'tfidf': tfidf, 'kmeans': kmeans}

def tabPrint(str, tabcount, f):
    f.write(' ' * tabcount*4 + str)
    
#  Keep track of what has already been declared
declared = {}

def isInputOrOutput(elem):
    if re.search('input|output', elem.tag):
        return True
    return False

"""                              
        Beginning of main processing block

"""

def main(argv):
    workflowfile = ''
    codefile = ''

    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])

    except getopt.GetoptError:
        print 'tocpp.py -i <workflowfile> -o <codefile>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'tocpp.py -i <workflowfile> -o <codefile>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            workflowfile = arg
        elif opt in ("-o", "--ofile"):
            codefile = arg

    workflow = open(workflowfile, "r")
    code = open(codefile, "w")
    
    #  Parse the workfown from XML
    wftree=ET.parse(workflowfile)
    root = wftree.getroot()
    
    # Miscellaneous 
    tabcount=0
    
    # print start of main block
    tabPrint("int main() {\n\n", tabcount, code)
    tabcount=1

    # Add the declarations for input and output childs of operator for child in operator:
    tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", tabcount, code)
    ctr=0
    interDataParamQueue=deque([])
    interFileIOParamQueue=deque([])
    for operator in root.findall('ops/operator'):
        opName = operator.find('run')
        # for child in operator.findall('input'):
        for child in operator:
            if isInputOrOutput(child) is not True:
                continue
            declaration=operator_map[opName.text].declarations[child.tag]
            if declaration is not None :
                var = child.tag+`ctr`
                # interDataParamQueue=[var]
                interFileIOParamQueue.append(var)
                declarationStr = operator_map[opName.text] \
                                     .declarations[child.tag] \
                                     .replace("VARIN", var).replace("VAROUT", child.text)
                declared[var]=True
                tabPrint (declarationStr, tabcount, code)
                tabPrint ("\n", tabcount, code)
    tabPrint ("\n", tabcount, code)
    
    # Print declarations for ordinary variables (read from attributes in operator 'run' element)
    tabPrint("//  Variable Declarations ## \n\n", tabcount, code)
    ctr=0;
    for operator in root.findall('ops/operator'):
        opName = operator.find('run')
        for attributeTuple in opName.items():
            declaration=operator_map[opName.text].declarations[attributeTuple[0]]
            if declaration is not None :
                var = attributeTuple[0]+`ctr`
                operator_map[opName.text].paramQueue.append(var)
                declarationStr = operator_map[opName.text] \
                                     .declarations[attributeTuple[0]] \
                                     .replace("VARIN", var).replace("VAROUT", attributeTuple[1]) \
                                     + '\n'
                declared[var]=True
                tabPrint (declarationStr, tabcount, code)
        ctr += 1
    tabPrint ("\n", tabcount, code)
    
    # Print calls to the actual operator core functions
    tabPrint ("//  Calls to Operator functions  \n\n", tabcount, code)
    ctr=0
    for operator in root.findall('ops/operator'):
        opName = operator.find('run')
        for child in operator:
            if child.tag in operator_map[opName.text].action_map:
                if child.text is not None:
                    statement="var"+`ctr` + " = " + func_call(child.tag, \
                                 operator_map[opName.text].action_map[child.tag], \
                                 interDataParamQueue, \
                                 interFileIOParamQueue, \
                                 operator_map[opName.text].paramQueue) \
                                 + '\n'
                    interDataParamQueue.append("var"+`ctr`)
                    tabPrint (statement, tabcount, code)
            ctr += 1
    tabcount -=1
    tabPrint ("\n}", tabcount, code)
    
if __name__ == "__main__":
   main(sys.argv[1:])
