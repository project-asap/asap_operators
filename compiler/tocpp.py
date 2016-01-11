#!/usr/bin/python

import sys, getopt
import elementtree.ElementTree as ET
from elementtree.ElementTree import Element, SubElement
from collections import deque
import re

"""
        Builds the operator function call by replacing PARAM placeholders with the actual generated variable names.
        The variable names have been previously queued at declaration time in 2 of 3 parameter type queues.

        The 3 parameter queue type are:
        1. Data parameters queue contains 'linking' variables which accept return values from 
                  one function call for input as an argument to the next function. 
        2. Operator Parameters queue contains specific tuning parameters for the operators (eg. #clusters)
        3. File parameters queue contains variables which hold input and output filename as specified in
                  the input and output elements of the workflow xml description.
  
"""
def func_call(tag, signature, data_op_queue, fileio_op_queue, op_queue):

    # Set DATA_PARAMS first (ie. vars named data* which are workflow parameters)
    paramPtr=1
    currentParam="DATA_PARAM"+`paramPtr` 
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
    paramPtr=1
    currentParam="FILE_PARAM"+`paramPtr` 
    while currentParam in signature:
        replaceParam=fileio_op_queue.popleft()
        signature = signature.replace(currentParam, replaceParam)
        paramPtr+=1
        currentParam="PARAM"+`paramPtr` 
    return signature

"""
        Operator classes contains template information for constructs (declarations and function calls) necessary
        to build an actual c++ execution call from the description of workflow.

        For example, PARAM placeholders in template code are replaced with previously declared/generated variable names.

        A Queue of operator parameters (specified as attributes to the 'run' element in XML workflow) is stashed in paramQueue
"""
class tfidf:
    """ Represents options and declaration templates for tfidf """
    declarations = {'input': 'const char * VARIN = VAROUT;', 'output': 'const char * VARIN = VAROUT;',
                    'maxiters': 'const int VARIN = VAROUT;', 'numclusters': 'const int VARIN = VAROUT;' }

    action_map = {'input':'readDir(FILE_PARAM1);', 'run': 'tfidf(DATA_PARAM1, OP_PARAM1, OP_PARAM2);','output':'output(DATA_PARAM1, FILE_PARAM1);'}

    # To queue operator parameters
    paramQueue=deque([])

class kmeans:
    """ Represents options and declaration templates for kmeans """
    declarations = {'input': 'const char * VARIN = VAROUT;', 'output': 'const char * VARIN = VAROUT;',
                    'numclusters': 'const int VARIN = VAROUT;'}

    action_map = {'input':'readFile(FILE_PARAM1);', 'run': 'kmeans(DATA_PARAM1, OP_PARAM1);', 'output':'output(DATA_PARAM1, FILE_PARAM1);'}

    # To queue operator parameters
    paramQueue=deque([])



"""
        map from xml element name to python library/templace code class above
"""
operator_map = {'tfidf': tfidf, 'kmeans': kmeans}


"""
        Miscellaneous functions
"""
def tabPrint(str, tabcount, f):
    f.write(' ' * tabcount*4 + str)

def isInputOrOutput(elem):
    if re.search('input|output', elem.tag):
        return True
    return False

def isInput(elem):
    if re.search('input', elem.tag):
        return True
    return False

def isOutput(elem):
    if re.search('output', elem.tag):
        return True
    return False

"""                              
        Beginning of main processing block

"""
def main(argv):
    workflowfile = ''
    codefile = ''

    """  Read arguments """
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

    """  Parse workflow description from XML file """
    workflow = open(workflowfile, "r")
    code = open(codefile, "w")
    wftree=ET.parse(workflowfile)
    root = wftree.getroot()
    
    """ Set tab level for generated code """
    tabcount=0
    
    """ print start of main block """
    tabPrint("int main() {\n\n", tabcount, code)
    tabcount=1

    """ 
        Initialise queue for inter-op data parameters and inter op IO file parameters
        This have to have 'main' scope as they span beyond operator scope
    """
    interDataParamQueue=deque([])
    interFileIOParamQueue=deque([])


    """ 
        Keep track of IO files declared so we can merge variables names so we have 1 var for the same filename
    """
    declaredIOFiles={}

    """ 
        Add the declarations for input and output 'childs' of operator 
    """
    tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", tabcount, code)
    ctr=0
    for operator in root.findall('ops/operator'):
        opName = operator.find('run')
        for child in operator:
            if isInputOrOutput(child) is not True:
                continue
            declaration=operator_map[opName.text].declarations[child.tag]
            if declaration is not None :
                var = child.tag+`ctr`
                # print "After appending in ", child.tag, " ", child.text, " var q has ", interFileIOParamQueue
                ## Keep track of io files for condensing vars, if it already exists use existing var handle
                if declaredIOFiles.has_key(child.text):
                    prevVar = declaredIOFiles[child.text]
                    var = prevVar
                else:
                    declaredIOFiles[child.text]=var
                    declarationStr = operator_map[opName.text] \
                                     .declarations[child.tag] \
                                     .replace("VARIN", var).replace("VAROUT", child.text)
                    tabPrint (declarationStr, tabcount, code)
                    tabPrint ("\n", tabcount, code)
                interFileIOParamQueue.append(var)
                ctr += 1 
    tabPrint ("\n", tabcount, code)
    
    """
       Print declarations for ordinary variables (read from attributes in operator 'run' element)
    """ 
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
                tabPrint (declarationStr, tabcount, code)
        ctr += 1
    tabPrint ("\n", tabcount, code)
    
    """ 
       Print calls to the actual operator core functions
    """
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
                    if isOutput(child) is False:
                        interDataParamQueue.append("var"+`ctr`)
                    tabPrint (statement, tabcount, code)
            ctr += 1
    tabcount -=1
    tabPrint ("\n}", tabcount, code)
    

"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
