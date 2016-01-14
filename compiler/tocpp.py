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
    declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;'}
                    # 'max_iters': 'const int VARIN = VAROUT;', 'num_clusters': 'const int VARIN = VAROUT;' }

    # action_map = {'input':'readDir(FILE_PARAM1);', 'run': 'tfidf(DATA_PARAM1, OP_PARAM1, OP_PARAM2);','output':'output(DATA_PARAM1, FILE_PARAM1);'}
    action_map = {}


    # set default data structure type in case none is specified in operator library
    dataStructType='word_map_type'

    # func for setting data structure type
    # @staticmethod
    def setDataStructType(self, dst):
        self.dataStructType=dst
        
    # To queue operator parameters
    paramQueue=deque([])

class kmeans:
    """ Represents options and declaration templates for kmeans """
    declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;',
                    'num_clusters': 'const int VARIN = VAROUT;', 'max_iters': 'const int VARIN = VAROUT;',
                    'force_dense': 'const bool VARIN = VAROUT;'}

    # action_map = {'input':'readFile(FILE_PARAM1);', 'run': 'kmeans(DATA_PARAM1, OP_PARAM1);', 'output':'output(DATA_PARAM1, FILE_PARAM1);'}
    action_map = {}

    # set default data structure type in case none is specified in operator library
    dataStructType='word_map_type'

    # func for setting data structure type
    # @staticmethod
    def setDataStructType(self, dst):
        self.dataStructType=dst
        

    # To queue operator parameters
    paramQueue=deque([])

"""
    Declare global instances of operator class instances so we can set/get member vars
    Note: not the best way.  If we ever need an operator more than once in workflow?
    these need to be made local to main and passesd as args to functions such as 
    setActionMappings etc
"""
g_tfidf = tfidf()
g_kmeans = kmeans()

"""
        map from xml element name to python library/templace code class above
"""
operator_map = {'tfidf': tfidf, 'kmeans': kmeans}

"""
    The template details of what an operator (including input and output) call 
    looks like for each of the operators is defined in an operators library xml file.
    This function reads in this file and sets the operator->call_template within
    each operators map member variable 'action_map'

"""
def setActionMappings(tree):
    root = tree.getroot()
    for operator in root.findall('ops/operator'):
        opName = operator.find('name')
        for template in operator.findall('template'):
            key=template.find('key').text
            value=template.find('value').text
            eval('g_'+opName.text).action_map[key]=value
        dataStructType = operator.find('datastructure/type')
        eval('g_'+opName.text).setDataStructType(dataStructType.text)
        # print opName.text, " imm after setting, struct type is ", eval('g_'+opName.text).dataStructType



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
    oplibfile = ''

    """  Read arguments """
    try:
        opts, args = getopt.getopt(argv,"hi:o:l:",["ifile=","ofile=","lfile="])

    except getopt.GetoptError:
        print 'tocpp.py -i <workflowfile> -l <operatorlibraryfile> -o <codefile>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'tocpp.py -i <workflowfile> -o <codefile>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            workflowfile = arg
        elif opt in ("-l", "--lfile"):
            oplibfile = arg
        elif opt in ("-o", "--ofile"):
            codefile = arg

    """" Parse the operators library file to gather info for op call templates """
    oplib  = open(oplibfile, "r")
    oplibtree=ET.parse(oplibfile)
    oplib.close()
    setActionMappings(oplibtree)

    """  Parse workflow description from XML file """
    workflow = open(workflowfile, "r")
    code = open(codefile, "w")
    wftree=ET.parse(workflowfile)
    workflow.close()
    root = wftree.getroot()
    
    """ Set tab level for generated code """
    tabcount=0
    
    """ print start of main block """
    # tabPrint("int main() {\n\n", tabcount, code)
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


    """" START OF SECTION PRINTING TEMPLATE FILES """

    """   
        Loop here so we get each operators version of header and main declaration sections
    """

    """ print header and main_declarations template sections of code """
    with open('templates/header.template', 'r') as myfile:
        data=myfile.read()
    myfile.close()
    tabPrint(data, 0, code)

    for operator in root.findall('ops/operator'):
        outterOpName = operator.find('run').text
    
        """ 
            Add the declarations for input and output 'child's of operator 
        """
        tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", 0, code)
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
                        tabPrint (declarationStr, 0, code)
                        tabPrint ("\n", 0, code)
                    interFileIOParamQueue.append(var)
                    ctr += 1 
        tabPrint ("\n", 0, code)
        
        """
           Print declarations for ordinary variables (read from attributes in operator 'run' element)
        """ 
        tabPrint("//  Variable Declarations \n\n", 0, code)
        # ctr=0; 
        for operator in root.findall('ops/operator'):
            opName = operator.find('run')
            for attributeTuple in opName.items():
                declaration=operator_map[opName.text].declarations[attributeTuple[0]]
                if declaration is not None :
                    # var = attributeTuple[0]+`ctr`
                    var = attributeTuple[0]
                    operator_map[opName.text].paramQueue.append(var)
                    declarationStr = operator_map[opName.text] \
                                         .declarations[attributeTuple[0]] \
                                         .replace("VARIN", var).replace("VAROUT", attributeTuple[1]) \
                                         + '\n'
                    tabPrint (declarationStr, 0, code)
            # ctr += 1
        tabPrint ("\n", 0, code)

        """ 
            output parse params codes for operator 
            Actually NO, these come from workflow & operator library for now

        with open('templates/'+outterOpName+'parseparams.template', 'r') as myfile:
            data=myfile.read()
        myfile.close
        tabPrint(data, 0, code)
        """


        tabPrint ("//  Calls to Operator functions  \n\n", tabcount, code)
        ctr=0
        for runoperator in root.findall('ops/operator/run'):
            opName = runoperator.text

            """ 
                output start of main section 
            """
            with open('templates/'+opName+'maindeclarations.template', 'r') as myfile:
                data=myfile.read()
            myfile.close
            tabPrint(data, 0, code)

            """ operator input section """
            with open('templates/'+opName+'input.template', 'r') as myfile:
                data=myfile.read().replace('FILE_PARAM1',interFileIOParamQueue.popleft())
            myfile.close()
            tabPrint(data, 0, code)

            """ operators calling sequence 
                first read the catalog_build replacement code from the appropriate file
            """
            with open('templates/'+opName+'_'+'callsequence.template', 'r') as myfile:
                
                data=myfile.read().replace('WORD_TYPE',eval('g_'+opName).dataStructType)
                myfile.close()
                if 'CATALOG_BUILD_CODE' in data:
                    with open('templates/'+opName+'_'+eval('g_'+opName).dataStructType+'_catalogbuild.template', 'r') as myfile:
                        catalogBuildCode=myfile.read()
                        myfile.close()
                    data=data.replace('CATALOG_BUILD_CODE',catalogBuildCode)
                # data=myfile.read().replace('WORD_TYPE','TEST').replace('CATALOG_BUILD_CODE',catalogBuildCode)
            tabPrint(data, 0, code)

            """ output section """
            with open('templates/'+opName+'output.template', 'r') as myfile:
                data=myfile.read().replace('OUTFILE',interFileIOParamQueue.popleft())
                myfile.close()
            tabPrint(data, 0, code)

        """ output close of main section """
        with open('templates/'+outterOpName+'mainclose.template', 'r') as myfile:
            data=myfile.read()
            myfile.close()
        tabPrint(data, 0, code)

    code.close()

    """  TODELETE, 
       Print calls to the actual operator core functions
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
    

"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
