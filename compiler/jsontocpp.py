#!/usr/bin/python

import json
from pprint import pprint
import sys, getopt
from collections import deque
import re
import unittest
""" todelete
import elementtree.ElementTree as ET
from elementtree.ElementTree import Element, SubElement
"""
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
    io_declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;'}

    attribute_declarations = {'max_iters': 'const int VARIN = VAROUT;', 'num_clusters': 'const int VARIN = VAROUT;' }

    # records function signatures from operator library
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
    io_declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;'}

    attribute_declarations = {
                    'num_clusters': 'const int VARIN = VAROUT;', 'max_iters': 'const int VARIN = VAROUT;',
                    'force_dense': 'const bool VARIN = VAROUT;',
                    'by_words': 'const bool VARIN = VAROUT;', 'do_sort': 'const bool VARIN = VAROUT;'}

    # records function signatures from operator library
    action_map = {}

    # set default data structure type in case none is specified in operator library
    dataStructType='word_map_type'

    # func for setting data structure type
    # @staticmethod
    def setDataStructType(self, dst):
        self.dataStructType=dst

    # To queue operator parameters
    paramQueue=deque([])

class tfidfkmeans:
    """ Represents options and declaration templates for tfidfkmeans """
    io_declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;'}

    attribute_declarations = {
                    'num_clusters': 'const int VARIN = VAROUT;', 'max_iters': 'const int VARIN = VAROUT;',
                    'force_dense': 'const bool VARIN = VAROUT;',
                    'by_words': 'const bool VARIN = VAROUT;', 'do_sort': 'const bool VARIN = VAROUT;'}

    # records function signatures from operator library
    action_map = {}


    # set default data structure type in case none is specified in operator library
    dataStructType='word_list_type'

    # func for setting data structure type
    def setDataStructType(self, dst):
        self.dataStructType=dst

    # To queue operator parameters
    paramQueue=deque([])



# Perhaps won't be needed
# class outputTypes:
    # def __init__(self, types):
        # pass # todo, store in zero based array

class Constraint:
    def __init__(self, ctype):
        self.ctype = cdes

class DataSetConstraint(Constraint):

    # Constructor for dataset 
    def __init__(self, dinfotype, fs, dstype):
        self.dinfotype = dinfotype
        self.Engine_FS = fs
        self.dstype = dstype

class OperatorConstraint(Constraint):

    # Constructor for operarator 
    def __init__(self, fs, runFile, algname, alg_dstructtype, outnum, outtypes):
        self.EngineSpecification_FS = fs
        self.runFile = runFile
        self.algname = algname
        self.algtype = alg_dstructtype
        self.outnum = outnum
        self.outtypes = outtypes

class Operator:

    """ Represents options and declaration templates for an operator """
    def __init__(self, name, description, constraint, inlist, status="stopped"):
        self.name = name
        self.description = description
        self.constraint = constraint
        # self.epath = epath
        self.inlist = inlist
        self.status = status

class Dataset:

    """ Represents options and declaration templates for a dataset """
    declarations = {'input': 'char const * VARIN = VAROUT;', 'output': 'char const * VARIN = VAROUT;'}

    def __init__(self, name, description, constraint, epath, inlist, status="stopped"):
        self.name = name
        self.description = description
        self.constraint = constraint
        self.epath = epath
        self.inlist = inlist
        self.status = status

class Signature:
    def __init__(self, input, output, run):
        self.input = input
        self.output = output
        self.run = run

"""   
    Classes for representing information from the Workflow Description Boxes
"""

class InputBox:

    """ Represents an input box in the workflow description """
    def __init__(self, name, filename):
        self.name = name
        self.filename = filename

class OutputBox:

    """ Represents an output box in the workflow description """
    def __init__(self, name, filename):
        self.name = name
        self.filename = filename

class OperatorBox:

    """ Represents an operator box in the workflow description """
    def __init__(self, name, args, binput, boutput):
        self.name = name
        self.args = args
        self.binput = binput
        self.boutput = boutput

def loadWorkflowData(data):
    for box in data["boxes"]:
        if box["type"] == "input":
            newinputbox = InputBox(box["name"],
                                     box["filename"])
            g_inputBoxMap[box["id"]] = newinputbox
        elif box["type"] == "output":
            newoutputbox = OutputBox(box["name"],
                                     box["filename"])
            g_outputBoxMap[box["id"]] = newoutputbox
        elif box["type"] == "operator":
            newoperatorbox = OperatorBox(box["name"],
                                     box["args"],
                                     box["input"],
                                     box["output"])
            g_operatorBoxMap[box["id"]] = newoperatorbox

"""
    Declare global instances of operator classes so we can set/get member vars
    Note: not the best way.  If we ever need an operator more than once in workflow?
    these need to be made local to main and passed as arguments to functions such as 
    setActionMappings etc
"""
g_tfidf = tfidf()
g_kmeans = kmeans()
g_tfidfkmeans = kmeans()
g_datasetsMap = {}
g_operatorsMap = {}
g_signatureMap = {}
g_typedefMap = {}
g_iodeclMap = {}
g_argsdeclMap = {}
g_inputBoxMap = {}
g_outputBoxMap = {}
g_operatorBoxMap = {}
g_argMap = {}

"""
        map from xml element name to python library/template code class above
"""
operator_map = {'tfidf': tfidf, 'kmeans': kmeans, 'tfidfkmeans': tfidfkmeans}

"""
    The template details of what an operator (including input and output) call 
    looks like for each of the operators as defined in an operators library xml file.
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


def loadOperatorLibraryData(data):
    for operator in data["operators"]:
        if operator["type"] == "dataset":
            print "process dataset"
            newconstraint = DataSetConstraint(operator["Constraints.DataInfo.type"], 
                                             operator["Constraints.Engine.FS"],
					     operator["Constraints.type"])
            newdataset = Dataset(operator["name"], 
                                             operator["description"], 
                                             newconstraint, 
                                             operator["Execution.path"], 
                                             operator["input"], 
                                             operator["status"])
            g_datasetsMap[operator["name"]] = newdataset
            print "Added dataset"
        elif operator["type"] == "operator":
            newconstraint = OperatorConstraint(
                                             operator["Constraints.EngineSpecification.FS"],
					     operator["Constraints.runFile"],
					     operator["Constraints.Algorithm.name"],
					     operator["Constraints.Algorithm.dstruct_type"],
					     operator["Constraints.Output.number"],
					     operator["Constraints.Output.types"])
            newoperator = Operator(operator["name"], 
                                             operator["description"], 
                                             newconstraint, 
                                             operator["input"], 
                                             operator["status"])
            g_operatorsMap[operator["name"]] = newoperator
            print "Added operator"
        elif operator["type"] == "signature_rule":
            newsignature = Signature(operator["input"], operator["output"], operator["run"])
            for algorithmName in operator["algorithm.names"]:
                g_signatureMap[algorithmName] = newsignature
            print "Added signature"
        elif operator["type"] == "typedef":
            for algorithmName in operator["algorithm.names"]:
                for algorithmType in operator["algorithm.types"]:
                    g_typedefMap[tuple([algorithmName, algorithmType])] = operator["types"]
            print "Added typedef"
        elif operator["type"] == "inout_declaration":
            for algorithm in operator["algorithm.names"]:
		g_iodeclMap[tuple([algorithm,"input"])] = operator["input"]
		g_iodeclMap[tuple([algorithm,"output"])] = operator["output"]
            print "inout_delcaration"
        elif operator["type"] == "arg_declaration":
            for algorithm in operator["algorithm.names"]:
                ## for template in operator["argTemplates"]:
                    ## # print template.argName, template.template
		    ## # g_argsdeclMap[algorithm] = operator["argTemplates"]
		g_argsdeclMap[algorithm] = operator["argTemplates"]
            print "arg_delcaration"
        else:
            print "Error"
 	    sys.exit



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
    liboperatorsfile = ''

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
            liboperatorsfile = arg
        elif opt in ("-o", "--ofile"):
            codefile = arg
    """
    Parse the operators library file to gather info for op call templates 
    oplib  = open(oplibfile, "r")
    oplibtree=ET.parse(oplibfile)
    oplib.close()
    setActionMappings(oplibtree)

    Parse workflow description from XML file 
    workflow = open(workflowfile, "r")
    code = open(codefile, "w")
    wftree=ET.parse(workflowfile)
    workflow.close()
    root = wftree.getroot()
    """

    """ 
        Parse QUB's materialsed operators, including input output datasets
        and store all relevant information on datasets, operators, typedefs
    """
    workflow = open(workflowfile, "r")
    flow = json.load(workflow)
    workflow.close()

    liboperators = open(liboperatorsfile, "r")
    libdata = json.load(liboperators)
    liboperators.close()

    code = open(codefile, "w")

    loadOperatorLibraryData(libdata)

    loadWorkflowData(flow)
    """
    print "-------------------------------- Operators -------------------------------------------- "
    pprint(g_operatorsMap)
    print "-------------------------------- Signatures -------------------------------------------- "
    pprint(g_signatureMap)
    print "-------------------------------- Typedefs -------------------------------------------- "
    pprint(g_typedefMap)
    """
    print "-------------------------------- iodelcs -------------------------------------------- "
    pprint(g_iodeclMap)
    print "-------------------------------- arg decls -------------------------------------------- "
    print g_argsdeclMap
    print "-------------------------------- Datasets -------------------------------------------- "
    pprint(g_datasetsMap)
    print "-------------------------------- operator Boxes -------------------------------------------- "
    pprint(g_operatorBoxMap)
    print "-------------------------------- input Boxes -------------------------------------------- "
    pprint(g_inputBoxMap)
    print "-------------------------------- output Boxes -------------------------------------------- "
    pprint(g_outputBoxMap)

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


    """ print header and main_declarations template sections of code """
    with open('templates/header.template', 'r') as myfile:
        data=myfile.read()
    myfile.close()
    tabPrint(data, 0, code)
    tabPrint ("\n", 0, code)

    """   
        1st pass
        Outter Loop here so we get each operators version of code section template files 
        Or perhaps this will be a first pass to gather lookahead info needed
    """
    for box in flow["boxes"]:

        """ 
            Print the declarations for input and output files used by operators
        """
        if box["type"] == "operator":
            algName = g_operatorsMap[box["name"]].constraint.algname

            tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", 0, code)
            for iotype in ["input","output"]:
                declarationTemplate = g_iodeclMap[tuple([algName,iotype])]
                filename = eval('g_'+iotype+'BoxMap')[box[iotype][0]].filename
                inputId = box[iotype][0]
                outputId = box[iotype][0]
                datasetPath =  g_datasetsMap[eval('g_'+iotype+'BoxMap')[eval(iotype+'Id')].name].epath
    
                # Todo assertions
                # unittest.TestCase.assertTrue(g_inputBoxMap[box["input"][0]].type == "input")
        
                ctr=0
                if declarationTemplate is not None :
                        var = iotype+`ctr`
    
                        ## Keep track of io files for condensing vars, if it already exists use existing var handle
                        if declaredIOFiles.has_key(filename):
                            prevVar = declaredIOFiles[filename]
                            var = prevVar
                        else:
                            declaredIOFiles[filename]=var
                            declarationStr = declarationTemplate.replace("VARIN", var) \
                                                       .replace("VAROUT","\""+datasetPath+filename+"\"")
                            tabPrint (declarationStr, 0, code)
                            tabPrint ("\n", 0, code)
                        g_argMap[(algName,inputId)] = var
                        interFileIOParamQueue.append(var)
                        ctr += 1 
    tabPrint ("\n", 0, code)

    for box in flow["boxes"]:

        """
           Print the declarations for ordinary variables (read from args field in operator Box)
        """ 
        if box["type"] == "operator":
            actual_args = box["args"]
            pprint(actual_args)

            tabPrint ("//  Variable Declarations for operator arguments \n\n", 0, code)
	    for arg in actual_args.keys():
                algName = g_operatorsMap[box["name"]].constraint.algname
                pprint(g_argsdeclMap[algName])
                declDict = g_argsdeclMap[algName][0]
                if declDict.has_key(arg):
                   declarationTemplate=declDict[arg] 
                   var = arg 
                   declarationStr = declarationTemplate.replace("VARIN",var) \
                                         .replace("VAROUT",actual_args[arg]) \
                                         + '\n'
                   tabPrint(declarationStr, 0, code)

                else:
                    print "Error: ", arg, " is not a valid argument to ", algName

        else:
            continue

        tabPrint ("\n", 0, code)
        
    """ 2nd pass """
    for box in flow["boxes"]:

        """
           Print calls to Operator functions according to 'run' element from workflow
        """ 
        ctr=0
        if box["type"] == "operator":

            """ 
                output start of main section 
            """
            tabPrint ("//  Start of main section \n\n", 0, code)
            algName = g_operatorsMap[box["name"]].constraint.algname
            algStructType = g_operatorsMap[box["name"]].constraint.algtype
            with open('templates/'+algName+'maindeclarations.template', 'r') as myfile:
                data=myfile.read()
            myfile.close
            tabPrint(data, 0, code)

            tabPrint ("//  Start of typedefs section \n\n", tabcount, code)
            typedefs = g_typedefMap[tuple([algName, algStructType])]
            for typedef in typedefs:
                tabPrint(typedef, tabcount, code)
                tabPrint ("\n", tabcount, code)
            tabPrint ("\n", tabcount, code)

            tabPrint ("//  Input section \n\n", tabcount, code)

            """ operator input section, """
            """ NOTE: for now assume we have 1 input to operator...
                Planning ahead--> if there are say 2 inputs, then there should be 
                2 FILE_PARAMX markers in the template file, in which case 
                this code section could become a loop for every marker, pick off the inputs of
                the operator - still TODO """
            with open('templates/'+algName+'input.template', 'r') as myfile:
                # data=myfile.read().replace('FILE_PARAM1',interFileIOParamQueue.popleft())
                data=myfile.read().replace('FILE_PARAM1',g_argMap[(algName,box["input"][0])]) # TODO when in loop, remove subscript
            myfile.close()
            tabPrint(data, tabcount, code)
            tabPrint ("\n", tabcount, code)

            tabPrint ("//  Calling sequence section \n\n", tabcount, code)
            """ operators calling sequence 
                first read the catalog_build replacement code from the appropriate file
            """
            with open('templates/'+algName+'_'+'callsequence.template', 'r') as myfile:
                
                # data=myfile.read().replace('WORD_TYPE',eval('g_'+algName).dataStructType)
                data=myfile.read().replace('WORD_TYPE',algStructType)
                myfile.close()
                if 'CATALOG_BUILD_CODE' in data:
                    with open('templates/'+algName+'_'+algStructType+'_catalogbuild.template', 'r') as myfile:
                        catalogBuildCode=myfile.read()
                        myfile.close()
                    data=data.replace('CATALOG_BUILD_CODE',catalogBuildCode)
            tabPrint(data, tabcount, code)

            tabPrint ("//  Output section \n\n", tabcount, code)
            """ output section """
            with open('templates/'+algName+'output.template', 'r') as myfile:
                # data=myfile.read().replace('OUTFILE',interFileIOParamQueue.popleft())
                data=myfile.read().replace('OUTFILE',g_argMap[(algName,box["output"][0])]) # TODO when in loop, remove subscript
                myfile.close()
            tabPrint(data, tabcount, code)
            tabPrint ("\n", tabcount, code)

            tabPrint ("//  Closing main section \n\n", tabcount, code)
            """ output close of main section """
            with open('templates/'+algName+'mainclose.template', 'r') as myfile:
                data=myfile.read()
                myfile.close()
            tabPrint(data, tabcount, code)

    code.close()

"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
