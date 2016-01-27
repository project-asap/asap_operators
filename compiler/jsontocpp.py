#!/usr/bin/python
"""
	This program translates a high leve workflow description in json to swan codes.

	The code can be categorised into sections as:

		Definitions of dictionaries used to associate rules of between:
                   	operators, inputs and output

		Definitions of the holding classes for:
			materialised operators
			workflow descriptions

		Functions which load:
			workflow descriptions into holding classes
			materialised operator descriptions into holding classes

		Main function, which :
			parses json files
			calls the load function to populate holding classes
			print code in section to output code file by:
			    for each operator:
				print header file section
				print file section
				print Variable Declarations holding input/output filenames                                                                           
				print Variable Declarations for operator arguments
				print Start of main section
				print typedefs section
				Input section
				print Calling sequence section
				print Output section
				print Closing main section 
"""

import json
from pprint import pprint
import sys, getopt
from collections import deque
import re
import unittest



"""
    A set of dictionary maps which relate operators and algorithms to rules, signatures, 
    typedefs, io declarations and relate library operators to workflow boxes.
"""

""" operator library dictionaries """
g_datasetsMap = {}
g_operatorsMap = {}
g_signatureMap = {}
g_typedefMap = {}
g_iodeclMap = {}
g_argsdeclMap = {}
g_argsdefaultsMap = {}

""" workflow dictionaries """
g_inputBoxMap = {}
g_outputBoxMap = {}
g_operatorBoxMap = {}
g_argMap = {}


"""
        SCHEMA CLASSES which hold data parsed from a 
        JSON files representing materialised operators 
        and a JSON file represeting a description of 
        the user workflow
"""

""" holds data about data constraints """
class DataSetConstraint:
    def __init__(self, dinfotype, fs, dstype):
        self.dinfotype = dinfotype
        self.Engine_FS = fs
        self.dstype = dstype

""" holds data about operator constraints """
class OperatorConstraint:
    def __init__(self, fs, runFile, algname, alg_dstructtype, outnum, outtypes):
        self.EngineSpecification_FS = fs
        self.runFile = runFile
        self.algname = algname
        self.algtype = alg_dstructtype
        self.outnum = outnum
        self.outtypes = outtypes

""" holds data about operators """
class Operator:

    def __init__(self, name, description, constraint, inlist, status="stopped"):
        self.name = name
        self.description = description
        self.constraint = constraint
        self.inlist = inlist
        self.status = status

""" holds data about datasets """
class Dataset:

    def __init__(self, name, description, constraint, epath, inlist, status="stopped"):
        self.name = name
        self.description = description
        self.constraint = constraint 
        self.epath = epath
        self.inlist = inlist
        self.status = status

""" holds data about the function call signatures for running an operator
    as well as the function signatures for their corresponding input and output 
    function calls 
"""
class Signature:
    def __init__(self, input, output, run):
        self.input = input
        self.output = output
        self.run = run

"""   
    Classes for representing information about the user Workflow Description 
"""

""" Represents an input box from the workflow description """
class InputBox:

    def __init__(self, name, filename):
        self.name = name
        self.filename = filename

""" Represents an output box from the workflow description """
class OutputBox:

    def __init__(self, name, filename):
        self.name = name
        self.filename = filename

""" Represents an operator box from the workflow description """
class OperatorBox:

    def __init__(self, name, args, binput, boutput):
        self.name = name
        self.args = args
        self.binput = binput
        self.boutput = boutput

"""   
    Function to load workflow data from json into workflow classes
    and create and store a dictionary of instances of each player 
    (operator, input or output) by id.
"""
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
    Function to load materialised operator data from json into 
    materialised operator classes.
    Constraints on data types, engines, algorithms, inputs and outputs
    are recorded within the classes.
    Separate classes instances are created for:
    	Operators, inputs, outputs, typedefs,
 	function signatures, argument rules and 
	default argument values.
    These class instances define rules of use and are stored in a dictionary
    keyed by operator name, or operator/input/output name combination
    and in doing so associates these rules with materialised operators, inputs and
    outputs
"""
def loadOperatorLibraryData(data):
    for operator in data["operators"]:
        if operator["type"] == "dataset":
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
        elif operator["type"] == "signature_rule":
            newsignature = Signature(operator["input"], operator["output"], operator["run"])
            for algorithmName in operator["algorithm.names"]:
                g_signatureMap[algorithmName] = newsignature
        elif operator["type"] == "typedef":
            for algorithmName in operator["algorithm.names"]:
                for algorithmType in operator["algorithm.types"]:
                    g_typedefMap[tuple([algorithmName, algorithmType])] = operator["types"]
        elif operator["type"] == "inout_declaration":
            for algorithm in operator["algorithm.names"]:
		g_iodeclMap[tuple([algorithm,"input"])] = operator["input"]
		g_iodeclMap[tuple([algorithm,"output"])] = operator["output"]
        elif operator["type"] == "arg_declaration":
            for algorithm in operator["algorithm.names"]:
		g_argsdeclMap[algorithm] = operator["argTemplates"]
		g_argsdefaultsMap[algorithm] = operator["argDefaults"]
        else:
            print "Error"
 	    sys.exit



"""
        Miscellaneous functions
"""
def tabPrint(str, tabcount, f):
    f.write(' ' * tabcount*4 + str)

"""                              
        Beginning of main processing block
"""
def main(argv):
    workflowfile = ''
    codefile = ''
    liboperatorsfile = ''

    """  
	Read arguments 
    """
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
        Parse workflow description from workflow tool description
    """
    workflow = open(workflowfile, "r")
    flow = json.load(workflow)
    workflow.close()

    """ 
        Parse QUB's materialsed operators, including input output datasets
        and store all relevant information on datasets, operators, typedefs,
	declaration templates, signature rules, argument rules
    """
    liboperators = open(liboperatorsfile, "r")
    libdata = json.load(liboperators)
    liboperators.close()

    """ 
        Open the output codefile 
    """
    code = open(codefile, "w")

    """ 
        Load materialised operator data from json
    """
    loadOperatorLibraryData(libdata)

    """ 
        Load workflow data from json
    """
    loadWorkflowData(flow)

    """ DEBUG TRACE BLOCK
    print "-------------------------------- Operators -------------------------------------------- "
    pprint(g_operatorsMap)
    print "-------------------------------- Signatures -------------------------------------------- "
    pprint(g_signatureMap)
    print "-------------------------------- Typedefs -------------------------------------------- "
    pprint(g_typedefMap)
    print "-------------------------------- iodelcs -------------------------------------------- "
    pprint(g_iodeclMap)
    print "-------------------------------- arg decls -------------------------------------------- "
    print g_argsdeclMap
    print "-------------------------------- arg defaults -------------------------------------------- "
    print g_argsdefaultsMap
    print "-------------------------------- Datasets -------------------------------------------- "
    pprint(g_datasetsMap)
    print "-------------------------------- operator Boxes -------------------------------------------- "
    pprint(g_operatorBoxMap)
    print "-------------------------------- input Boxes -------------------------------------------- "
    pprint(g_inputBoxMap)
    print "-------------------------------- output Boxes -------------------------------------------- "
    pprint(g_outputBoxMap)
    """

    """ Set tab level for generated code """
    tabcount=0
    
    """ print start of main block """
    tabcount=1

    """ 
        Keep track of IO files declared so we can merge variables names so we have 1 var for the same filename
    """
    declaredIOFiles={}

    """ 
	START PRINTING TEMPLATE FILES 

        Template files contain predefined code sections for the operators
	with placeholders to be replaced with appriate data from the
	workflow and materialised operator descriptions.
     
        They are split into sections representing approximately 6 sections of
        an operators full code.

    """

    """   
        1st pass
        Loop all operators to get all header files required.  
        TODO remove duplicates headers when more than one operator
	in workflow
    """
    for box in flow["boxes"]:

        """ print materialised header template to code output"""
        if box["type"] == "operator":
            algName = g_operatorsMap[box["name"]].constraint.algname
            with open('templates/'+algName+'header.template', 'r') as myfile:
                data=myfile.read()
            myfile.close()
            tabPrint(data, 0, code)
            tabPrint ("\n", 0, code)

    """ 
        2nd pass
	Loop all operators to print variable declarations for input/output filenames
    """
    for box in flow["boxes"]:

        """ print materialised declarations from operator dictionary rules to code output"""
        if box["type"] == "operator":
            algName = g_operatorsMap[box["name"]].constraint.algname

            tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", 0, code)
            for iotype in ["input","output"]:
                declarationTemplate = g_iodeclMap[tuple([algName,iotype])]
                filename = eval('g_'+iotype+'BoxMap')[box[iotype][0]].filename
                inputId = box[iotype][0]
                outputId = box[iotype][0]
                datasetPath =  g_datasetsMap[eval('g_'+iotype+'BoxMap')[eval(iotype+'Id')].name].epath
    
                ctr=0
                if declarationTemplate is not None :
                        var = iotype+`ctr`
    
                        ## Keep track of io files for condensing vars, 
			## if it already exists use existing var handle
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
                        ctr += 1 
    tabPrint ("\n", 0, code)

    """ 
        3rd pass
	Loop all operators to print variable declarations for operator arguments
        read from args field in the operator box
    """
    for box in flow["boxes"]:

        if box["type"] == "operator":
            actual_args = box["args"]

            tabPrint ("//  Variable Declarations for operator arguments \n\n", 0, code)
            algName = g_operatorsMap[box["name"]].constraint.algname
            declDict = g_argsdeclMap[algName][0]
	    defaultsDict = g_argsdefaultsMap[algName][0]

	    # Create the declarations for actual supplied op arguments
	    for arg in actual_args.keys():
                if declDict.has_key(arg):
                   declarationTemplate=declDict[arg] 
                   declarationStr = declarationTemplate.replace("VARIN",arg) \
                                         .replace("VAROUT",actual_args[arg]) \
                                         + '\n'
                   tabPrint(declarationStr, 0, code)

                else:
                    print "Error: ", arg, " is not a valid argument to ", algName

	    # Now create the declarations for default operator attributes which
	    # which were not supplied by use in workflow description
            for arg in declDict.keys():
		if arg not in actual_args.keys():
                   declarationTemplate=declDict[arg] 
                   declarationStr = declarationTemplate.replace("VARIN",arg) \
                                         .replace("VAROUT",defaultsDict[arg]) \
                                         + '\n'
                   tabPrint(declarationStr, 0, code)


        else:
            continue

        tabPrint ("\n", 0, code)

    """  
        4th pass 
	Loop all operators to print 'main' including operator calling sequence
        code.  
        This uses a combination of code template files and what is stored in materialised
	operator rules
    """
    for box in flow["boxes"]:

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

            """ 
                output input section 
            """
            tabPrint ("//  Input section \n\n", tabcount, code)

            """ NOTE: for now assume we have 1 input to operator...
                Planning ahead--> if there are say 2 inputs, then there should be 
                2 FILE_PARAMX markers in the template file, in which case 
                this code section will be changed to loop for every marker, picking off the inputs of
                the operator - TODO """
            with open('templates/'+algName+'input.template', 'r') as myfile:
                data=myfile.read().replace('FILE_PARAM1',g_argMap[(algName,box["input"][0])]) # TODO when in loop, remove subscript
            myfile.close()
            tabPrint(data, tabcount, code)
            tabPrint ("\n", tabcount, code)

            """ 
                output operators call sequencec section 
            """
            tabPrint ("//  Calling sequence section \n\n", tabcount, code)
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

            """ 
                output output section 
            """
            tabPrint ("//  Output section \n\n", tabcount, code)
            with open('templates/'+algName+'output.template', 'r') as myfile:
                data=myfile.read().replace('OUTFILE',g_argMap[(algName,box["output"][0])]) # TODO when in loop, remove subscript
                myfile.close()
            tabPrint(data, tabcount, code)
            tabPrint ("\n", tabcount, code)

            """ 
                output close of main section 
            """
            tabPrint ("//  Closing main section \n\n", tabcount, code)
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
