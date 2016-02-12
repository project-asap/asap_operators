#!/usr/bin/python
"""
	This program translates a high level workflow description in json to swan codes.

	The code can be categorised into sections for aid of readability:

		Definitions of dictionaries used to associate rules of:
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
import os



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
g_inputNodeMap = {}
g_outputNodeMap = {}
g_inputBoxMap = {}
g_outputBoxMap = {}
g_operatorBoxMap = {}
g_nodeBoxMap = {}
g_nodeMap = {}
g_argMap = {}
g_edges = []
g_nodeInEdges = {}
g_nodeOutEdges = {}

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

""" holds data about IO constraints """
class IOConstraint:
    def __init__(self, fileSystem, iotype):
        self.fileSystem = fileSystem
        self.iotype = iotype

""" holds data about operator constraints """
class OperatorConstraint:
    def __init__(self, fs, runFile, algname, alg_dstructtype, inputs, outputs):
        self.EngineSpecification_FS = fs
        self.runFile = runFile
        self.algname = algname
        self.algtype = alg_dstructtype
	self.inputConstraint = []
	self.outputConstraint = []
        for i in range(0,int(inputs["number"])):
	    inputConstraint = IOConstraint(inputs["Engine"]["FS"],
                                               inputs["type"])
            self.inputConstraint.append(inputConstraint)
        for i in range(0,int(outputs["number"])):
	    outputConstraint = IOConstraint(outputs["Engine"]["FS"],
                                               outputs["type"])
            self.outputConstraint.append(outputConstraint)

""" holds data about operators """
class Operator:

    def __init__(self, name, description, constraint, inlist, status="stopped"):
        self.name = name
        self.description = description
        self.constraint = constraint
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

    def __init__(self, filename):
        self.name = filename
        # self.itype = itype

""" Represents an output box from the workflow description """
class OutputBox:

    def __init__(self, filename):
        self.name = filename
        # self.otype = otype

class OpSpecification:

    def __init__(self, algorithm, args):
	self.algorithm = algorithm
	self.args = args

""" Represents an operator box from the workflow description """
class OperatorBox:

    def __init__(self, nodeid, name, constraints, computational):
        self.nodeid = nodeid
        self.name = name
        self.constraints = constraints
        self.inputs = []
        self.outputs = []
        var=0

	if computational is True:
	    if constraints["opSpecification"].get("algorithm"):
	        self.opSpecification = OpSpecification(constraints["opSpecification"]["algorithm"],
						       constraints["opSpecification"]["args"])
	    else:
	        self.opSpecification = OpSpecification(constraints["opSpecification"]["format"],
						   constraints["opSpecification"]["args"])

        numinputs = constraints["input"]
        for i in range(0,int(numinputs)):
            newinputbox = InputBox(constraints["input"+str(i)])
            self.inputs.append(newinputbox)

        numoutputs = constraints["output"]
        for i in range(0,int(numoutputs)):
            newoutputbox = OutputBox(constraints["output"+str(i)])
            self.outputs.append(newoutputbox)

""" Represents a node operator box from the workflow description """
class Node:

    def __init__(self, taskids, name):
        self.taskids = taskids
        self.name = name
""" Represents an edge from one node to another from the workflow description """
class Edge:

    def __init__(self, src, dst):
        self.src = src
        self.dst = dst


"""   
    USES UNIGE's WORKFLOW DESCRIPTION FORMAT
    Function to load workflow data from json into workflow classes
    and create and store a dictionary of instances of each player 
    (operator, input or output) by id.
"""
def loadWorkflowData(data):

    global g_edges
    g_edges = data["workflow"]["edges"]

    for box in data["workflow"]["nodes"]:
        newnode = Node(box["taskids"],
                       box["name"])
        g_nodeMap[box["id"]] = newnode

    for box in data["workflow"]["tasks"]:
        newoperatorbox = OperatorBox( 
                                 box["nodeId"],
                                 box["name"],
                                 box["operator"]["constraints"],
				 True)

	"""
		Here we are transforming Unige's workflow so each input/output is an 
		oprator in its own right.  This makes it possible/easier to 
		identify in-memory workflow optimisations
		TODO - merge input and output processing, so 1 code section
	"""
        numinputs = box["operator"]["constraints"]["input"]
        for i in range(0,int(numinputs)):
            # newinputbox = InputBox(box["operator"]["constraints"]["input"+str(i)])
            # self.inputs.append(newinputbox)

	    """ Create new nodes for this input """
	    newnodeid = 'i_'+box["id"]+'_'+str(i) 
	    newtaskids = [newnodeid]
	    newnodename = box["name"]
            newnode = Node(newtaskids, newnodename)
            g_nodeMap[newnodeid] = newnode

	    """ Create new task for this input """
	    newiooperatorbox = OperatorBox(newnodeid, newnodename,
					box["operator"]["constraints"],
					False)

	    """ Create input edge to original operator """
	    g_edges.append((newnodeid, box["nodeId"]))
	    if not g_nodeInEdges:
		g_nodeInEdges[box["nodeId"]] = [newnodeid]
	    else:
		g_nodeInEdges[box["nodeId"]].append(newnodeid)
		
            g_nodeBoxMap[newnodeid] = newiooperatorbox

        numoutputs = box["operator"]["constraints"]["output"]
        for i in range(0,int(numoutputs)):

	    """ Create new nodes for this output """
	    newnodeid = 'o_'+box["id"]+'_'+str(i) 
	    newtaskids = [newnodeid]
	    newnodename = box["name"]
            newnode = Node(newtaskids, newnodename)
            g_nodeMap[newnodeid] = newnode

	    """ Create new task for this output """
	    newiooperatorbox = OperatorBox(newnodeid, newnodename,
					box["operator"]["constraints"],
					False)

	    """ Create input edge to original operator """
	    g_edges.append((box["nodeId"], newnodeid))
	    if not g_nodeOutEdges:
		g_nodeOutEdges[box["nodeId"]] = [newnodeid]
	    else:
		g_nodeOutEdges[box["nodeId"]].append(newnodeid)

            g_nodeBoxMap[newnodeid] = newiooperatorbox

        # newinputbox = InputBox(box["operator"]["constraints"]["input0"])
        # newoutputbox = OutputBox(box["operator"]["constraints"]["output0"])

        g_nodeBoxMap[box["nodeId"]] = newoperatorbox


"""
    create a new node and task, with id same as op's node id and task id but with
    iX suffix (where X identifies which i/o the new node 
    represents to/from the op (in the where case the op has multiple inputs).
    Add the input specification to the new i/o operator node
"""
def createIOOperatorNode(origNode, origTaskId):


    return newNode
	
""" Insert a node edge going from this new i/o op to the original op """
def insertNewEdge(newOpNode, origNode):

    return null

""" Test for whether or not operator node is computational or input/output """
def isComputationalNode(key):

    task = g_nodeBoxMap[key]
    if hasattr(task, "opSpecification"): 
	return True
    else:
	return False

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
            newconstraint = DataSetConstraint(operator["Constraints"]["DataInfo"]["type"], 
                                             operator["Constraints"]["Engine"]["FS"],
					     operator["Constraints"]["type"])
            newdataset = Dataset(operator["name"], 
                                             operator["description"], 
                                             newconstraint, 
                                             operator["Execution"]["path"], 
                                             operator["input"], 
                                             operator["status"])
            g_datasetsMap[operator["name"]] = newdataset
        elif operator["type"] == "operator":
            newconstraint = OperatorConstraint(
                                             operator["Constraints"]["EngineSpecification"]["FS"],
					     operator["Constraints"]["runFile"],
					     operator["Constraints"]["Algorithm"]["name"],
					     operator["Constraints"]["Algorithm"]["dstruct_type"],
                                             operator["Constraints"]["Input"],
                                             operator["Constraints"]["Output"])

            newoperator = Operator(operator["name"], 
                                             operator["description"], 
                                             newconstraint, 
                                             operator["status"])
            g_operatorsMap[operator["name"]] = newoperator
            g_operatorsMap[operator["Constraints"]["Algorithm"]["name"]] = newoperator
        elif operator["type"] == "signature_rule":
            newsignature = Signature(operator["input"], operator["output"], operator["run"])
            for algorithmName in operator["algorithm.names"]:
                g_signatureMap[algorithmName] = newsignature
        elif operator["type"] == "typedef":
            for algorithmName in operator["algorithm.names"]:
                for algorithmType in operator["algorithm.types"]:
                    g_typedefMap[tuple([algorithmName, algorithmType])] = operator["types"]
        elif operator["type"] == "inout_declaration":
            for dataset in operator["dataset.names"]:
		g_iodeclMap[tuple([dataset,"input"])] = operator["input"]
		g_iodeclMap[tuple([dataset,"output"])] = operator["output"]

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
        Process/Generate IO declaration and initialisation statements
"""
def createIODeclaration(inEdge, ioId, declaredIOFiles, opcode, ioCount):

    algName = g_nodeMap[inEdge].name
    ioConstraints = g_operatorsMap[g_nodeMap[inEdge].name].constraint.inputConstraint
    ioType = ioConstraints[ioCount].iotype
    declarationTemplate = g_iodeclMap[tuple([ioType, ioId])]
    
    filename=""
    if ioId is "input": 
    	filename = g_nodeBoxMap[inEdge].inputs[ioCount].name 
    else:
    	filename = g_nodeBoxMap[inEdge].outputs[ioCount].name 
    
    ctr=0
    if declarationTemplate is not None :
	var = ioId+str(ioCount)

	## Keep track of io files for condensing vars, 
	## if it already exists use existing var handle
	if declaredIOFiles.has_key(filename):
	    prevVar = declaredIOFiles[filename]
	    var = prevVar
	else:
	    declaredIOFiles[filename]=var
	    declarationStr = declarationTemplate.replace("VARIN", var) \
						.replace("VAROUT","\""+filename+"\"")
	    tabPrint (declarationStr, 0, opcode)
	    tabPrint ("\n", 0, opcode)
	g_argMap[(algName,var)] = var
	print "created argMap with ", algName, " and ", var
        ctr += 1 


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
    # we now write the code to individual source code files whose names ar
    # generated from a derivitave of algorithm (and possibly to be datastructure)
    # code = open(codefile, "w")

    """ 
        Load materialised operator data from json
    """
    loadOperatorLibraryData(libdata)

    """ 
        Load workflow data from json
    """
    loadWorkflowData(flow)

    # splitIOFromNodeTasks()

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
    print "-------------------------------- Edges -------------------------------------------- "
    pprint(g_edges)
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

    """ For each node(task) in the workflow """
    for key in g_nodeMap:

	print "1key is ", key
	if isComputationalNode(key) is False:
	    # The computational operator will drive the compiler
	    continue

        """ 
            Open the output file for this operator 
        """

	# This logic will needs to change -  If we are carrying out a merge
	# then we actually want to rename the previous op code file
	# to a combination of oplast_and_opcurrent eg.
	# we could reaname oplast.cpp this new name structure
	# and open it here, appending the code in this iteration

   	genDir = os.path.splitext(workflowfile)[0]+'.dir'
	if not os.path.exists(genDir):
    		os.makedirs(genDir)
        opcode = open(genDir+'/'+g_nodeMap[key].name+'.cpp', "w")

        """   
	    STAGE 1
            Print header files required.  
        """

        # ASSUMPTION
        # We should only ever have one task per node as Unige 
        # documents state they optimise the workflow to make tasks nodes(vertices) in 
        # their own right ?  TODO fix if this turns out not to be the case
        # Either way we will still loop round the tasks here for extensibility 

        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 node 

            task = g_nodeMap[key].taskids[i]

            """ print materialised header template to code output"""
            algName = g_nodeMap[key].name
            with open('templates/'+algName+'header.template', 'r') as myfile:
                data=myfile.read()
            myfile.close()
            tabPrint(data, 0, opcode)
       
            tabPrint ("\n", 0, opcode)

        """ 
            STAGE 2
	    Print variable declarations for input/output filenames
        """

        """ print materialised declarations from operator dictionary rules to code output"""
        algName = g_nodeMap[key].name

        tabPrint ("//  Variable Declarations holding input/output filenames  \n\n", 0, opcode)


        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 task per node

            """ Input files IO """

            ioCount = 0
	    for inEdge in g_nodeInEdges[key]:

		createIODeclaration(inEdge, 'input', declaredIOFiles, opcode, ioCount)
		ioCount += 1

            ioCount = 0
	    for outEdge in g_nodeOutEdges[key]:

		createIODeclaration(outEdge, 'output', declaredIOFiles, opcode, ioCount)
		ioCount += 1

        tabPrint ("\n", 0, opcode)

        """ 
            STAGE 3
	    Print variable declarations for operator arguments
            read from args field in the operator box
        """
        # for key in g_nodeMap:


        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 node per task
            tabPrint ("//  Variable Declarations for operator arguments \n\n", 0, opcode)
            algName = g_nodeMap[key].name

            operatorBox = g_nodeBoxMap[key] 
            actual_args = operatorBox.opSpecification.args

            declDict = g_argsdeclMap[algName][0]
	    defaultsDict = g_argsdefaultsMap[algName][0]

	    # Create the declarations for actual supplied op arguments
	    for arg in actual_args.keys():
                if declDict.has_key(arg):
                   declarationTemplate=declDict[arg] 
                   declarationStr = declarationTemplate.replace("VARIN",arg) \
                                         .replace("VAROUT",actual_args[arg]) \
                                         + '\n'
                   tabPrint(declarationStr, 0, opcode)
    
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
                   tabPrint(declarationStr, 0, opcode)
                else:
                    continue

        tabPrint ("\n", 0, opcode)

        """  
            STAGE 4
	    Print 'main' including operator calling sequence code.  
            This uses a combination of code template files and what is stored in materialised
	    operator rules
        """

        ctr=0
        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 node per task

            algName = g_nodeMap[key].name

            """ 
                output start of main section 
            """
            tabPrint ("//  Start of main section \n\n", 0, opcode)
	    algStructType = g_operatorsMap[g_nodeMap[key].name].constraint.algtype
            with open('templates/'+algName+'maindeclarations.template', 'r') as myfile:
                data=myfile.read()
            myfile.close
            tabPrint(data, 0, opcode)

            tabPrint ("//  Start of typedefs section \n\n", tabcount, opcode)
            typedefs = g_typedefMap[tuple([algName, algStructType])]
            for typedef in typedefs:
                tabPrint(typedef, tabcount, opcode)
                tabPrint ("\n", tabcount, opcode)
            tabPrint ("\n", tabcount, opcode)

            """
                output input section
            """
	    # Only do this input section if we haven't identified an 'in-memory' merge from 
	    # output of previous op
	    # otherwise the earlier assigned variable (which holds the output from the 
	    # previous operator) is effectively the input section 
	    # in which case we can either re-assign the input variable or
	    # change the name of the input variable to the earlier assignment in question

            tabPrint ("//  Input section \n\n", tabcount, opcode)

            """ NOTE: for now assume we have 1 input to operator...
                Planning ahead--> if there are say 2 inputs, then there should be 
                2 FILE_PARAMX markers in the template file, in which case 
                this code section will be changed to loop for every marker, picking off the inputs of
                the operator - TODO """

            paramInfo = g_operatorsMap[g_nodeMap[key].name].constraint.inputConstraint
            numParams = len(paramInfo)
            with open('templates/'+algName+'input.template', 'r') as myfile:
                data=myfile.read()
                for inputParam in range(0,numParams):
                    inputId = 'input'+str(inputParam)
                    data=data.replace('FILE_PARAM'+str(inputParam+1),g_argMap[(algName,inputId)]) 
            myfile.close()
            tabPrint(data, tabcount, opcode)
            tabPrint ("\n", tabcount, opcode)

            """ 
                print to code output the operators call sequencec section 
            """
            tabPrint ("//  Calling sequence section \n\n", tabcount, opcode)
            with open('templates/'+algName+'_'+'callsequence.template', 'r') as myfile:
                
                # data=myfile.read().replace('WORD_TYPE',eval('g_'+algName).dataStructType)
                data=myfile.read().replace('WORD_TYPE',algStructType)
                myfile.close()
                if 'CATALOG_BUILD_CODE' in data:
                    with open('templates/'+algName+'_'+algStructType+'_catalogbuild.template', 'r') as myfile:
                        catalogBuildCode=myfile.read()
                        myfile.close()
                    data=data.replace('CATALOG_BUILD_CODE',catalogBuildCode)
            tabPrint(data, tabcount, opcode)

            """ 
                output 'output' section 
            """
	    # We will only do this if we haven't identified an in-memory merge situation
 	    # Otherwise we should record the variable name which holds the output for 
	    # replacement of the next input variable

            tabPrint ("//  Output section \n\n", tabcount, opcode)
            paramInfo = g_operatorsMap[g_nodeMap[key].name].constraint.outputConstraint
            numParams = len(paramInfo)
            with open('templates/'+algName+'output.template', 'r') as myfile:
                data=myfile.read()
                for outputParam in range(0,numParams):
                    outputId = 'output'+str(outputParam)
                    data=data.replace('FILE_PARAM'+str(outputParam+1),g_argMap[(algName,outputId)]) 

            myfile.close()
            tabPrint(data, tabcount, opcode)
            tabPrint ("\n", tabcount, opcode)

            """ 
                output close of main section 
            """
            tabPrint ("//  Closing main section \n\n", tabcount, opcode)
            with open('templates/'+algName+'mainclose.template', 'r') as myfile:
                data=myfile.read()
                myfile.close()
            tabPrint(data, tabcount, opcode)

	# We don't want to close this code file if we have identified a merge-optimisation
	# Though we may rename it op1_and_op2

        opcode.close()

"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
