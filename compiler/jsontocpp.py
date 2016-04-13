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
g_compEdges = []
g_nodeInEdges = {}
g_nodeOutEdges = {}

""" internal compiler maps """
g_optimisedForward = {}
g_optimisedBackward = {}


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

""" holds data about IO constraints """
class IOConstraint:
    def __init__(self, inputConstraint):
        self.fileSystem = inputConstraint["Engine"]["FS"]
        self.iotype = inputConstraint["type"]

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
            inputLocator = "Input" + str(i)
	    inputConstraint = IOConstraint(inputs["Input"+str(i)])
            self.inputConstraint.append(inputConstraint)
        for i in range(0,int(outputs["number"])):
	    outputConstraint = IOConstraint(outputs["Output"+str(i)])
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

    def __init__(self, nodeid, name, constraints):
        self.nodeid = nodeid
        self.name = name
        self.constraints = constraints
			  # We don't need array of inputs as the outer func loops inputs and
			  # we really want a different operator for each input.
        var=0

        """
	if computational is True:
	    if constraints["opSpecification"].get("algorithm"):
	        self.opSpecification = OpSpecification(constraints["opSpecification"]["algorithm"],
						       constraints["opSpecification"]["args"])
	    else:
	        self.opSpecification = OpSpecification(constraints["opSpecification"]["format"],
						   constraints["opSpecification"]["args"])

        numinputs = constraints["input"]
	print "numinputs is ", numinputs
        for i in range(0,int(numinputs)):
            newinputbox = InputBox(constraints["input"+str(i)])
            self.inputs.append(newinputbox)

        numoutputs = constraints["output"]
        for i in range(0,int(numoutputs)):
            newoutputbox = OutputBox(constraints["output"+str(i)])
            self.outputs.append(newoutputbox)
	"""


""" Represents a computational operator box from the workflow description """
class ComputationalOperatorBox(OperatorBox):

    def __init__(self, nodeid, name, constraints):
	OperatorBox.__init__(self, nodeid, name, constraints)

        self.opSpecification = OpSpecification(constraints["opSpecification"]["algorithm"],
						       constraints["opSpecification"]["args"])
	

""" Represents an input operator box from the workflow description """
class InputOperatorBox(OperatorBox):

    def __init__(self, nodeid, name, constraints, index):
	OperatorBox.__init__(self, nodeid, name, constraints)

	# self.opSpecification = OpSpecification(constraints["opSpecification"]["format"],
						   # constraints["opSpecification"]["args"])
	self.inputSpec = InputBox(constraints["input"+str(index)])

""" Represents an output operator box from the workflow description """
class OutputOperatorBox(OperatorBox):

    def __init__(self, nodeid, name, constraints, index):
	OperatorBox.__init__(self, nodeid, name, constraints)

        # self.opSpecification = OpSpecification(constraints["opSpecification"]["format"],
						   # constraints["opSpecification"]["args"])
	self.outputSpec = OutputBox(constraints["output"+str(index)])

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
    global g_compEdges
    g_edges = data["workflow"]["edges"]

    # Keep note of only computational edges for in-mem optimisations
    # g_compEdges = data["workflow"]["edges"]
    g_compEdges = list(g_edges)

    for box in data["workflow"]["nodes"]:
        newnode = Node(box["taskids"],
                       box["name"])
        g_nodeMap[box["id"]] = newnode

    for box in data["workflow"]["tasks"]:
        newoperatorbox = ComputationalOperatorBox( 
                                 box["nodeId"],
                                 box["name"],
                                 box["operator"]["constraints"]
				 )

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
	    newiooperatorbox = InputOperatorBox(newnodeid, newnodename,
					box["operator"]["constraints"],
					i)

	    """ Create input edge to original operator """
	    # g_edges.append(newnodeid, box["nodeId"]))
	    g_edges.append({'sourceId':newnodeid, 'targetId': box["nodeId"]})
	    if box["nodeId"] not in g_nodeInEdges:
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
	    newiooperatorbox = OutputOperatorBox(newnodeid, newnodename,
					box["operator"]["constraints"],
					i)

	    """ Create input edge to original operator """
	    # g_edges.append((box["nodeId"], newnodeid))
	    g_edges.append({'sourceId':box["nodeId"], 'targetId': newnodeid})
	    # g_nodeOutEdges[box["nodeId"]].append(newnodeid)
	    if box["nodeId"] not in g_nodeOutEdges:
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

headerSection=""
ioDeclSection=""
argsDeclSection=""
mainSection=""

"""
        Miscellaneous functions
"""
def collectCode(str, tabcount, section):
    global headerSection
    global ioDeclSection
    global argsDeclSection
    global mainSection

    globals()[section] += str


"""
        Miscellaneous functions
"""
def tabPrint(str, tabcount, f):
    f.write(' ' * tabcount*4 + str)

"""
        Process/Generate IO declaration and initialisation statements
"""
def createIODeclaration(ioEdge, ioId, declaredIOFiles, opcode, ioCount, algName, tabcount):

    algName = g_nodeMap[ioEdge].name
    ioConstraints = g_operatorsMap[g_nodeMap[ioEdge].name].constraint.inputConstraint
    ioType = ioConstraints[ioCount].iotype
    declarationTemplate = g_iodeclMap[tuple([ioType, ioId])]
    
    filename=""
    if ioId is "input": 
    	filename = g_nodeBoxMap[ioEdge].inputSpec.name 
    else:
    	filename = g_nodeBoxMap[ioEdge].outputSpec.name 
    
    ctr=0
    if declarationTemplate is not None :
	var = algName+"_"+ioId+str(ioCount)

	"""
	## Keep track of io files for condensing vars, 
	## if it already exists use existing var handle
	if declaredIOFiles.has_key(filename):
	    prevVar = declaredIOFiles[filename]
	    var = prevVar
	    print "var set to ", prevVar
	else:
	"""
	declaredIOFiles[filename]=var
	declarationStr = declarationTemplate.replace("VARIN", var) \
					.replace("VAROUT","\""+filename+"\"")
	collectCode(declarationStr+"\n", tabcount, "ioDeclSection")
	g_argMap[(algName,var)] = var
        ctr += 1 

"""
        Test if the 'key' node is forward in-memory optimisable
	and store a map of the edge so we can backwards check
	when processing the next computation operator if it is
	optimised/merged with this one
"""
def optimisableForward(key):

        for edge in g_compEdges:
            if edge['sourceId'] == key:
                destNode = edge['targetId']

                for i in range(0, len(g_nodeMap[key].taskids)):
                    ioCount = 0
                    for outEdge in g_nodeOutEdges[key]:
                        outFilename = g_nodeBoxMap[outEdge].outputSpec.name
                        ioCount = 0

                for i in range(0, len(g_nodeMap[destNode].taskids)):
                    ioCount = 0
                    for inEdge in g_nodeInEdges[destNode]:
                        inFilename = g_nodeBoxMap[inEdge].inputSpec.name
                        ioCount = 0

		g_optimisedForward[key] = destNode
		return outFilename == inFilename

	return False

"""
        Test if the 'key' node is backward in-memory optimisable
	and store a map of the edge so we can backwards check
	when processing the next computation operator if it is
	optimised/merged with this one
"""
def optimisableBackward(key):

        for edge in g_compEdges:
            if edge['targetId'] == key:
                srcNode = edge['sourceId']

                for i in range(0, len(g_nodeMap[srcNode].taskids)):
                    ioCount = 0
                    for outEdge in g_nodeOutEdges[srcNode]:
                        outFilename = g_nodeBoxMap[outEdge].outputSpec.name
                        ioCount = 0

                for i in range(0, len(g_nodeMap[key].taskids)):
                    ioCount = 0
                    for inEdge in g_nodeInEdges[key]:
                        inFilename = g_nodeBoxMap[inEdge].inputSpec.name
                        ioCount = 0

		g_optimisedBackward[srcNode] = key
		return outFilename == inFilename

	return False



"""                              
        Beginning of main processing block
"""
def main(argv):
    workflowfile = ''
    codefile = ''
    liboperatorsfile = ''
    # g_compEdges

    # mystr = " This was added"
    # collectCode(mystr, 0, "headerSection")

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
    print "-------------------------------- OutEdges -------------------------------------------- "
    pprint(g_nodeOutEdges)
    print "-------------------------------- InEdges -------------------------------------------- "
    pprint(g_nodeInEdges)
    print "-------------------------------- Comp Edges -------------------------------------------- "
    pprint(g_compEdges)
    print "-------------------------------- Edges -------------------------------------------- "
    pprint(g_edges)
    """

    """ Set tab level for generated code """
    tabcount=0
    
    """ 
        Keep track of IO files declared so we can merge variables names so we have 1 var for the same filename
    """
    declaredIOFiles={}

    """
	Output code filename.
	Build up code file name for joined operators, reset each time an optimisation flow ends
    """
    opcode = ""

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

	if isComputationalNode(key) is False:
	    # The computational operator will drive the compiler
	    continue

        """ 
	    Prepare the output filename for this operator's code.
	    This may involve concatenating operatornames if in-memory
	    optimisation was made.
        """

	# This logic will needs to change -  If we are carrying out a merge
	# then we actually want to rename the previous op code file
	# to a combination of oplast_and_opcurrent eg.
	# we could reaname oplast.cpp this new name structure
	# and open it here, appending the code in this iteration

   	genDir = os.path.splitext(workflowfile)[0]+'.dir'
	if not os.path.exists(genDir):
    		os.makedirs(genDir)

	if optimisableBackward(key) is False:
            # opcode = open(genDir+'/'+g_nodeMap[key].name+'.cpp', "w")
            opcode = g_nodeMap[key].name
	else:
	    opcode += "_"+g_nodeMap[key].name 

        """   
	    STAGE 1
            Print header files required.  
        """

	if optimisableBackward(key) is False:

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
    		collectCode(data+"\n", 0, "headerSection")
	else:
	    altFilename = 'templates/crossheaderim.template'
	    if os.path.isfile(altFilename):
                with open(altFilename, 'r') as myfile:
                    data=myfile.read()
		myfile.close()
    		collectCode(data+"\n", 0, "headerSection")

        """ 
            STAGE 2
	    Print variable declarations for input/output filenames
        """

        """ print materialised declarations from operator dictionary rules to code output"""
        algName = g_nodeMap[key].name

	collectCode("//  Variable Declarations holding input/output filenames  \n\n", 0, "ioDeclSection")

        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 task per node

            """ Input files IO """
	    if optimisableBackward(key) is False:
                ioCount = 0
	        for inEdge in g_nodeInEdges[key]:
		    createIODeclaration(inEdge, 'input', declaredIOFiles, opcode, ioCount, algName, tabcount)
		    ioCount += 1

            """ Output files IO """
	    if optimisableForward(key) is False:
                ioCount = 0
	        for outEdge in g_nodeOutEdges[key]:
		    createIODeclaration(outEdge, 'output', declaredIOFiles, opcode, ioCount, algName, tabcount)
		    ioCount += 1
    
	collectCode("\n", 0, "ioDeclSection")

        """ 
            STAGE 3
	    Print variable declarations for operator arguments
            read from args field in the operator box
        """
        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 node per task
	    collectCode("//  Variable Declarations for operator arguments \n\n", tabcount, "ioDeclSection")
            algName = g_nodeMap[key].name

            operatorBox = g_nodeBoxMap[key] 
            actual_args = operatorBox.opSpecification.args

	    if algName in g_argsdeclMap:
            	declDict = g_argsdeclMap[algName][0]
	    	defaultsDict = g_argsdefaultsMap[algName][0]

	        # Create the declarations for actual supplied op arguments
	        for arg in actual_args.keys():
                    if declDict.has_key(arg):
                        declarationTemplate=declDict[arg] 
                        declarationStr = declarationTemplate.replace("VARIN",arg) \
                                             .replace("VAROUT",actual_args[arg]) \
                                             + '\n'
	    	        collectCode(declarationStr, tabcount, "ioDeclSection")
        
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
	    	        collectCode(declarationStr, tabcount, "ioDeclSection")
                    else:
                        continue

	collectCode("\n", 0, "ioDeclSection")

        """  
            STAGE 4
	    Print 'main' including operator calling sequence code.  
            This uses a combination of code template files and what is stored in materialised
	    operator rules
        """

        ctr=0
        for i in range(0,len(g_nodeMap[key].taskids)):  # Should only be once if 1 node per task

	    if optimisableBackward(key) is False:

                algName = g_nodeMap[key].name

                """ 
                    output start of main section 
                """
		collectCode("//  Start of main section \n\n", 0, "mainSection")
	        algStructType = g_operatorsMap[g_nodeMap[key].name].constraint.algtype
                with open('templates/'+algName+'maindeclarations.template', 'r') as myfile:
                    data=myfile.read()
                myfile.close
		collectCode(data, 0, "mainSection")

    		tabcount=1

    
		collectCode("//  Start of typedefs section \n\n", tabcount, "mainSection")
                typedefs = g_typedefMap[tuple([algName, algStructType])]
                for typedef in typedefs:
		    collectCode(typedef+"\n", tabcount, "mainSection")
		collectCode("\n", tabcount, "mainSection")

            """
                output 'input' section
            """
	    # Only do this input section if we haven't identified an 'in-memory' merge from 
	    # output of previous op
	    # otherwise the earlier assigned variable (which holds the output from the 
	    # previous operator) is effectively the input section 
	    # in which case we can either re-assign the input variable or
	    # change the name of the input variable to the earlier assignment in question

	    collectCode("//  Input section if not optimised \n\n", tabcount, "mainSection")

	    if optimisableBackward(key) is False:

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
                        inputId = algName+'_input'+str(inputParam)
                        data=data.replace('FILE_PARAM'+str(inputParam+1),g_argMap[(algName,inputId)]) 
                myfile.close()
	        collectCode(data+"\n", tabcount, "mainSection")
	    else:
		altFilename = 'templates/'+algName+'inputim.template'
		if os.path.isfile(altFilename):
                    with open(altFilename, 'r') as myfile:
                        data=myfile.read()
		    myfile.close()
	            collectCode(data+"\n", tabcount, "mainSection")

            """ 
                print code output for the operators call sequence section 
            """
	    collectCode("//  Calling sequence section \n\n", tabcount, "mainSection")
            with open('templates/'+algName+'_'+'callsequence.template', 'r') as myfile:
                
                # data=myfile.read().replace('WORD_TYPE',eval('g_'+algName).dataStructType)
                data=myfile.read().replace('WORD_TYPE',algStructType)
                myfile.close()
                if 'CATALOG_BUILD_CODE' in data:
                    with open('templates/'+algName+'_'+algStructType+'_catalogbuild.template', 'r') as myfile:
                        catalogBuildCode=myfile.read()
                        myfile.close()
                    data=data.replace('CATALOG_BUILD_CODE',catalogBuildCode)
	    collectCode(data, tabcount, "mainSection")

            """ 
                output 'output' section if not forward optimised
            """
	    # We will only do output and 'close of main' if we haven't identified an in-memory merge 
	    # situation.  Otherwise we should record the variable name which holds the output for 
	    # replacement of the next input variable

	    if optimisableForward(key) is False:
	        collectCode("//  Output section \n\n", tabcount, "mainSection")
                paramInfo = g_operatorsMap[g_nodeMap[key].name].constraint.outputConstraint
                numParams = len(paramInfo)
                with open('templates/'+algName+'output.template', 'r') as myfile:
                    data=myfile.read()
                    for outputParam in range(0,numParams):
                        outputId = algName+'_output'+str(outputParam)
                        data=data.replace('FILE_PARAM'+str(outputParam+1),g_argMap[(algName,outputId)]) 

                myfile.close()
	        collectCode(data+"\n", tabcount, "mainSection")

                """ 
                    output close of main section 
                """
	        collectCode("//  Closing main section \n\n", tabcount, "mainSection")
                with open('templates/'+algName+'mainclose.template', 'r') as myfile:
                    data=myfile.read()
                    myfile.close()
	        collectCode(data, tabcount, "mainSection")

		# Print the code to file
                codeFile = open(genDir+'/'+opcode+'.cpp', "w")
		tabPrint(headerSection, 0, codeFile)
		tabPrint(ioDeclSection, 0, codeFile)
		tabPrint(argsDeclSection, 0, codeFile)
		tabPrint(mainSection, 0, codeFile)
                codeFile.close()

	# We may not want to close this code file if we have identified a merge-optimisation
	# Though we may rename it op1_and_op2, so depends on the final solution for handling
	# this situation

        # opcode.close()


"""
    Call main with passed params
"""
if __name__ == "__main__":
   main(sys.argv[1:])
