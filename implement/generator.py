import subprocess
import re
import sys
from readinput import print_results, read_from_file, mf_structure

'''
For the file inputs of mf_example, i'm also going to say that the SUCHTHAT
needs to have spaces between characters, and this is how format_conditionals() operates.
'''

def format_conditionals(r):
    '''
    Formats the such-that of our r clause into python-readable form
    this one assumes suchthat clause, in the format of 'NUMBER.attribute = something'

    Noticed that the queries are in MF style, no "x.cust = cust" yet, and the aggregate
    calculations seems to occur MF style
    '''
    tmp = []
    for st in list(r):
        tmpstring = (re.sub(r'(?<![<>=!])=(?![=])', '==', st))  # if we have an isolated =, turn it to ==
        tmpstring = tmpstring.split(" ")
        first = int(tmpstring[0].split(".")[0])
        attr = tmpstring[0].split(".")[1]
        if first - 1 in range(len(r)):
            # Groupvar should always agree because it'll always run according to the grouping var it's on, but it's here anyway
            tmp.append("groupVar == " + str(first - 1) + " and " + "(row[ATTRIBUTE_INDEX['" + attr + "']]) == " + str(tmpstring[2]))
        else:
            # in case a conditional falls outside the number of grouping vars
            raise Exception("format_conditionals: " + str(first - 1) + " not in range.")
    
    final = tmp[0]
    for c in tmp[1:]:
        final += " or " + c
    return final # this was MUCH faster than using eval()


def format_conditionals2(data, listAggVars, combineList):
    tempList = []
    for x in combineList:
        for y in x:
            tempList.append(y[0] + "_" + y[1] + "_"+ y[2])
    #print(tempList)
    
    temp = data
    for substring in tempList:
        if substring in temp:
            temp = temp.replace(substring, "rowDict[uniqueID]['" + substring + "']")
            #print(temp)
    return temp

def main():
    file = sys.argv[1] # read argument
    MF_str = (read_from_file(file))
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    selectAtts = MF_str.S
    listAggVars = MF_str.V #this is v
    numGroupVars = MF_str.n # this is n
    f = MF_str.F #this is f
    suchthat = MF_str.r
    having = MF_str.G
    condList = format_conditionals(suchthat)
    condList2 = format_conditionals2(having, listAggVars, f)

    body = f"""
    listAggVars = {listAggVars}
    numGroupVars = {numGroupVars}
    suchthat = {suchthat}
    # having = {str(having)}
    rowDict = {{}}  # renamed to rowDict from dict
    #dataDict = {{}} just data for the keys not the final value

    ATTRIBUTE_INDEX = {{  # use this to get the index of the database's attributes
        'cust': 0,
        'prod': 1,
        'day': 2,
        'month': 3,
        'year': 4,
        'state': 5,
        'quant': 6,
        'date': 7,
    }}

    
    for row in cur: # loop to put all keys in dictionary
        #_global.append(row)
        uniqueID = "" # this is gonna be a combination of the agg vars for the rowDict key
        for aggVar in listAggVars:
            uniqueID = uniqueID + row[aggVar]
        rowDict[uniqueID] = {{}}

        grpList = []     # list of the grouping variables' indices
        for get_var in listAggVars:
            grpList.append(ATTRIBUTE_INDEX[get_var])    # turn them into indices so we can pass them over to the aggregates
            rowDict[uniqueID][get_var] = row[ATTRIBUTE_INDEX[get_var]]
            # print(grpList)
        
        # for index in grpList:   # grouping vars
        #     # print(str(index) + ": ADD " + str(row[index]))
        #     rowDict[uniqueID][row[index]] = row[index]   # replaced with index

        for groupVar in range(numGroupVars):   # for group in n
            for agg in {f}[groupVar]:
                if agg[1] == "count":
                    rowDict[uniqueID][str(groupVar + 1) + "_count_" + agg[2]] = 0
                if agg[1] == "sum":
                    rowDict[uniqueID][str(groupVar + 1) + "_sum_" + agg[2]] = 0
                if agg[1] == "max":
                    rowDict[uniqueID][str(groupVar + 1) + "_max_" + agg[2]] = 0
                if agg[1] == "min":
                    rowDict[uniqueID][str(groupVar + 1) + "_min_" + agg[2]] = 0
                if agg[1] == "avg":
                    rowDict[uniqueID][str(groupVar + 1) + "_sumAvg_" + agg[2]] = 0
                    rowDict[uniqueID][str(groupVar + 1) + "_countAvg_" + agg[2]] = 0

                                        
    cur.execute("SELECT * FROM sales")


    
    for groupVar in range(numGroupVars): #loop, for group in n, to calculate all the aggregates
        for row in cur:
            uniqueID = "" # this is gonna be a combination of the agg vars for the rowDict key
            for aggVar in listAggVars:
                uniqueID = uniqueID + row[aggVar]

            for agg in {f}[groupVar]:
                if {condList}:   # where the conditional happens, grouped up by grouping vars
                    if agg[1] == "count":
                        rowDict[uniqueID][str(groupVar + 1) + "_count_" + agg[2]] += 1
                    if agg[1] == "sum":
                        rowDict[uniqueID][str(groupVar + 1) + "_sum_" + agg[2]] += row[agg[2]]
                    if agg[1] == "max":
                        if(row[agg[2]] > rowDict[uniqueID][str(groupVar + 1) + "_max_" + agg[2]]):
                            rowDict[uniqueID][str(groupVar + 1) + "_max_" + agg[2]] = row[agg[2]]
                    if agg[1] == "min":
                        if(row[agg[2]] < rowDict[uniqueID][str(groupVar + 1) + "_min_" + agg[2]]):
                            rowDict[uniqueID][str(groupVar + 1) + "_min_" + agg[2]] = row[agg[2]]
                    if agg[1] == "avg":
                        rowDict[uniqueID][str(groupVar + 1) + "_sumAvg_" + agg[2]] += row[agg[2]]
                        rowDict[uniqueID][str(groupVar + 1) + "_countAvg_" + agg[2]] += 1

        cur.execute("SELECT * FROM sales")
        

    for groupVar in range(numGroupVars): #loop to calculate avg and clean up average in rowDict`
        for agg in {f}[groupVar]:
            for uniqueID in list(rowDict.keys()):
                if agg[1] == "avg":
                    rowDict[uniqueID][str(groupVar + 1) + "_avg_" + agg[2]] =  rowDict[uniqueID][str(groupVar + 1) + "_sumAvg_" + agg[2]] / rowDict[uniqueID][str(groupVar + 1) + "_countAvg_" + agg[2]]
                    del rowDict[uniqueID][str(groupVar + 1) + "_sumAvg_" + agg[2]]
                    del rowDict[uniqueID][str(groupVar + 1) + "_countAvg_" + agg[2]]
    for groupVar in range(numGroupVars): # have to run a second series of loops to allow averages to be computed first before having
        for agg in {f}[groupVar]:  
            for uniqueID in list(rowDict.keys()): 
                #print(list(rowDict[uniqueID].keys()))
                if not ({condList2}):
                    #print("it happened")
                    del rowDict[uniqueID]
    for groupVar in range(numGroupVars): # NEED ANOTHER SEREIS OF LOOP FOR NO ERROR
        for agg in {f}[groupVar]:  
            for uniqueID in list(rowDict.keys()): 
                if not ((str(groupVar + 1) + "_count_" + agg[2]) in {selectAtts}):
                    (rowDict[uniqueID]).pop(str(groupVar + 1) + "_count_" + agg[2], None)
                if not ((str(groupVar + 1) + "_sum_" + agg[2]) in {selectAtts}):
                    (rowDict[uniqueID]).pop(str(groupVar + 1) + "_sum_" + agg[2], None)
                if not ((str(groupVar + 1) + "_max_" + agg[2]) in {selectAtts}):
                    (rowDict[uniqueID]).pop(str(groupVar + 1) + "_max_" + agg[2], None)
                if not ((str(groupVar + 1) + "_min_" + agg[2]) in {selectAtts}):
                    (rowDict[uniqueID]).pop(str(groupVar + 1) + "_min_" + agg[2], None)
                if not ((str(groupVar + 1) + "_avg_" + agg[2]) in {selectAtts}):
                    (rowDict[uniqueID]).pop(str(groupVar + 1) + "_avg_" + agg[2], None)
    print(rowDict.keys())

    
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}

    
    # test of sorting to compare with sql
    the_keys = list(rowDict.keys())
    the_keys.sort()
    sd = {{i: rowDict[i] for i in the_keys}}

    for x in list(sd.values()):
        _global.append(list(x.values()))
    
    return tabulate.tabulate(_global,
                        headers=list(sd[list(sd.keys())[0]].keys()), tablefmt="postgres")

def main():
    print(query())
    #query()
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    #subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
