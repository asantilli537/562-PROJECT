import subprocess
import re
import sys
from readinput import print_results, read_from_file, read_from_input, mf_structure

'''
For the file inputs of mf_example, i'm also going to say that the SUCHTHAT
needs to have spaces between characters, and this is how format_conditionals() operates.
'''


def format_suchthat(suchthat, F_list, attrs):
    '''
    Format the suchthat list into a conditional to be processed.

    '''
    print("SUCHTHAT: " + str(suchthat))
    finalList = ""
    tmplist = []
    condlist = []

    aggList = return_aggregates(F_list)

    for st in list(suchthat): # for every string in the suchthat list
        tmpstring = (re.sub(r'(?<![<>=!])=(?![=])', '==', st)) # turn any isolated = into ==
        condstring = tmpstring.split(" ")
        othertmp = []
        for substring in condstring:
            if "." in substring:
                first = int(substring.split(".")[0])
                attr = substring.split(".")[1]
                substring = "row[ATTRIBUTE_INDEX['" + attr + "']]"
            else:
                if substring in attrs:
                    substring = "rowDict[uniqueID]['" + substring + "']"
            othertmp.append(substring)
            finalstr = " ".join(othertmp)
            print("FINALSTR: " + finalstr)
            condlist.append(finalstr)


        for aggName in aggList:
            if aggName in st:
                st = st.replace(substring, "rowDict[uniqueID]['" + substring + "']")
        
        tmpstring = "groupVar == " + str(first - 1) + " and (" + finalstr + ")"
        tmplist.append(tmpstring)

    finalList = tmplist[0]
    for c in tmplist[1:]:
        finalList += " or " + c

    return finalList

def return_aggregates(F):

    '''
    since this function is used twice I'm just gonna extract it here
    formats the aggregates back into their original strings (1_sum_quant)
    '''
    tempList = []
    for x in F:
        for y in x:
            tempList.append(y[0] + "_" + y[1] + "_"+ y[2])
    return tempList

def format_having(data, listAggVars, F_list):
    # takes having clause and F list
    if data == None:
        # print("NO HAVING HERE")
        return True  # if there's no having, skip checking it
    tempList = return_aggregates(F_list)
    
    temp = data
    for substring in tempList:
        if substring in temp:
            temp = temp.replace(substring, "rowDict[uniqueID]['" + substring + "']")
            #print(temp)
    return temp

def main():
    file = sys.argv[1] # read argument
    if file == 'INPUT':
        MF_str = read_from_input()
    else:
        MF_str = (read_from_file(file))
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    ATTRIBUTE_INDEX = {  # use this to get the index of the database's attributes
        'cust': 0,
        'prod': 1,
        'day': 2,
        'month': 3,
        'year': 4,
        'state': 5,
        'quant': 6,
        'date': 7,
    }
    selectAtts = MF_str.S
    listAggVars = MF_str.V #this is v
    numGroupVars = MF_str.n # this is n
    f = MF_str.F #this is f
    suchthat = MF_str.r
    print("SUCHTHAT: " + str(suchthat))
    having = MF_str.G
    suchThatList = format_suchthat(suchthat, f, list(ATTRIBUTE_INDEX.keys()))
    print("FULL SUCHTHAT: " + str(suchThatList))
    havingList = format_having(having, listAggVars, f)

    body = f"""
    listAggVars = {listAggVars}
    numGroupVars = {numGroupVars}
    suchthat = {suchthat}
    # having = {str(having)}
    rowDict = {{}}  # renamed to rowDict from dict
    #dataDict = {{}} just data for the keys not the final value
    ATTRIBUTE_INDEX = {ATTRIBUTE_INDEX}

    
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
                if {suchThatList}:   # where the conditional happens, grouped up by grouping vars
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
                print(list(rowDict[uniqueID].keys()))
                if not ({havingList}):
                    print("it happened")
                    del rowDict[uniqueID]
            
    for groupVar in range(numGroupVars): # NEED ANOTHER SERIES OF LOOP FOR NO ERROR
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
