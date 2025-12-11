import sys

'''
class CFG:
    # class for CFG object
    def __init__(self, terminals, variables, substitutions):
        self.term = terminals
        self.var = variables
        self.sub = substitutions

'''
class mf_structure:
    # class for mf_structure object
    def __init__(self, S, n, V, F, r, G = None):
        self.S = S
        self.n = n
        self.V = V
        self.F = F
        self.r = r
        self.G = G

def read_from_file(f):
    '''
    Reads the input file and puts results into MF structure.
    Implies format similar to project description.

    should i put .lower() here?
    '''
    with open(f) as the_file:
        lines = the_file.readlines()
        S = lines[1].rstrip().split(", ")
        n = int(lines[3])
        V = lines[5].rstrip().split(", ") # list of group vars
        F = split_aggregates(lines[7].rstrip().split(", "), n) # list of the aggregates
        r = []
        i = 9
        while True:
            if (lines[i].rstrip() == "HAVING_CONDITION(G):"):
                i += 1
                break
            r.append(lines[i].rstrip())
            i += 1
        
        G = lines[i].rstrip()
    
    return mf_structure(S, n, V, F, r, G)

def split_aggregates(F: list[str], n):
    s = [[] for _ in range(n)]
    for aggregate in F:
        print(aggregate)
        print("Index: " + str(int(aggregate[0]) - 1))
        s[int(aggregate[0]) - 1].append(tuple(aggregate.split("_")))
    return s


def print_results(MF: mf_structure):
    '''
    Print the values after taking in the phi operator.
    '''
    print("S: " + str(MF.S))
    print("n: " + str(MF.n))
    print("V: " + str(MF.V))
    print("F: " + str(MF.F))
    print("r: " + str(MF.r))
    print("G: " + str(MF.G))

'''
Take the filename as a command-line argument.
This can be adjusted depending on how the input's taken,
but it's based on the first example from the project page.
'''

def get_aggregates(F: list[list]):
    for aggregate in F:
        pass

file = sys.argv[1]
print_results(read_from_file(file))

'''

'''