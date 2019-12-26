import pyspark
from operator import add
import re
import sys

def parseNeighbors(edge):
    """Parses a urls pair string into urls pair."""
    edge = re.split(r'\s+', edge)
    return (edge[0], edge[1])


def parseNeighborsT(edge):
    """Parses a urls pair string into urls pair."""
    edge = re.split(r'\s+', edge)
    return (edge[1], edge[0])
'''
def form_row(x):
    row = [0 for i in range(number)]
    for i in list(x):
        row[i] = 1
    return row
'''
'''
def form_row_2(x):
    x = list(x)
    if not x[1]:
        return [0 for i in range(number)]
    return x[1]
'''

def compute(x,y):
    for val in x:
        yield (val,y)



if __name__ == "__main__":
    #if len(sys.argv) != 3:
       # print("Usage: hits <file> <iterations>", file=sys.stderr)
       # exit(-1)

    sc = pyspark.SparkContext()

    data = sc.textFile(sys.argv[1])
    number = int(data.flatMap(lambda x: x.split()).max())
    L = data.map(lambda edge: parseNeighbors(edge)).distinct().groupByKey().mapValues(list)
    LT = data.map(lambda edge: parseNeighborsT(edge)).distinct().groupByKey().mapValues(list)
    h = data.map(lambda x: x.split()).flatMap(lambda x: [(x[0], 1.0), (x[1], 1.0)]).distinct()

    #L = data.map(lambda x: x.split()).map(lambda x: (int(x[0]), int(x[1]))).groupByKey().mapValues(lambda x: form_row(x))
    #print(L.collect())
    #LT = data.map(lambda x: x.split()).map(lambda x: (int(x[1])-1, int(x[0])-1)).groupByKey().mapValues(lambda x: form_row(x))
    '''
    h = sc.parallelize([(i, 1) for i in range(number)])
    #print(h.collect())
    temp = sc.parallelize([(i, 1) for i in range(number)])
    L = temp.leftOuterJoin(L)
    L = L.mapValues(lambda x: form_row_2(x))
    #print(L.collect())
    LT = temp.leftOuterJoin(LT)
    LT = LT.mapValues(lambda x: form_row_2(x))
    print(LT.collect())
    '''
    for iter in range(int(sys.argv[2])):
        auth = L.join(h).flatMap(lambda i: compute(i[1][0],i[1][1])) #map(lambda row_h: compute(row_h[1][0], row_h[1][1]))
        a = auth.reduceByKey(add)
        max_value_auth = a.map(lambda x: x[1]).max()
        a = a.map(lambda x: (x[0],x[1]/max_value_auth))
    
        hub = LT.join(a).flatMap(lambda i: compute(i[1][0],i[1][1]))
        h = hub.reduceByKey(add)
        max_value_hub = h.map(lambda x: x[1]).max()
        h = h.map(lambda x: (x[0],x[1]/max_value_hub))
    
    a = a.map(lambda x: (int(x[0]), x[1]))
    h = h.map(lambda x: (int(x[0]), x[1]))
    fhandle = open('p2.txt', 'w')
    hub_dict = h.collectAsMap()
    auth_dict = a.collectAsMap()


    for i in range(1, number + 1):
        if i in hub_dict:
            val = round(hub_dict[i], 2)
            if val == 0.0:
                val = 0
        else:
            val = 0
        if i != number:
            fhandle.write(str(val) + " ")
        else:
            fhandle.write(str(val) + "\n")
    
    for i in range(1, number+ 1):
        if i in auth_dict:
            val = round(auth_dict[i], 2)
            if val == 0.0:
                val = 0
        else:
            val = 0
        if i != number:
            fhandle.write(str(val) + " ")
        else:
            fhandle.write(str(val))
    fhandle.close()





    
    

    
    
    
    
    
    
