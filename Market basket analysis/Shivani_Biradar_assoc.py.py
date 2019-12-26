from pyspark import SparkContext
import sys
from itertools import combinations,chain
from collections import Counter
import numpy as np

def subsets(arr):
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])

sc = SparkContext("local[*]")
input_file = sys.argv[1]
support = int(sys.argv[2])

data = sc.textFile(input_file)
header = data.first()
baskets = data.filter(lambda x: x != header).map(lambda x : x.split(",")).map(lambda x : (int(x[0]),int(x[1]))).groupByKey().map(lambda x: list(set(x[1])))
totalCount = baskets.count()
#print(baskets)
numpartitions = baskets.getNumPartitions()
def candidate_occurences(baskets, candidates):
	count_dict = {}
	baskets = list(baskets)

	for candidate in candidates:
		if type(candidate) is int:
			candidate = [candidate]
			key = tuple(sorted(candidate))
		else:
			key = candidate

		candidate = set(candidate)
		for basket in baskets:
			if candidate.issubset(basket):
				if key in count_dict:
					count_dict[key] = count_dict[key] + 1
				else:
					count_dict[key] = 1

	return count_dict.items()
def get_candidate_pairs(frequentItems):
    pairs = []
    for subset in combinations(frequentItems,2):
    	subset = list(subset)
        subset.sort()
        pairs.append(subset)
    return pairs

def get_candidate_items(frequentItems,k):
    combs = []
    frequentItems = list(frequentItems)
    for i in range(len(frequentItems)-1):
    	for j in range(i+1,len(frequentItems)):
        	a = frequentItems[i]
        	b = frequentItems[j]
        	if a[0:(k - 2)] == b[0:(k - 2)]:
                	combs.append(list(set(a) | set(b)))
            	else:
                	break
    return combs
def get_frequent_items(baskets, candidatefrequentitems, sup_threshold):
    k_itemset_count_dict = {}
    for candidate in candidatefrequentitems:
    	candidate = set(candidate)
    	key = tuple(sorted(candidate))
        for basket in baskets :
        	if candidate.issubset(basket):
             		if key in k_itemset_count_dict:
                    		k_itemset_count_dict[key]+=1
                	else:
                    		k_itemset_count_dict[key] = 1
    k_itemset_count = Counter(k_itemset_count_dict)
    kfrequentitems = {x : k_itemset_count[x] for x in k_itemset_count if k_itemset_count[x]>=sup_threshold}
    kfrequentitems = sorted(kfrequentitems)
    return kfrequentitems

<<<<<<< HEAD:Market basket analysis/Shivani_Biradar_assoc.py.py


def apriori(baskets,support,totalCount):
=======
def apriori(baskets,support):
>>>>>>> 986cf57bac4357f06f4f4189ff170d8d425d7588:Market basket analysis/Shivani_Biradar_assoc.py
    baskets = list(baskets)
    sup_threshold = np.floor(float(support)/numpartitions)
    
    final_result = []
    singleton = Counter()
    for basket in baskets:
        singleton.update(basket)
    candidatesingletons = {x : singleton[x] for x in singleton if singleton[x]>=sup_threshold}
    freq_singletons = sorted(candidatesingletons)
    final_result.extend(freq_singletons)
    frequentItems = set(freq_singletons)
    k=2
    while len(frequentItems)!=0:
        if k == 2:
            candidatefrequentitems = get_candidate_pairs(frequentItems)
        else:
            candidatefrequentitems = get_candidate_items(frequentItems,k)

        new_frequent_items = get_frequent_items(baskets, candidatefrequentitems, sup_threshold)
        final_result.extend(new_frequent_items)
        frequentItems = list(set(new_frequent_items))
        frequentItems.sort()
        k+=1
    return final_result

<<<<<<< HEAD:Market basket analysis/Shivani_Biradar_assoc.py.py

map_output_phase1 = baskets.mapPartitions(lambda x: apriori(x,support,totalCount )).map(lambda x: (x,1))
#temp = list(set(map_output_phase1))
#print(len(map_output_phase1))

#print(len(temp))
reduce_output_phase1 = map_output_phase1.reduceByKey(lambda x,y: (1)).keys().collect()

map_output_phase2 = baskets.mapPartitions(lambda x : candidate_occurences(x,reduce_output_phase1))
reduce_output_phase2 = map_output_phase2.reduceByKey(lambda x,y: (x+y))
final_res = reduce_output_phase2.filter(lambda x: x[1] >= support)
freq_items_for_conf = final_res.map(lambda x:x[0]).collect()
freq_items_count = final_res.collectAsMap()
#print(freq_items_count)
outputfile = open("Output_temp.txt","w")

frequent_items = final_res.map(lambda x:x[0]).map(lambda x: set(x)).collect()
frequent_items = sorted(frequent_items, key = lambda x: (len(x), x))

confidence_threshold = float(sys.argv[3])
dict = {}
#for I in freq_items_for_conf:
   # for j in baskets:

    #if I not in dict:
     #   dict[I] = 1
    #else:
     #   dict[I] += 1



#def getSupport(item):
 #   return float(dict[item]) / totalCount
def conf_calc(baskets,freq_items_count,confidence_threshold):
=======
def conf_calc(freq_items_count,confidence_threshold):
>>>>>>> 986cf57bac4357f06f4f4189ff170d8d425d7588:Market basket analysis/Shivani_Biradar_assoc.py
    association_list = []
    for value in freq_items_count.keys():
    	_subsets = map(frozenset,[x for x in subsets(value)])
        for element in _subsets:
        	remain = set(value).difference(set(element))
            if len(remain) > 0:
            	confidence = float(freq_items_count[tuple(value)]) / freq_items_count[tuple(element)]
            	if confidence >= confidence_threshold:
                	rule=(remain,tuple(element),confidence)
                	association_list.append(rule)

    return association_list

<<<<<<< HEAD:Market basket analysis/Shivani_Biradar_assoc.py.py
association = baskets.mapPartitions(lambda x: conf_calc(x,freq_items_count,confidence_threshold)).collect()
print(association)
=======
sc = SparkContext("local[*]")
input_file = sys.argv[1]
support = int(sys.argv[2])
confidence_threshold = float(sys.argv[3])

data = sc.textFile(input_file)
header = data.first()
baskets = data.filter(lambda x: x != header).map(lambda x : x.split(",")).map(lambda x : (int(x[0]),int(x[1]))).groupByKey().map(lambda x: list(set(x[1])))
totalCount = baskets.count()
numpartitions = baskets.getNumPartitions()


map_output_phase1 = baskets.mapPartitions(lambda x: apriori(x,support)).map(lambda x: (x,1))

reduce_output_phase1 = map_output_phase1.reduceByKey(lambda x,y: (1)).keys().collect()
>>>>>>> 986cf57bac4357f06f4f4189ff170d8d425d7588:Market basket analysis/Shivani_Biradar_assoc.py



<<<<<<< HEAD:Market basket analysis/Shivani_Biradar_assoc.py.py
=======
association = baskets.mapPartitions(lambda x: conf_calc(freq_items_count,confidence_threshold)).collect()
>>>>>>> 986cf57bac4357f06f4f4189ff170d8d425d7588:Market basket analysis/Shivani_Biradar_assoc.py

outputfile.write("Frequent Itemset")
outputfile.write("\n\n")
if len(frequent_items) != 0:
	p = len(frequent_items[0])
	Val = str(frequent_items[0]).replace(',', '')
	outputfile.write(Val)
	for i in range(1, len(frequent_items)):
		c = len(frequent_items[i])
		if p == c:
			outputfile.write(", ")
		else:
			outputfile.write("\n\n")

		if c == 1:
			Val = str(frequent_items[i]).replace(',', '')
		else:
			Val = str(frequent_items[i])
		outputfile.write(Val)
		p = c
outputfile.write("\n\n")
outputfile.write("Confidence")
outputfile.write("\n\n")

for p in association:
    outputfile.write(str(p[0]))
    outputfile.write(" ")
    outputfile.write(str(p[1]))
    outputfile.write(" ")
    outputfile.write(str(p[2]))
    outputfile.write("\n")

#print(frequent_items)
#for item in map_output_phase2:
 #   if item[1] >= support:
  #      frequent_items.append(item[0])

#print(frequent_items)


