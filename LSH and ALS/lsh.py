from pyspark import SparkContext
from itertools import combinations
import operator
import sys
import pandas as pd
import csv
sc = SparkContext()
sc.setLogLevel("WARN")
input_file = sys.argv[1]
input_file_rdd = sc.textFile(input_file,minPartitions=None)
input_file_rdd = input_file_rdd.mapPartitions(lambda x: csv.reader(x))
header = input_file_rdd.first()
input_file_rdd = input_file_rdd.filter(lambda x: x!=header)
user_movie_ratings_dict = dict(input_file_rdd.map(lambda x: ((int(x[0]),int(x[1])),float(x[2]))).collect())
#print(user_movie_ratings_dict)
users_list = sorted(input_file_rdd.map(lambda x : int(x[0])).distinct().collect())
movies_list = sorted(input_file_rdd.map(lambda x: int(x[1])).distinct().collect())
jaccard_data = input_file_rdd.map(lambda x: (int(x[0]),int(x[1]))).groupByKey().mapValues(list)
user_movie_dict_data = jaccard_data.collect()
user_movie_dict = dict(user_movie_dict_data)
#print(user_movie_dict)

#function to hash into buckets
def minhash(x):
    hash_buckets = []
    for i in range(1,51):
        temp_bucket = []
        for m in x[1]:
            h = (3*m+11*i)%100 + 1
            temp_bucket.append(h)
        hash_buckets.append((x[0],min(temp_bucket)))
    return hash_buckets

'''def get_signature_matrix(x):
    signature_matrix = []
    signature_matrix.append(x)
    return signature_matrix'''




def LSH_bands(band):
    return [((band[0],0),band[1][0:5]),((band[0],1),band[1][5:10]),((band[0],2),band[1][10:15]),((band[0],3),band[1][15:20]),((band[0],4),band[1][20:25]),((band[0],5),band[1][25:30]),((band[0],6),band[1][30:35]),((band[0],7),band[1][35:40]),((band[0],8),band[1][40:45]),((band[0],9),band[1][45:50])]

def modify_data(x):
    userid_bandnum = x[0]
    signature = x[1]
    userid_signature = [userid_bandnum[0],signature]
    return (userid_bandnum[1],userid_signature)

def get_candidates(inp):

    band_num = inp[0]
    user_signatures = inp[1]
    similar_users_list = []
    user_signature_dict = {}
    LSH_pairs=[]
    for v in user_signatures:
        temp=tuple(v[1])
        if temp not in user_signature_dict:
            user_signature_dict[temp] = [v[0]]
        else:
            if v[0] not in user_signature_dict[temp]:
                user_signature_dict[temp].append(v[0])

    for users in user_signature_dict.values():
        if len(users)>1:
            similar_users_list.append(users)
    for users in similar_users_list:
        comb_pairs = list(combinations(users,2))
        LSH_pairs+=comb_pairs
    return list(set(LSH_pairs))

def Jaccard_similarity(user_1,user_2):
    intersect_len = len(set(user_movie_dict.get(user_1)).intersection(set(user_movie_dict.get(user_2))))
    union_len = len(set(user_movie_dict.get(user_1)).union(set(user_movie_dict.get(user_2))))
    similarity = (intersect_len/union_len)
    return (user_1,(user_2,similarity))


#get the 50 hash functions with userID as the key
jaccard_rdd = jaccard_data.flatMap(lambda x:minhash(x)).groupByKey().mapValues(list)

signatureMatrix = jaccard_rdd.sortByKey()#flatMap(lambda x:get_signature_matrix(x)).sortByKey()
signature_rdd = signatureMatrix.flatMap(LSH_bands)
#modify the data to have band as the key and userid as value and group by band
candidates_rdd = signature_rdd.map(modify_data).groupByKey().mapValues(list)

candidates_rdd = candidates_rdd.flatMap(lambda x: get_candidates(x)).distinct().flatMap(lambda x: [(x[0],x[1]),(x[1],x[0])])


jaccard_similarity_rdd = candidates_rdd.map(lambda x :(Jaccard_similarity(x[0],x[1])))
top_3_users = jaccard_similarity_rdd.groupByKey().mapValues(list).map(lambda x: (x[0],sorted(sorted(x[1]),key=lambda y:y[1],reverse=True)[:3])).map(lambda x: (x[0],[x[1][i][0] for i in range(len(x[1]))])).collect()
top_3_users = sorted(top_3_users)
top_3_users_dict = dict(top_3_users)
#print(top_3_users_dict)
#print(len(top_3_users))

list_of_users_movies_avg = []
for user in users_list:
    for movie in movies_list:
        if (user,movie) not in user_movie_ratings_dict:
            similar_users = top_3_users_dict.get(user)
            #if user in top_3_users_dict:
            if similar_users!= None:

                #similar_users = top_3_users_dict[user]
                sum = 0
                count = 0
                for each_user in similar_users:
                    if (each_user,movie) in user_movie_ratings_dict:
                        sum += user_movie_ratings_dict[(each_user,movie)]
                        count+=1
                if count!=0:
                    average = float(sum/count)
                    list_of_users_movies_avg.append((user,movie,average))
columns = ["Users","Movies","Ratings"]
final_df = pd.DataFrame(list_of_users_movies_avg,columns=columns)
predictions_file = sys.argv[2]
final_df.to_csv(predictions_file,index=None)



            


