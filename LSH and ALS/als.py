
from __future__ import print_function

import sys

import numpy as np
import pandas as pd
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us, dict, movie_dict):
    diff = 0
    R_temp = ms*us.T
    for k in dict:
        x = k[0]-1
        y = movie_dict[k[1]]-1
        t = R.item((x,y))
        diff+=np.power(t-R_temp.item((x,y)),2)
    diff = np.sqrt(diff/len(dict))
    return diff


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
#
    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T
#
    for j in range(ff):
        XtX[j, j] += LAMBDA * uu
#
    return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext
    
    predictions_file = sys.argv[2]
    numPartitions = 2
    F = 5
    partitions = int(sys.argv[4]) if len(sys.argv) > 4 else 2
    ITERATIONS = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    lines = sc.textFile(sys.argv[1], numPartitions)
    header = lines.first()
    lines = lines.filter(lambda x:x!=header).map(lambda line:line.split(','))
    rdd = lines.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    movies_list = lines.map(lambda x: int(x[1])).distinct().collect()
    users_list = lines.map(lambda x:int(x[0])).distinct().collect()
    movies_lines = sc.textFile("movies.csv", numPartitions)
    M = len(users_list)
    U = len(movies_list)
    movie_header = movies_lines.first()
    movies_lines = movies_lines.filter(lambda x:x!=movie_header)
    movies_lines = movies_lines.map(lambda line:line.split(','))
    movie_rdd = movies_lines.flatMap(lambda x:[(int(x[0]),i) for i in range(1, M+1)]).distinct()
    movieMapping_dict = dict(movie_rdd.collect())
    rating_Dict = dict(rdd.collect())

    print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
          (M, U, F, ITERATIONS, partitions))

    R = np.zeros((M,U))
    for key in rating_Dict:
        user = key[0]
        movie = key[1]
        R[user-1][movieMapping_dict[movie]-1] = rating_Dict[(user,movie)]
    
    R = matrix(R)
    ms = matrix(np.ones((M, F)))
    us = matrix(np.ones((U, F)))

    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)

    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us, rating_Dict, movieMapping_dict)
        print("Iteration %d:" % (i+1))
        print("\nRMSE: %5.4f\n" % error)
    
    RFinal = ms*us.T
    columns = ["user", "movie", "rating"]
    
    users_list.sort()
    movies_list.sort()
    rows = []
    for user in users_list:
        for movie in movies_list:
            if (user,movie) not in rating_Dict:
                rows.append([user, movie, RFinal.item((user-1,movieMapping_dict[movie]-1))])
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(predictions_file, index=None, header=True)

    spark.stop()
