
Main Task:

- User based Collaborative Filtering Recommendation System with Jaccard based Locality Sensitive Hashing (LSH)
	- predict ratings of movies users have not watched
- UV decomposition method - Alternating Least Squares utilising the Utility Matrix

Execution format:

spark-submit lsh.py ratings.csv predictions.csv

spark-submit als.py ratings.csv predictions.csv


