
Main Task:

- Discover frequent itemsets i.e movies frequently watched by many users by implementing SON algorithm
- Discover association rules of the form I --> j where I is an itemset and j is a single item

Execution Format :

spark-submit assoc.py ratings.csv <support_threshold> <confidence_threshold>

where support_threshold is an integer(for support count) and the confidence threshold is a value between 0 and 1


