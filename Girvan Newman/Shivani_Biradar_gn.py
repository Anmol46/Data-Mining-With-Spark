from pyspark import SparkContext
import collections
from itertools import combinations
import sys

def BFS(root_node):
	start_level = 1
	bfs_list = [(root_node, start_level)]
	visit = [root_node]
	visited = [root_node]
	nodes_level = {1: [root_node]}

	while len(visit) > 0:
		start_level += 1
		nodes_level[start_level] = []
		form_new_nodes = []
		for n in visit:

			connected_nodes = adjacency_list_dict[n]

			for i in connected_nodes:

				if i not in visited:

					visited.append(i)

					bfs_list.append((i, start_level))
					nodes_level[start_level].append(i)

					if i not in form_new_nodes:

						form_new_nodes.append(i)

		visit = form_new_nodes
		if len(nodes_level[start_level]) == 0:
			del(nodes_level[start_level])

	return bfs_list, nodes_level


def edge_betweenness(bfs_list, nodes_level):

	edge_scores = {}
	node_scores = {}

	levels = dict(bfs_list)

	for level in sorted(list(nodes_level.keys()), reverse=True):

		nodes = nodes_level[level]

		for node in nodes:

			if node in node_scores:
				node_scores[node] += 1.0
			else:
				node_scores[node] = 1.0

			nodes_connected_to_this_node = adjacency_list_dict[node]
			divide_score = []
			counter = 0

			for conn_node in nodes_connected_to_this_node:

				if levels[conn_node] < level:

					count = 0

					level_of_conn_node = levels[conn_node]-1

					try:
						traveled_nodes = nodes_level[level_of_conn_node]

						for t_node in traveled_nodes:

							if t_node in adjacency_list_dict[conn_node]:

								count += 1

						divide_score.append((conn_node, count))
						counter += count
					except:
						divide_score.append((conn_node, 1))
						counter += 1

			# if main node is reached, len(divide_score) will be 0, this will avoid ZeroDivisionError error
			try:
				d_score = node_scores[node]/counter
			except:
				continue

			for conn_node in divide_score:

				try:
					node_scores[conn_node[0]] += d_score*conn_node[1]
				except:
					node_scores[conn_node[0]] = d_score*conn_node[1]

				try:
					edge_scores[tuple(sorted([conn_node[0], node]))] += d_score*conn_node[1]
				except:
					edge_scores[tuple(sorted([conn_node[0], node]))] = d_score*conn_node[1]

	return edge_scores


def execution(node):

	bfs_list, node_level = BFS(node)
	get_edge_scores = edge_betweenness(bfs_list, node_level)
	return get_edge_scores.items()

sc = SparkContext(appName="Girvan_Newman")
input_file = sc.textFile(sys.argv[1])
input_graph = input_file.map(lambda x: tuple(x.split(",")))
adjacency_list = input_file.map(lambda x: x.split(",")).flatMap(lambda x: [(x[0],x[1]),(x[1],x[0])]).groupByKey()
adjacency_list_dict = adjacency_list.mapValues(list).collectAsMap()
nodes = adjacency_list.keys()
edge_betweenness = nodes.flatMap(execution).groupByKey().map(lambda x: (x[0],sum(list(x[1]))/2)).collect()
edge_betweenness = sorted(edge_betweenness,key = lambda x: x[0])

for tup in edge_betweenness:
   print("("+tup[0][0]+","+tup[0][1]+")"+","+str(tup[1]))

