from pyspark.sql import SparkSession
from typing import List, Tuple
import time

# Define the main class for Single Source Shortest Path MapReduce
class SSSPMR:
    def __init__(self, in_f: str, out_f: str):
        self.in_f = in_f  # Store the input file path
        self.out_f = out_f  # Store the output file path
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("SSSPMR").getOrCreate()
        self.graph_rdd = None  # Initialize the graph RDD as None

    # Define a static method to parse the input lines into a tuple of integers
    @staticmethod
    def parse(line: str) -> Tuple[int, Tuple[int, float]]:
        parts = line.split(',')  # Split the line by commas
        return int(parts[0].strip()), (int(parts[1].strip()), float(parts[2].strip()))  # Return the parsed tuple

    # Define a static method to initialize nodes with distances and adjacency lists
    @staticmethod
    def init_nodes(edges_rdd):
        # Inner function to create a node
        def create_node(n_id, edges):
            return n_id, SSSPMR.N(n_id, 0 if n_id == 0 else float('inf'), list(edges))  # Node distance is 0 for source, else infinity
        
        grouped_edges = edges_rdd.groupByKey()  # Group edges by key
        return grouped_edges.map(lambda x: create_node(x[0], x[1]))  # Create nodes from grouped edges

    # Define the Node class to represent graph nodes
    class N:
        def __init__(self, n_id: int, dist: float, adj: List[Tuple[int, float]]):
            self.n_id = n_id  # Node ID
            self.dist = dist  # Distance from source
            self.adj = adj  # Adjacency list

        def __repr__(self):
            return f"N(id={self.n_id}, dist={self.dist}, adj={self.adj})"  # String representation of the node

    # Define the Mapper class for the map phase
    class M:
        @staticmethod
        def map(n_id: int, node: 'SSSPMR.N'):
            yield (n_id, node)  # Emit the node itself
            for adj_n, w in node.adj:  # For each adjacent node
                yield (adj_n, node.dist + w)  # Emit the adjacent node with updated distance

    # Define the Reducer class for the reduce phase
    class R:
        @staticmethod
        def reduce(n_id: int, vals: List):
            min_dist = float('inf')  # Initialize minimum distance as infinity
            node = None  # Initialize node as None
            for val in vals:
                if isinstance(val, SSSPMR.N):  # Check if the value is a node
                    node = val  # Assign the node
                else:
                    min_dist = min(min_dist, val)  # Update the minimum distance
            
            if node:
                node.dist = min(node.dist, min_dist)  # Update the node's distance
            return (n_id, node)  # Return the reduced node

    # Define the method to run the Single Source Shortest Path algorithm
    def run_sssp(self):
        iteration = 0  # Initialize iteration counter
        while True:
            print(f"Iteration {iteration}")
            
            # Map phase: apply the mapper to the graph RDD
            mapped = self.graph_rdd.flatMap(lambda x: SSSPMR.M.map(x[0], x[1]))
            
            # Reduce phase: group by key and apply the reducer
            new_graph = mapped.groupByKey().map(lambda x: SSSPMR.R.reduce(x[0], list(x[1])))
            
            # Define a function to compare old and new distances for convergence check
            def compare_dists(old, new):
                return old.dist == new.dist
            
            # Check for convergence
            unchanged = self.graph_rdd.join(new_graph).map(lambda x: compare_dists(x[1][0], x[1][1])).reduce(lambda x, y: x and y)
            if unchanged:
                break  # Exit the loop if distances have not changed
            
            self.graph_rdd = new_graph  # Update the graph RDD with the new graph
            iteration += 1  # Increment the iteration counter
        
        return self.graph_rdd  # Return the final graph RDD

    # Define the main run method to execute the entire process
    def run(self):
        print("Spark Web UI available at:", self.spark.sparkContext.uiWebUrl)  # Print the Spark Web UI URL
        
        # Read the input file and create the initial graph RDD
        input_rdd = self.spark.sparkContext.textFile(self.in_f)
        edges_rdd = input_rdd.map(self.parse)
        self.graph_rdd = SSSPMR.init_nodes(edges_rdd)
        
        # Run the SSSP algorithm
        result_rdd = self.run_sssp()
        
        # Collect and print the results
        results = result_rdd.collect()
        for n_id, node in sorted(results):
            print(f"Shortest distance to node {n_id}: {node.dist}")
        
        # Find the distance to node 4
        dist_to_4 = next(node.dist for n_id, node in results if n_id == 4)
        print(f"Shortest distance from node 0 to node 4: {dist_to_4}")

        # Write the results to the output file
        with open(self.out_f, "w") as f:
            for n_id, node in sorted(results):
                f.write(f"{n_id},{node.dist}\n")
        
        print("Results written to", self.out_f)  # Notify that results are written to the file
        print("Keeping Spark context alive for 100 minutes. Access the Web UI now.")
        time.sleep(6000)  # Keep the Spark context alive for 10 minutes
        
        self.spark.stop()  # Stop the Spark session

if __name__ == "__main__":
    # Define input and output file paths
    in_f = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/input/question1.txt"
    out_f = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/output/output_1.txt"
    # Create an instance of the SSSPMR class and run the process
    sssp = SSSPMR(in_f, out_f)
    sssp.run()




























