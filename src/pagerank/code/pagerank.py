from pyspark.sql import SparkSession
import re
import time
from operator import add
from pyspark import StorageLevel

# Parser class to handle parsing of lines in the input file
class Prs:
    @staticmethod
    def p_neigh(line):
        # Splits the line into parts based on ':'
        parts = line.split(':')
        # The first part is the node
        node = int(parts[0].strip())
        # The second part contains the neighbors 
        # Strip the brackets and split by comma to get individual neighbors
        neigh = [int(n.strip()) for n in parts[1].strip()[1:-1].split(',')]
        return node, neigh

# Class to calculate contributions from nodes to their neighbors
class CntrbCalc:
    @staticmethod
    def c_contribs(neigh_rank):
        # Unpack the neighbors and rank
        neigh, rank = neigh_rank
        n_neigh = len(neigh)
        # If the node has neighbors, distribute its rank among them
        if n_neigh > 0:
            for n in neigh:
                # Yield the contribution to each neighbor
                yield (n, rank / n_neigh)
        else:
            # If there are no neighbors (dead-end), yield a special identifier
            yield ('dead_end', rank)

# DataLoader class to load and parse the input data file
class DLoader:
    def __init__(self, f_path, parts):
        self.f_path = f_path  # File path to the input data
        self.parts = parts    # Number of partitions for the RDD

    def l_p_data(self, spark):
        # Read the input file into an RDD
        lines = spark.read.text(self.f_path).rdd.map(lambda r: r[0])
        # Print the number of lines in the input file
        print(f"Lines in file: {lines.count()}")

        # Check if the input file is empty and raise an error if it is
        if lines.isEmpty():
            raise ValueError(f"Error: The file {self.f_path} is empty.")

        # Parse each line into (node, neighbors) pairs and partition the data
        links = lines.map(Prs.p_neigh).partitionBy(self.parts).persist(StorageLevel.MEMORY_AND_DISK)

        # Get all nodes (distinct) and identify nodes with zero out-links
        all_nodes = links.flatMap(lambda x: [x[0]] + x[1]).distinct()
        zero_rank_nodes = all_nodes.map(lambda x: (x, [])).subtractByKey(links)
        links = links.union(zero_rank_nodes).persist(StorageLevel.MEMORY_AND_DISK)

        # Count the number of unique pages (nodes) in the graph
        n_pages = links.count()
        print(f"Nodes in graph: {n_pages}")

        # Check if any valid links were found and raise an error if none were found
        if links.isEmpty():
            raise ValueError("Error: No valid links in the file.")

        return links, n_pages

# PageRank calculation class
class PRCalc:
    def __init__(self, conv_thr, max_iter):
        self.conv_thr = conv_thr  # Convergence threshold for PageRank
        self.max_iter = max_iter  # Maximum number of iterations

    def run_pr(self, links, n_pages):
        # Initialize ranks for each page to 1 / number of pages
        ranks = links.mapValues(lambda _: 1.0 / n_pages)
        # Initialize previous ranks to zero
        prev_ranks = ranks.map(lambda x: (x[0], 0))
        # Get all distinct nodes
        all_nodes = links.keys().distinct().persist(StorageLevel.MEMORY_AND_DISK)
        it = 0  # Iteration counter

        # Iterative computation of PageRank
        while True:
            it += 1
            print(f"Iteration {it}")

            # Calculate contributions from each node to its neighbors
            contribs = links.join(ranks).flatMap(
                lambda u_r: CntrbCalc.c_contribs((u_r[1][0], u_r[1][1]))
            )

            # Sum contributions from dead-end nodes and remove them from the contributions list
            dead_end_sum = contribs.filter(lambda x: x[0] == 'dead_end').map(lambda x: x[1]).sum()
            contribs = contribs.filter(lambda x: x[0] != 'dead_end')

            # Calculate new ranks by adding contributions, handling dead-ends, and applying damping factor
            contribs = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15 / n_pages + dead_end_sum * 0.85 / n_pages)
            contribs = all_nodes.map(lambda n: (n, 0)).leftOuterJoin(contribs).mapValues(lambda x: x[1] if x[1] is not None else 0)

            ranks = contribs.mapValues(lambda rank: rank * 0.85 + 0.15 / n_pages + dead_end_sum * 0.85 / n_pages)

            # Calculate the difference between current and previous ranks to check for convergence
            diff = ranks.join(prev_ranks).map(lambda x: abs(x[1][0] - x[1][1])).sum()

            # Check if the ranks have converged
            if diff < self.conv_thr:
                print(f"Converged after {it} iterations.")
                break

            # Check if the maximum number of iterations is reached
            if it >= self.max_iter:
                print(f"Max iterations reached ({it}).")
                break

            # Update previous ranks for the next iteration
            prev_ranks = ranks

        return ranks

# Class to process and output the final PageRank results
class RProcessor:
    def __init__(self, out_path):
        self.out_path = out_path  # File path to save the output results

    def p_results(self, ranks):
        # Collect the ranks into a dictionary
        f_ranks = ranks.collectAsMap()

        # Check if PageRank calculation produced results
        if not f_ranks:
            raise ValueError("Error: PageRank produced no results.")

        # Sort ranks from highest to lowest
        s_ranks = sorted(f_ranks.items(), key=lambda x: x[1], reverse=True)

        # Print all PageRanks from highest to lowest
        print("PageRanks from highest to lowest:")
        for node, rank in s_ranks:
            print(f"Node {node}: {rank:.6f}")

        # Find and print the node with the highest and lowest PageRank
        max_r_node, max_rank = s_ranks[0]
        min_r_node, min_rank = s_ranks[-1]

        print(f"\nNode with highest PageRank: {max_r_node} (Rank: {max_rank:.6f})")
        print(f"Node with lowest PageRank: {min_r_node} (Rank: {min_rank:.6f})")

        # Write the sorted ranks to the output file
        with open(self.out_path, "w") as f:
            for node, rank in s_ranks:
                f.write(f"{node},{rank}\n")

        print(f"Results written to {self.out_path}")

# Main class to orchestrate the entire PageRank process
class PR:
    def __init__(self, f_path, parts=1, conv_thr=0.0001, max_iter=50):
        self.d_loader = DLoader(f_path, parts)  # DataLoader instance
        self.calc = PRCalc(conv_thr, max_iter)  # PRCalc instance
        self.proc = RProcessor("/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/output/output2.txt")  # RProcessor instance

    def run(self):
        # Create Spark session
        spark = SparkSession.builder.appName("PRAlgorithm").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        print("Starting PageRank...")
        try:
            # Load and parse data
            links, n_pages = self.d_loader.l_p_data(spark)
            # Run PageRank calculation
            ranks = self.calc.run_pr(links, n_pages)
            # Process and output the results
            self.proc.p_results(ranks)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Provide access to the Spark Web UI for debugging and monitoring
            print(f"Spark Web UI at: {spark.sparkContext.uiWebUrl}")
            print("Keeping Spark context alive for 100 minutes. Access the Web UI now.")
            time.sleep(60000)  # Keep Spark context alive for 1000 minutes
            spark.stop()

# Main function to kick off the process
def main():
    f_path = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/input/question2.txt"  # Path to the input file
    pr = PR(f_path)  # Create PR instance
    pr.run()  # Run the PageRank process

# Entry point of the script
if __name__ == "__main__":
    main()  # Call the main function
