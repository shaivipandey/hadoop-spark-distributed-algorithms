class PR:
    def __init__(self, file_path, d=0.85):
        # Initialize with the file path and damping factor
        self.d = d  # Set the damping factor
        self.links = self.Parser(file_path).links  # Parse the input file to get the links
        self.num_nodes = len(self.links)  # Get the number of nodes
        self.init_rank = 1.0 / self.num_nodes  # Initialize rank for each node
        self.ranks = {node: self.init_rank for node in self.links}  # Set initial ranks for all nodes

    class Parser:
        def __init__(self, file_path):
            # Initialize the parser with the file path
            self.file_path = file_path  # Store the file path
            self.links = self.parse()  # Parse the file to get the links

        def parse(self):
            # Parse the input file to extract the links
            links = {}  # Initialize an empty dictionary for links
            with open(self.file_path, "r") as file:
                for line in file:
                    parts = line.split(":")  # Split the line into node and neighbors
                    node = int(parts[0].strip())  # Extract the node
                    neighbors = [int(n.strip()) for n in parts[1].strip()[1:-1].split(',')]  # Extract neighbors
                    links[node] = neighbors  # Add the node and its neighbors to the dictionary
            return links  # Return the parsed links

    class Calculator:
        def __init__(self, links, ranks, d, num_nodes):
            # Initialize the calculator with links, ranks, damping factor, and number of nodes
            self.links = links  # Store the links
            self.ranks = ranks  # Store the initial ranks
            self.d = d  # Store the damping factor
            self.num_nodes = num_nodes  # Store the number of nodes

        def compute(self, iters):
            # Compute PageRank values for a given number of iterations
            for i in range(iters):
                print(f"Iter {i + 1}")  # Print the current iteration
                # Initialize new ranks with the base value (1 - damping factor) / number of nodes
                new_ranks = {node: (1 - self.d) / self.num_nodes for node in self.ranks}

                # Distribute PageRank values to outgoing links
                for node, outlinks in self.links.items():
                    if outlinks:
                        contrib = self.ranks[node] / len(outlinks)  # Calculate contribution of the rank
                        for outlink in outlinks:
                            if outlink in new_ranks:  # Ensure only valid nodes are considered
                                new_ranks[outlink] += self.d * contrib  # Distribute the rank
                    else:
                        # Handle dangling nodes (nodes with no outlinks)
                        for new_node in new_ranks:
                            new_ranks[new_node] += self.d * self.ranks[node] / self.num_nodes  # Distribute evenly

                # Print PageRank values for this iteration
                for node in sorted(new_ranks):
                    print(f"Node {node}: {new_ranks[node]:.6f}")

                self.ranks = new_ranks  # Update ranks for the next iteration

            return self.ranks  # Return the final ranks

    def compute(self, iters):
        # Compute PageRank using the Calculator subclass
        calc = self.Calculator(self.links, self.ranks, self.d, self.num_nodes)  # Initialize the calculator
        self.ranks = calc.compute(iters)  # Compute and update the final ranks
        return self.ranks  # Return the final ranks

    def save_ranks(self, output_path):
        # Save the final PageRank values to a file
        with open(output_path, "w") as file:
            for node in sorted(self.ranks):
                file.write(f"Node {node}: {self.ranks[node]:.6f}\n")


# Simulate the PageRank computation for 5 iterations
file_path = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/input/question2.txt"
output_path = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/output/pagerank_practical_output.txt"
pr = PR(file_path)  # Initialize the PageRank class with the file path
final_ranks = pr.compute(5)  # Compute PageRank for 5 iterations
pr.save_ranks(output_path)  # Save the final ranks to a file
