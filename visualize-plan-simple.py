import re
import os
import subprocess
from pathlib import Path

class SparkPlanVisualizer:
    def __init__(self):
        self.node_counter = 0
        self.nodes = {}
        self.edges = []
        self.last_nodes_at_depth = {}
        self.simple_mode = False


    def simplify_node_labels(self, operation_type, details):
        if operation_type.lower() == 'relation':
            # Try to extract table name from details
            table_match = re.search(r'spark_catalog\.([^\.]+\.[^\[]+)', details)
            if table_match:
                table_name = table_match.group(1)
                return f"{operation_type} {table_name}"
            else:
                # Alternative pattern for other relation formats
                table_match = re.search(r'([^\s\[]+)', details)
                if table_match:
                    table_name = table_match.group(1)
                    return f"{operation_type} {table_name}"
                else:
                    return operation_type
        else:
            return operation_type

    def parse_plan(self, file_path):
        """Parse the Spark query plan from a text file."""
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Process each line in the plan
        for line in lines:
            if line.strip() == '':
                continue

            line = line.replace("+- ", "   ")
            line = line.replace(":- ", "   ")
            line = line.replace(":  ", "")
            # Calculate the depth based on indentation
            # Count the number of spaces before the text
            leading_spaces = len(line) - len(line.lstrip())
            depth = leading_spaces // 3  # Assuming 3 spaces per indentation level

            # Extract operation name and attributes
            line_content = line.strip()

            # Handle various prefixes commonly found in Spark plans
            # This handles +-, :-, +, : and other potential prefixes
            def remove_non_letters_prefix(l):
                return re.sub(r'^[^a-zA-Z]+', '', l)

            line_content = remove_non_letters_prefix(line_content)
            # line_content = re.sub(r'^[+:|]+-?\s*', '', line_content)


            # Remove trailing colon (often used for join nodes)
            if line_content.endswith(':'):
                line_content = line_content[:-1].strip()

            # Create a node for this operation
            node_id = f"node_{self.node_counter}"
            self.node_counter += 1

            # Extract operation type and details
            # Look for the first word which is typically the operation
            operation_match = re.match(r'([A-Za-z]+[A-Za-z0-9]*)', line_content)
            if operation_match:
                operation_type = operation_match.group(1)
                print(f"OPTYPE: {operation_type}")
                details = line_content[len(operation_type):].strip()

                # Handle potential operation types with additional words
                if operation_type.lower() in ['exchange', 'sort', 'filter', 'project',
                                             'aggregate', 'scan', 'join', 'limit',
                                             'expand', 'window', 'union', 'generate',
                                             'globallimit', 'locallimit', 'broadcastexchange']:

                    def extend_op(l):
                        if 'join' in operation_type.lower():
                            return re.match(r'([A-Za-z]+[A-Za-z0-9]*\s*[A-Za-z]*[A-Za-z0-9]*)', line_content)
                        else:
                            return re.match(r'([A-Za-z]+[A-Za-z0-9]*)', line_content)

                    extended_op_match = extend_op(line_content)

                    if extended_op_match and ' ' in extended_op_match.group(1):
                        operation_type = extended_op_match.group(1)
                        details = line_content[len(operation_type):].strip()

                # Clean up details by removing complex expressions in brackets for cleaner visualization
                # This is optional and can be adjusted based on preference
                clean_details = re.sub(r'\[[^\]]*\]', '', details).strip()
                print(f"DETAILS: {clean_details}")

                # Create node label based on simple mode setting
                if self.simple_mode:
                    node_label = self.simplify_node_labels(operation_type, details)
                else:
                    # For detailed mode, include all details
                    if clean_details:
                        node_label = f"{operation_type} {clean_details}"
                    else:
                        node_label = operation_type
            else:
                # If we can't extract a clean operation type, use the whole line
                node_label = line_content

            # Store the node
            self.nodes[node_id] = {
                'label': node_label,
                'depth': depth,
                'original': line_content
            }

            # Create an edge from this node to its parent (if it exists)
            if depth > 0:
                # Find the most recent node at the parent depth level
                parent_depth = depth - 1
                while parent_depth >= 0:
                    if parent_depth in self.last_nodes_at_depth:
                        parent_node = self.last_nodes_at_depth[parent_depth]
                        self.edges.append((parent_node, node_id))
                        break
                    parent_depth -= 1

            # Update the last node at this depth
            self.last_nodes_at_depth[depth] = node_id

    def generate_dot(self, output_path):
        """Generate a DOT file from the parsed plan."""
        with open(output_path, 'w') as f:
            f.write('digraph SparkQueryPlan {\n')
            f.write('  rankdir=TB;\n')  # Top to bottom layout (TB) or Bottom to top (BT)
            f.write('  node [shape=box, style=filled, fillcolor=lightblue, fontname="Arial"];\n')
            f.write('  edge [arrowsize=0.8];\n')

            # Add nodes
            for node_id, node_info in self.nodes.items():
                # Escape double quotes and other special characters in the label
                label = node_info['label'].replace('"', '\\"').replace('\\', '\\\\')
                f.write(f'  {node_id} [label="{label}"];\n')

            # Add edges
            for source, target in self.edges:
                f.write(f'  {source} -> {target};\n')

            f.write('}\n')

    def generate_png(self, dot_path, output_path):
        """Generate a PNG file from the DOT file using Graphviz."""
        try:
            subprocess.run(['dot', '-Tpng', dot_path, '-o', output_path], check=True)
            print(f"Generated PNG file at {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error generating PNG: {e}")
        except FileNotFoundError:
            print("Graphviz 'dot' command not found. Please install Graphviz.")

def convert_plan_to_visualization(input_file):
    """Convert a Spark query plan file to DOT and PNG visualizations."""
    visualizer = SparkPlanVisualizer()

    # Parse the plan
    visualizer.parse_plan(input_file)

    # Generate output file paths
    input_path = Path(input_file)
    dot_path = input_path.with_suffix('.dot')
    png_path = input_path.with_suffix('.png')

    # Generate DOT file
    visualizer.generate_dot(dot_path)
    print(f"Generated DOT file at {dot_path}")

    # Generate PNG file
    visualizer.generate_png(dot_path, png_path)

if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Convert Spark query plans to DOT and PNG visualizations')
    parser.add_argument('input_file', help='Path to the Spark query plan text file')
    parser.add_argument('--format', choices=['png', 'svg', 'pdf'], default='png',
                        help='Output format for the visualization (default: png)')
    parser.add_argument('--simple', action='store_true',
                        help='Create a simplified visualization with minimal node details')

    args = parser.parse_args()

    visualizer = SparkPlanVisualizer()
    visualizer.simple_mode = args.simple
    visualizer.parse_plan(args.input_file)

    # Generate output file paths
    input_path = Path(args.input_file)
    dot_path = input_path.with_suffix('.dot')
    output_path = input_path.with_suffix(f'.{args.format}')

    # Generate DOT file
    visualizer.generate_dot(dot_path)
    print(f"Generated DOT file at {dot_path}")

    # Generate visualization file
    try:
        subprocess.run(['dot', f'-T{args.format}', str(dot_path), '-o', str(output_path)], check=True)
        print(f"Generated {args.format.upper()} file at {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error generating {args.format.upper()}: {e}")
    except FileNotFoundError:
        print("Graphviz 'dot' command not found. Please install Graphviz.")