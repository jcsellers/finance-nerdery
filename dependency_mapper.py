import os
import re
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt

def parse_imports(filepath):
    """Parse imports and dependencies from a Python file."""
    dependencies = set()
    with open(filepath, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=filepath)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dependencies.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    dependencies.add(node.module.split(".")[0])
    return dependencies

def find_env_variables(root_dir):
    """Find environment variables accessed in Python files."""
    env_usage = defaultdict(list)

    # Regex to match os.environ or os.getenv
    env_pattern = re.compile(r"os\\.(environ|getenv)\\(\\s*[\"'](\\w+)[\"']\\s*\\)")

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".py"):
                filepath = os.path.join(dirpath, filename)
                with open(filepath, "r", encoding="utf-8") as file:
                    for line_no, line in enumerate(file, start=1):
                        matches = env_pattern.findall(line)
                        for _, var in matches:
                            env_usage[var].append(f"{filepath}:{line_no}")
    return env_usage

def build_dependency_map(root_dir):
    """Build a dependency map for all Python files in a directory."""
    dependency_map = defaultdict(set)
    file_mapping = {}

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".py"):
                filepath = os.path.join(dirpath, filename)
                dependencies = parse_imports(filepath)
                file_mapping[filepath] = os.path.relpath(filepath, root_dir)
                dependency_map[filepath].update(dependencies)
    return dependency_map, file_mapping

def map_to_files(dependency_map, file_mapping):
    """Resolve module names to actual file paths."""
    resolved_map = defaultdict(set)
    for file, dependencies in dependency_map.items():
        resolved_dependencies = set()
        for dependency in dependencies:
            for dep_file, rel_path in file_mapping.items():
                if dependency in rel_path.replace("/", ".").rsplit(".", 1)[0]:
                    resolved_dependencies.add(dep_file)
        resolved_map[file] = resolved_dependencies
    return resolved_map

def visualize_dependency_map(dependency_map, file_mapping, env_variables, output="dependency_map.png"):
    """Visualize the dependency map using NetworkX."""
    G = nx.DiGraph()

    # Add nodes and edges
    for file, dependencies in dependency_map.items():
        file_node = file_mapping[file]
        G.add_node(file_node)
        for dep in dependencies:
            dep_node = file_mapping.get(dep, dep)
            G.add_edge(file_node, dep_node)

    # Add shared environment variable nodes
    for var, locations in env_variables.items():
        shared_var_node = f"ENV: {var}"
        G.add_node(shared_var_node)
        for location in locations:
            file = location.split(":")[0]
            if file in file_mapping:
                G.add_edge(file_mapping[file], shared_var_node)

    # Draw the graph
    plt.figure(figsize=(14, 10))
    pos = nx.spring_layout(G, seed=42)
    nx.draw(
        G, pos, with_labels=True, node_size=3000, font_size=10, font_weight="bold", arrowsize=20
    )
    plt.title("Dependency Map with Shared Environment Variables")
    plt.savefig(output)
    plt.show()

if __name__ == "__main__":
    root_directory = "src"  # Adjust this to your project root directory

    # Build the dependency map
    dependency_map, file_mapping = build_dependency_map(root_directory)
    resolved_map = map_to_files(dependency_map, file_mapping)

    # Find shared environment variables
    env_variable_usage = find_env_variables(root_directory)
    shared_envs = {var: locations for var, locations in env_variable_usage.items() if len(locations) > 1}

    # Visualize the dependency map with shared environment variables
    visualize_dependency_map(resolved_map, file_mapping, shared_envs, output="tools/dependency_map.png")
