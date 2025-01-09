import os
import pkgutil
import sys

print("PYTHONPATH:", sys.path)
print("Modules:", sys.modules.keys())

scripts_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../scripts")
)
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
)

modules = [module.name for module in pkgutil.iter_modules(["src"])]
print("Modules in src:", modules)
