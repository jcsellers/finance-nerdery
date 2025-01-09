import os
import sys
import pkgutil

print("PYTHONPATH:", sys.path)
print("Modules:", sys.modules.keys())

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

print("PYTHONPATH:", sys.path)
print("Modules in src:", [module.name for module in pkgutil.iter_modules(['src'])])
