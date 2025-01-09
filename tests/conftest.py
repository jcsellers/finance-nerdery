import sys
import pkgutil

print("PYTHONPATH:", sys.path)
print("Modules:", sys.modules.keys())


print("PYTHONPATH:", sys.path)
print("Modules in src:", [module.name for module in pkgutil.iter_modules(['src'])])
