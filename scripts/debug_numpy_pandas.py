import numpy as np
import pandas as pd

# Test Numpy
print("Numpy version:", np.__version__)
arr = np.array([True, False, True])
print("Numpy array:", arr)

# Test Pandas
print("Pandas version:", pd.__version__)
df = pd.DataFrame({"bool_col": [True, False, True]})
print("Pandas DataFrame:", df)
