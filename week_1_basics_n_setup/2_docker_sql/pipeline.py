
import pandas as pd

# Feel free to write Pandas code here
# But the import alone makes sure Pandas is being installed properly

print("Pandas installation was successful! Yeah!")
    
df = pd.read_csv("dataset.csv")

print(df.dtypes)