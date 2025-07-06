import pandas
import sys

file_path = sys.argv[1]
df = pandas.read_csv(file_path)
df = df.sort_values(by='SetID')
df.to_csv(file_path, index=False)
print(f"Sorted {file_path} by SetID.")