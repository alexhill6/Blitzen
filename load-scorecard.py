import sys
import pandas as pd

scorecard_df = pd.read_csv(sys.argv[1])

year = int(sys.argv[1][-14:-10]) + 1

print(year)