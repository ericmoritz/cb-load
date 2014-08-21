# coding: utf-8
import pandas as pd
import numpy as np
import sys
def main():
    filename_base = sys.argv[1]
    data = pd.read_csv(sys.stdin,
                       names=["elapsed", "error", "timestamp", "type"],
                       skipinitialspace=True, 
                       index_col='timestamp', 
                       parse_dates=True, 
                       dtype={"elapsed": "int64", "error": object, "type": object},
                       date_parser=pd.to_datetime)
    
    data.elapsed = data.elapsed / 1000000
    mean_sample = data.elapsed.resample("s", how="mean")
    counts = data.elapsed.resample("s", how="count")
    mean_sample.to_csv("{0}-elapsed-mean.csv".format(filename_base))
    counts.to_csv("{0}-counts.csv".format(filename_base))

if __name__ == '__main__':
    main()



