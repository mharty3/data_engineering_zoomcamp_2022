# simple script to stand in for data manipulation with pandas
# now it simply demonstration that docker is available in the container
#
# it uses sys.argv to pass command line arguments to the file in order to 
# parameterize the data pipeline

import pandas as pd
import sys

print(sys.argv)
day = sys.argv[1]

# some fancy stuff with pandas

print(f'job finished successfully for day = {day}')