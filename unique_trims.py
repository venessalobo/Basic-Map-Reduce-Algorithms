import MapReduce
import sys

"""
Consider a set of key-value pairs where each key is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....
Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line


def mapper(record):
    # key: trimmed sequence
    # value: occurrence
    key = record[1][:-10]
    mr.emit_intermediate(key, 1)


def reducer(key, value):
    # key: trimmed sequence
    # value: 1 for each trimmed sequence
    mr.emit(key)

# Do not modify below this line
# =============================


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
