import MapReduce
import sys

"""
Consider a simple social network dataset consisting of a set of key-value pairs (person, friend) 
representing a friend relationship between two people. 
Describe a MapReduce algorithm to count the number of friends for each person.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line


def mapper(record):
    # key: person
    key = record[0]
    mr.emit_intermediate(key, 1)


def reducer(key, value):
    # key: person
    # value: 1 for each person-friend tuple occurrence
    friend_count = len(value)
    mr.emit((key, friend_count))

# Do not modify below this line
# =============================


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
