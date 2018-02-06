import MapReduce
import sys

"""
The relationship "friend" is often symmetric, meaning that if I am your friend, you are my friend. 
Implement a MapReduce algorithm to check whether this property holds. 
Generate a list of all non-symmetric friend relationships.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line


def mapper(record):
    # key1: given tuple (person a, person b)
    # key1: inverted tuple (person b, person a)
    # value1: 1
    # value2: 1
    person = record[0]
    friend = record[1]
    mr.emit_intermediate((person, friend), 1)
    mr.emit_intermediate((friend, person), 1)


def reducer(key, value):
    # key: (person a, person b)
    # value: 1 for each person-friend tuple occurrence
    if len(value) < 2:
        mr.emit(key)

# Do not modify below this line
# =============================


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
