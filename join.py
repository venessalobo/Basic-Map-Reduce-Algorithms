import MapReduce
import sys

"""
Implement a relational join as a MapReduce query
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line


def mapper(record):
    # key: table identifier : order_id
    # value: all table attributes
    key = record[1]
    value = list(record)
    mr.emit_intermediate(key, value)


def reducer(key, value):
    # key: order_id
    # value: entire record
    for index in range(1, len(value)):
        mr.emit(value[0]+value[index])

# Do not modify below this line
# =============================


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
