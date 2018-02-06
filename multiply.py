import MapReduce
import sys

"""
Assume you have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. 
Design a MapReduce algorithm to compute the matrix multiplication A x B
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: cell identifier
    # value: cell value
    matrix, row, col, value = record
    for n in range(5):
        if matrix == 'a':
            cell = (row, n)
            mat = 'A'
            col_row = col
        else:
            cell = (n, col)
            mat = 'B'
            col_row = row
        mr.emit_intermediate(cell, (mat, col_row, value))


def reducer(key, list_of_values):
    # key: cell identifier
    # value: cell value
    matrix_a = [(item[1], item[2]) for item in list_of_values if item[0] == 'A']
    matrix_b = [(item[1], item[2]) for item in list_of_values if item[0] == 'B']
    result = 0

    for A_item in matrix_a:
        for B_item in matrix_b:
            if A_item[0] == B_item[0]:
                result += A_item[1] * B_item[1]

    if result != 0:
        mr.emit((key[0], key[1], result))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)