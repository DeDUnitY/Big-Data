from collections import defaultdict


def mapper_matrix(A, B):
    mapped = []
    for i in range(len(A)):
        for j in range(len(B[0])):
            partials = []
            for k in range(len(B)):
                partials.append(A[i][k] * B[k][j])
            mapped.append(((i, j), partials))
    return mapped


def shuffle_matrix(mapped):
    grouped = defaultdict(list)
    for key, values in mapped:
        grouped[key].extend(values)
    return grouped


def reducer_matrix(grouped):
    result = {}
    for key, values in grouped.items():
        result[key] = sum(values)
    return result


def matrix_multiply_mr(A, B):
    mapped = mapper_matrix(A, B)
    shuffled = shuffle_matrix(mapped)
    reduced = reducer_matrix(shuffled)

    n, k = len(A), len(B[0])
    C = [[0]*k for _ in range(n)]
    for (i, j), val in reduced.items():
        C[i][j] = val
    return C


if __name__ == "__main__":
    A = [[1, 2], [3, 4]]
    B = [[5, 6], [7, 8]]
    C = matrix_multiply_mr(A, B)

    for row in C:
        print(row)
