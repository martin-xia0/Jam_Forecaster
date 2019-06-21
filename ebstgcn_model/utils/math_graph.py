import numpy as np
import pandas as pd
from scipy.sparse.linalg import eigs
from os.path import join as pjoin

def scaled_laplacian(W):
    '''
    Normalized graph Laplacian function.
    :param W: np.ndarray, [n_route, n_route], weighted adjacency matrix of G.
    :return: np.matrix, [n_route, n_route].
    '''
    # d ->  diagonal degree matrix
    n, d = np.shape(W)[0], np.sum(W, axis=1)
    # L -> graph Laplacian
    L = -W
    L[np.diag_indices_from(L)] = d
    for i in range(n):
        for j in range(n):
            if (d[i] > 0) and (d[j] > 0):
                L[i, j] = L[i, j] / np.sqrt(d[i] * d[j])
    # lambda_max \approx 2.0, the largest eigenvalues of L.
    lambda_max = eigs(L, k=1, which='LR')[0][0].real
    return np.mat(2 * L / lambda_max - np.identity(n))


def cheb_poly_approx(L, Ks, n):
    '''
    Chebyshev polynomials approximation function.
    :param L: np.matrix, [n_route, n_route], graph Laplacian.
    :param Ks: int, kernel size of spatial convolution.
    :param n: int, number of routes / size of graph.
    :return: np.ndarray, [n_route, Ks*n_route].
    '''
    L0, L1 = np.mat(np.identity(n)), np.mat(np.copy(L))

    if Ks > 1:
        L_list = [np.copy(L0), np.copy(L1)]
        for i in range(Ks - 2):
            Ln = np.mat(2 * L * L1 - L0)
            L_list.append(np.copy(Ln))
            L0, L1 = np.matrix(np.copy(L1)), np.matrix(np.copy(Ln))
        # L_lsit [Ks, n*n], Lk [n, Ks*n]
        return np.concatenate(L_list, axis=-1)
    elif Ks == 1:
        return np.asarray(L0)
    else:
        raise ValueError('ERROR: the size of spatial kernel must be greater than 1, but received "{}".'.format(Ks))


def first_approx(W, n):
    '''
    1st-order approximation function.
    :param W: np.ndarray, [n_route, n_route], weighted adjacency matrix of G.
    :param n: int, number of routes / size of graph.
    :return: np.ndarray, [n_route, n_route].
    '''
    A = W + np.identity(n)
    d = np.sum(A, axis=1)
    sinvD = np.sqrt(np.mat(np.diag(d)).I)
    # refer to Eq.5
    return np.mat(np.identity(n) + sinvD * A * sinvD)


def weight_matrix(n, edge_based=True, sigma2=10, epsilon=0.5, scaling=True):
    '''
    Load weight matrix function.
    :param file_path: str, the path of saved weight matrix file.
    :param sigma2: float, scalar of matrix W.
    :param epsilon: float, thresholds to control the sparsity of matrix W.
    :param scaling: bool, whether applies numerical scaling on W.
    :return: np.ndarray, [n_route, n_route].
    '''
    if edge_based:
        file_path = pjoin('./dataset', 'W_{}_E.csv'.format(n))
    else:
        file_path = pjoin('./dataset', 'W_{}_P.csv'.format(n))
    W = pd.read_csv(file_path, header=None).values
    print("weight matrix {}".format(file_path))
    print("row weight matrix {}".format(W))
    # check whether W is a point-based or edge-based matrix.
    if edge_based:
        print('The input graph is a edge-based graph')
        print("processed weight matrix {}".format(W))
        return W
    else:
        print('The input graph is a point-based graph')
        n = W.shape[0]
        W = W / 1000
        W2, W_mask = W * W, np.ones([n, n])-np.identity(n)
        # refer to Eq.10
        W_new = np.exp(-W2 / sigma2) * (np.exp(-W2 / sigma2) >= epsilon) * W_mask
        print("processed weight matrix {}".format(W_new))
        return W_new
