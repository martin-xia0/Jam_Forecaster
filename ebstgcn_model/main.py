import os

os.environ["CUDA_VISIBLE_DEVICES"] = "0"
from os.path import join as pjoin

import tensorflow as tf

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
tf.Session(config=config)

from utils.math_graph import *
from data_loader.data_utils import *
from models.trainer import model_train
from models.tester import model_test

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--n_route', type=int, default=43)
parser.add_argument('--n_his', type=int, default=12)
parser.add_argument('--n_pred', type=int, default=9)
parser.add_argument('--batch_size', type=int, default=20)
parser.add_argument('--epoch', type=int, default=80)
parser.add_argument('--save', type=int, default=10)
parser.add_argument('--ks', type=int, default=3)
parser.add_argument('--kt', type=int, default=3)
parser.add_argument('--lr', type=float, default=1e-3)
parser.add_argument('--opt', type=str, default='RMSProp')
parser.add_argument('--inf_mode', type=str, default='merge')

parser.add_argument('--edge_based', type=bool, default=True)

args = parser.parse_args()
print('Training configs: {}'.format(args))

n, n_his, n_pred = args.n_route, args.n_his, args.n_pred
Ks, Kt = args.ks, args.kt
# blocks: settings of channel size in st_conv_blocks / bottleneck design
blocks = [[1, 32, 64], [64, 32, 128]]

# Load wighted adjacency matrix W
W = weight_matrix(n, args.edge_based)

# Calculate graph kernel
L = scaled_laplacian(W)
# Alternative approximation method: 1st approx - first_approx(W, n).
Lk = cheb_poly_approx(L, Ks, n)
tf.add_to_collection(name='graph_kernel', value=tf.cast(tf.constant(Lk), tf.float32))

# Data Preprocessing
data_file = 'V_{}.csv'.format(n)
n_train, n_val, n_test = 34, 5, 5
PeMS = data_gen(pjoin('./dataset', data_file), (n_train, n_val, n_test), n, n_his + n_pred)
print(PeMS)
print('>> Loading dataset with Mean: {:.2f}, STD: {:.2f}'.format(PeMS.mean, PeMS.std))

if __name__ == '__main__':
    model_train(PeMS, blocks, args)
    model_test(PeMS, PeMS.get_len('test'), n_his, n_pred, args.inf_mode)
