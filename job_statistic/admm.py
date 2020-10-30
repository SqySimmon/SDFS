# coding: utf-8

import numpy as np

import matplotlib.pyplot as plt
from math import sqrt, log
import pickle
import sys
import os

np.set_printoptions(threshold=np.inf)

sys.path.append('/home/dsjxtjc/2018211149/')

from mapreduce.map_reduce import *

from utils import io_utils as iou

# default rho
DEFAULT_RHO = 0.1

"""
网上的ADMM1
"""


def ADMM(A, y):
    """Alternating Direction Method of Multipliers

    This is a python implementation of the Alternating Direction
    Method of Multipliers - a method of constrained optimisation
    that is used widely in statistics (http://stanford.edu/~boyd/admm.html).

    This is simplified version, specifically for the LASSO
    """

    m, n = A.shape
    print(A)
    # A*A'
    A_t_A = A.T.dot(A)
    # w 特征值 v 特征向量
    w, v = np.linalg.eig(A_t_A)
    # 最大的迭代次数
    MAX_ITER = 10000

    # Function to caluculate min 1/2(y - Ax) + l||x||
    # via alternating direction methods
    x_hat = np.zeros([n, 1])
    # z
    z_hat = np.zeros([n, 1])

    # u是累计残差
    u = np.zeros([n, 1])

    # Calculate regression co-efficient and stepsize
    # 求最大的特征值
    r = np.amax(np.absolute(w))
    # ？？？
    l_over_rho = sqrt(2 * log(n, 10)) * r / 2.0  # I might be wrong here
    # 残差的最小阈值
    rho = DEFAULT_RHO

    # Pre-compute to save some multiplications
    # A*y
    A_t_y = A.T.dot(y)

    # A*A' + ρI
    Q = A_t_A + rho * np.identity(n)
    # A*A' + ρI 求逆
    Q = np.linalg.inv(Q)
    # 包含本身的矩阵乘法
    Q_dot = Q.dot
    # 阶跃函数sign
    sign = np.sign
    #
    maximum = np.maximum
    absolute = np.absolute

    for _ in xrange(MAX_ITER):
        # (ATA+ρI)−1(ATy+ρ(zk−uk))
        x_hat = Q_dot(A_t_y + rho * (z_hat - u))
        # z minimisation via soft-thresholding
        u = x_hat + u
        # Πc(xk+1+uk) 对于lasso
        z_hat = sign(u) * maximum(0, absolute(u) - l_over_rho)
        # mulitplier update
        u = u + x_hat - z_hat

    print(A)
    return z_hat


def test(m=50, n=200):
    """Test the ADMM method with randomly generated matrices and vectors"""
    A = np.random.randn(m, n)

    num_non_zeros = 10
    positions = np.random.randint(0, n, num_non_zeros)
    amplitudes = 100 * np.random.randn(num_non_zeros, 1)
    x = np.zeros((n, 1))
    x[positions] = amplitudes

    y = A.dot(x) + np.random.randn(m, 1)

    r = ADMM(A, y)

    f = open('test_a', "w")
    f.write(str(r))
    f.close()
    # with open('lasso_test.pickle', 'wb') as f:
    #     pickle.dump(A, f, 0)
    # data = {}

    # data['A'] = A
    # iou.write_dict('lasso_test.txt', data)
    # plot(x, ADMM(A, y))


# def plot(original, computed):
#     """Plot two vectors to compare their values"""
#     plt.plot(original, label='Original')
#     plt.plot(computed, label='Estimate')
#
#     plt.legend(loc='upper right')

# plt.show()

def map(string_data_path, rou, last_data_path, res_data_path, reduce_num, map_id):
    # result = []
    # file_object = open(string_data_path)
    # for line in file_object.readlines():
    #     result.append((line.strip('\n'), 1))
    # file_object.close()

    # with open(string_data_path, 'rb') as f:
    #     data = pickle.load(f)
    data = iou.read_dict(string_data_path)
    # print(data)
    last_result = None
    state = True
    if os.path.exists(last_data_path):
        z = iou.read_dict(res_data_path)
        last_result = iou.read_dict(last_data_path)
        state = False
    else:
        z = np.zeros([200, 1])

        # u是累计残差
        last_result = {'ui': np.zeros([200, 1])}
    # x_hat = Q_dot(A_t_y + rho * (z_hat - u))

    A = [[1, 2, 3, 5, 6], [4, 5, 6], [7, 8, 9],]
    #
    b = 4

    num_non_zeros = 10
    # positions = np.random.randint(0, n, num_non_zeros)
    # amplitudes = 100 * np.random.randn(num_non_zeros, 1)
    # x = np.zeros((n, 1))
    # x[positions] = amplitudes

    # y = A.dot(data) + np.random.randn(50, 1)

    # z = ADMM(A, y)

    result = last_result if last_result else {}
    # if state:
    #     result['xi'] = np.linalg.inv(data.T * data + rou * np.eye(data.shape[1])) * (
    #             data.T * b + rou * (z - result['ui']))
    # else:
    result['xi'] = np.linalg.inv(data.T * data + rou * np.eye(data.shape[1])) * (
            data.T * b + rou * (z - result['ui']))

    result['ui'] = result['ui'] + result['xi'] - z
    # result['ui'] =
    # if os.path.exists(last_data_path):
    iou.write_dict(last_data_path, A)
    # else:
    # iou.write_dict('last_data_path', A)

    partition_admm(result, reduce_num, map_id)
    return 'one map success'


def reduce(string_data_path, data_output_path, rou):
    f = open(string_data_path)
    all_amp = f.read()
    # print(11111)
    data = eval(all_amp)

    # f = open(string_data_path)
    #  = f.read()
    # # print(11111)
    # data = eval(a)

    z_c = np.sum(data['xi']) + np.sum(data['ui'])

    finish_one_admm(string_data_path, z_c, data_output_path)
    return ' one reduce success'


if __name__ == "__main__":
    test()

    # with open('lasso_test.pickle', 'wb') as f:
    #     pickle.dump({111:111, 222:np.zeros(10)}, f, 0)
    #
    # with open('lasso_test.pickle', 'rb') as f:
    #     data = pickle.load(f)
    # print(data)
    # print(type(data))

"""————————————————————————分割线——————————————————————"""

"""网上的ADMM2"""

# class ADMM_method():
#     def __init__(self, A, b, mu, init_iteration, max_iteration, tol):
#         self.A = A
#         self.AT = self.A.T
#         self.b = b
#         self.m, self.n = self.A.shape
#         self.mu = mu
#         self.init_iteration = init_iteration
#         self.max_iteration = max_iteration
#         self.tol = tol
#         self.AAT = np.dot(self.A, self.AT)
#         self.ATb = np.dot(self.A.T, self.b)
#         self.cov = np.dot(self.AT, self.A)
#         self.step_size = 1.0 / np.linalg.norm(self.cov, 2)
#         self.coef = np.linalg.inv(np.eye(m) + 1.0 * self.AAT)
#         self.result_path = []
#
#     def loss(self, x):
#         x = x.reshape(-1)
#         return 0.5 * np.sum(np.square(np.dot(self.A, x) - self.b)) + self.mu * np.sum(np.abs(x))
#
#     def train(self, method="dual"):
#         import time
#         start_time = time.time()
#         print method + ' is Solving...'
#
#         if method == "dual":
#             # initial weights
#             self.y = np.random.normal(size=(self.m))
#             self.z = np.dot(self.AT, self.y)
#             self.x = np.zeros(self.n)
#
#             def proj_inf_norm(z, uu):
#                 v = 1.0 * z[:]
#                 v[z >= uu] = 1.0 * uu
#                 v[z <= -uu] = -1.0 * uu
#                 return v
#
#             def update(y, z, w, uu, t):
#                 z = np.dot(self.AT, y) + w / t
#                 z = proj_inf_norm(z, uu)
#                 y = np.dot(self.coef, self.b + t * np.dot(self.A, z - w / t))
#                 w = w + t * (np.dot(self.AT, y) - z)
#                 return y, z, w
#
#             self.iters = 1
#             self.err_rate = 1.0
#             new_max_iteration = self.max_iteration + 6 * self.init_iteration
#             while (self.err_rate > self.tol and self.iters < new_max_iteration):
#                 self.result_path.append(self.loss(self.x))
#                 x_ = self.x
#                 self.y, self.z, self.x = update(self.y, self.z, self.x, self.mu, t=1.0)
#                 self.err_rate = np.abs(self.loss(self.x) - self.loss(x_)) / self.loss(x_)
#                 self.iters += 1
#
#         elif method == "dal":
#             # initial weights
#             self.y = np.random.normal(size=(self.m))
#             self.z = np.dot(self.AT, self.y)
#             self.x = np.zeros(self.n)
#
#             def proj_inf_norm(z, uu):
#                 v = 1.0 * z[:]
#                 v[z >= uu] = 1.0 * uu
#                 v[z <= -uu] = -1.0 * uu
#                 return v
#
#             def update(y, z, w, uu, t):
#                 for i in range(2):
#                     z = np.dot(self.AT, y) + w / t
#                     z = proj_inf_norm(z, uu)
#                     y = np.dot(self.coef, self.b + t * np.dot(self.A, z - w / t))
#                 w = w + t * (np.dot(self.AT, y) - z)
#                 return y, z, w
#
#             self.iters = 1
#             self.err_rate = 1.0
#             new_max_iteration = self.max_iteration + 6 * self.init_iteration
#             while (self.err_rate > self.tol and self.iters < new_max_iteration):
#                 self.result_path.append(self.loss(self.x))
#                 x_ = self.x
#                 self.y, self.z, self.x = update(self.y, self.z, self.x, self.mu, t=1.0)
#                 self.err_rate = np.abs(self.loss(self.x) - self.loss(x_)) / self.loss(x_)
#                 self.iters += 1
#
#         elif method == "linear":
#             # initial weights
#             self.x = np.random.normal(size=(self.n))
#             self.y = np.dot(self.A, self.x)
#             self.z = np.zeros(self.m)
#
#             def soft_thresholding(x, h):
#                 y = 1.0 * x[:]
#                 y[x >= h] = 1.0 * (y[x >= h] - h)
#                 y[x <= -h] = 1.0 * (y[x <= -h] + h)
#                 y[np.abs(x) <= h] = 0.0
#                 return y
#
#             def update(x, y, z, u, t):
#                 grad = t * np.dot(self.cov, x) - t * np.dot(self.AT, self.b + y - z / t)
#                 x = soft_thresholding(x - self.step_size * grad, self.step_size * u)
#                 y = (t * np.dot(self.A, x) + z - t * self.b) / (1.0 + t)
#                 z = z + t * (np.dot(self.A, self.x) - self.b - y)
#                 return x, y, z
#
#             for hot_mu in [1e3, 1e2, 1e1, 1e-1, 1e-2, 1e-3]:
#                 for k in range(self.init_iteration):
#                     self.x, self.y, self.z = update(self.x, self.y, self.z, hot_mu, t=1.0)
#                     self.result_path.append(self.loss(self.x))
#
#             self.iters = 1
#             self.err_rate = 1.0
#             while (self.err_rate > self.tol and self.iters < self.max_iteration):
#                 self.result_path.append(self.loss(self.x))
#                 x_ = self.x
#                 self.x, self.y, self.z = update(self.x, self.y, self.z, hot_mu, t=1.0)
#                 self.err_rate = np.abs(self.loss(self.x) - self.loss(x_)) / self.loss(x_)
#                 self.iters += 1
#
#         else:
#             print "Such method is not yet supported!!"
#
#         self.run_time = time.time() - start_time
#         print 'End!'
#
#     def plot(self, method='dual'):
#         # from bokeh.plotting import figure, output_file, show
#         x = range(len(self.result_path))
#         y = self.result_path
#         # output_file("./admm_" + method + ".html")
#         # p = figure(title="ADMM Method_" + method, x_axis_label='iteration', y_axis_label='loss')
#         # p.line(x, y, legend=method, line_width=2)
#         # show(p)

# if __name__ == '__main__':
#     import numpy as np
#
#     # from bokeh.plotting import figure, output_file, show
#
#     # for reproducibility
#     np.random.seed(1337)
#
#     n = 1024
#     m = 512
#     mu = 1e-3
#     init_iteration = int(1e3)
#     max_iteration = int(1e3)
#     tol = 1e-9
#
#     # Generating test matrices
#     A = np.random.normal(size=(m, n))
#     u = np.random.normal(size=(n)) * np.random.binomial(1, 0.1, (n))
#     b = np.dot(A, u).reshape(-1)
#
#     result_time = []
#     result_mse = []
#     # output_file("./ADMM.html")
#     # p = figure(title="ADMM Method", x_axis_label='iteration', y_axis_label='loss')
#
#     for method, color in zip(["dual", "dal", "linear"], ["orange", "red", "blue"]):
#         model = ADMM_method(A, b, mu, init_iteration, max_iteration, tol)
#         model.train(method)
#         result_time.append(model.run_time)
#         result_mse.append(np.mean(np.square(model.x - u)))
#         x = range(len(model.result_path))
#         y = model.result_path
#         # p.line(x, y, legend=method, line_width=2, line_color=color)

# show(p)
