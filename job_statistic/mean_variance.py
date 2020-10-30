# coding=utf-8

import sys
import numpy as np
import os

import utils.io_utils as iou

np.set_printoptions(threshold=np.inf)

sys.path.append('/home/dsjxtjc/2018211149/')

from mapreduce.map_reduce import partition, combine, finish


# TODO 增加ADMM参数"rou"
# TODO 指定输出的dict必须为指定的key：xi,ui，文件名还是以mapid命名
# TODO map还得知道上一次优化求得的最优的xi ui，第一次没有是None
def map(string_data_path, rou, last_data_path, res_data_path, reduce_num, map_id):
    result = []
    if last_data_path:
        last_data = iou.read_dict(last_data_path)

    file_object = open(string_data_path)
    for line in file_object.readlines():
        result.append((line.strip('\n'), 1))
    file_object.close()

    result_dict = {}
    result_final = []
    for i in result:
        result_dict[i[0]] = 0

    for i in result:
        result_dict[i[0]] = result_dict[i[0]] + 1

    for i in result_dict.iterkeys():
        result_final.append((i, result_dict[i]))
    print(len(result_final))

    partition(combine(result_final), reduce_num, map_id)
    return 'map success'


# TODO 增加ADMM参数"rou"
def reduce(string_data_path, data_output_path, rou):
    """
    计算均值，方差
    :param string_data_path:
    :return:
    """
    print(string_data_path)
    f = open(string_data_path)
    a = f.read()
    # print(11111)
    data = eval(a)
    print(len(data))
    f.close()
    data_count = {}
    avg = 0.0
    var = 0.0
    for d in data.keys():
        s = sum(data[d])
        data_count[d] = s
        avg = avg + s
    avg = avg / len(data_count)
    for c in data_count.keys():
        var = var + (avg - data_count[c]) * (avg - data_count[c])
    var = var / len(data_count)
    print(avg)
    print(var)
    result = {}
    result['avg'] = avg
    result['var'] = var
    finish(result, data_output_path)
    return 'reduce success'


if __name__ == '__main__':
    # combine()
    # print(sys.argv)
    if len(sys.argv) != 0:
        if sys.argv[1] == 'map':
            print(map(sys.argv[2], sys.argv[3], sys.argv[4]))
        elif sys.argv[1] == 'reduce':
            print(reduce(sys.argv[2], sys.argv[3]))
            # print('1\n2\n3')
            # map('1\n2\n3')
