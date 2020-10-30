# coding=utf-8

''' mapper of pangerank algorithm'''
import sys
import numpy as np
import os

sys.path.append('/home/dsjxtjc/2018211149/')

from mapreduce.map_reduce import partition, combine, finish


def map(string_data_path, reduce_num, map_id):
    id1 = id2 = None
    heros = value = None
    count1 = count2 = 0

    result = []
    file_object = open(string_data_path)
    for line in file_object.readlines():
        result.append((line.strip('\n'), 1))
    file_object.close()

    for line in sys.stdin:
        data = line.strip().split('\t')
        if len(data) == 3 and data[1] == 'a':  # This is the pangerank value
            count1 += 1
            if count1 >= 2:
                print '%s\t%s' % (id1, 0.0)

            id1 = data[0]
            value = float(data[2])
        else:  # This the link relation
            id2 = data[0]
            heros = data[1:]
        if id1 == id2 and id1:
            v = value / len(heros)
            for hero in heros:
                print '%s\t%s' % (hero, v)
            print '%s\t%s' % (id1, 0.0)
            id1 = id2 = None
            count1 = 0

    partition(combine(result_final), reduce_num, map_id)
    return 'map success'


''' reducer of pagerank algorithm'''


def reduce(string_data_path, data_output_path):
    print(string_data_path)
    f = open(string_data_path)
    a = f.read()
    # print(11111)
    data = eval(a)
    print(len(data))
    f.close()
    last = None
    values = 0.0
    alpha = 0.8
    N = 4  # Size of the web pages
    for line in sys.stdin:
        data = line.strip().split('\t')
        hero, value = data[0], float(data[1])
        if data[0] != last:
            if last:
                values = alpha * values + (1 - alpha) / N
                print '%s\ta\t%s' % (last, values)
            last = data[0]
            values = value
        else:
            values += value  # accumulate the page rank value
    if last:
        values = alpha * values + (1 - alpha) / N
        print '%s\ta\t%s' % (last, values)
    finish(result, data_output_path)
