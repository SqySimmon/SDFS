# coding=utf-8


import os
import sys
import commands
from multiprocessing import Pool
import time
import pickle
import numpy as np
import math

np.set_printoptions(threshold=np.inf)

sys.path.append('/home/dsjxtjc/2018211149/')

import utils.io_utils as iou

import dfs_sqy.dfs as dfs

import client.dfs_client as dfs_client

# job脚本保存路径
DEFAULT_JOB_PATH = '/home/dsjxtjc/2018211149/mr_job'
# 中间结果保存路径s
DEFAULT_INTERMEDIATE_DATA_PATH = '/home/dsjxtjc/2018211149/mr_inter_data'

DEFAULT_MAP_REDUCE_PATH = '/home/dsjxtjc/2018211149/mapreduce/map_reduce.py'

"""
1.基于DFS的map reduce，master就是DFS的master(namenode)，所有的slave节点和配置的datanode相同
2.框架内部定义的是map 和 reduce 的执行流程
    根据用户输入的DFS文件目录，取得DFS文件对应的chunk，在chunk所在的slave上执行map
    map：map中执行用于定义的job的map，并将中间结果写到特定的中间结果文件夹（所有slave相同的本地磁盘）partition，combine
    combine：将一个key的value合并成list
    partition：根据key决定哪些key对应哪些reduce（写死0,1，2，3），进行合并，并追加写到中间结果文件夹
        以reduce编号命名的文件中（0,1，2，3，4）
    reduce：从中间结果文件夹（循环？？？）中取出根自己编号相同中间结果文件（如果不在本机scp到本机），
        并执行用户定义的reduce，调用dfs的方法将结果（追加）保存在master中的特定的结果文件夹中，scp
    finish：从特定的结果文件夹中将结果copyFromLocal上传到DFS
    执行用户可以配置reduce的数量，对于均值方差，只能是1，因为要得到全部的数据
3.框架内部提供提交job脚本（python文件）的函数——将job脚本scp到各个slave的特定的保存脚本的文件夹（每个slave都相同）
4.用户提交的job——python脚本，必须按照固定的结构：map，reduce，map和reduce的输入和输出必须按照确定的格式
5.用户定义的map 和 reduce
    map：
    输入：数据块，字符串
    输出：list(key,value)对,dict
    reduce：
    输入：key,list(value)对,dict
    输出：list(key,value)对
6.注：
    1.Hadoop map reduce 中在map之后把相同key的value combine成一个key,list(value)，本文中使用相同的操作
    2.本文的split基于DFS已经划分好的chunk，所以没有操作
"""


def init():
    settings = dfs.read_dfs_setting()
    for s in settings['nodes']:
        commands.getstatusoutput('ssh ' + s + ' mkdir ' + DEFAULT_JOB_PATH)
        commands.getstatusoutput('ssh ' + s + ' mkdir ' + DEFAULT_INTERMEDIATE_DATA_PATH)
        commands.getstatusoutput('ssh ' + s + ' mkdir ' + DEFAULT_ADMM_PARA_PATH)


# TODO 增加了ADMM的参数"rou"
# TODO 增加了ADMM的参数"repeat_state"，用于判断是否是迭代的执行
def do_job(python_job_local_path, file_path, dfs_path, rou, ftype='dfs', reduce_num=1):
    """
    执行提交job，将本地python脚本scp到各个slave节点，可能有的slave并未执行
    """
    settings = dfs.read_dfs_setting()
    if not os.path.exists(python_job_local_path):
        return 'job file does not exist'
    if not os.path.isfile(python_job_local_path):
        return 'job is not a file'
    if not os.access(python_job_local_path, os.R_OK):
        return 'job file can not read'
    else:
        # TODO 还要把上次的python的脚本记录保存下来
        record = {}
        record['job_path'] = python_job_local_path
        record['file_path'] = file_path
        record['dfs_path'] = dfs_path
        record['rou'] = rou
        record['ftype'] = ftype
        record['reduce_num'] = reduce_num
        iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/job_record', record)
    for i in settings['nodes']:
        commands.getstatusoutput('scp ' + python_job_local_path + ' ' + i + ':' + DEFAULT_JOB_PATH)

    # 调用shell，并行执行map，默认的dfs路径
    if ftype == 'dfs':
        dfs_chunks = dfs.read_dfs_dfs_chunk()
        if file_path not in dfs_chunks.keys():
            return 'data file does not exist'
        else:
            # TODO 将参数rou写到本地文件
            set_admm_para('rou', rou)

            # 检查chunk错误
            dfs.check_chunk_client(settings['master'], file_path)
            start_time = time.time()
            # current_para = {}
            # current_para['rou'] = rou
            # current_para['N'] = rou
            # current_para['rou'] = rou
            # current_para['rou'] = rou
            # iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/z_last', z_new)

            # 并行执行map
            slaves = multi_maps_dfs(DEFAULT_JOB_PATH + '/' + os.path.basename(python_job_local_path),
                                    dfs_chunks[file_path], rou, reduce_num)
            # TODO 添加了一个初始化参数——N，还有rho、dual residual threshold、primal residual threshold
            set_admm_para('N', len(slaves))

            # print(slaves)
            # 并行的reduce
            # slaves = ['192.168.0.101', 'thumm04', 'thumm03', 'thumm05', 'thumm02', 'thumm04', '192.168.0.101',
            #           'thumm03', 'thumm05', 'thumm02', 'thumm04', '192.168.0.101', 'thumm03', 'thumm05', 'thumm02',
            #           'thumm04', '192.168.0.101', 'thumm03', 'thumm05', 'thumm02', 'thumm04', '192.168.0.101',
            #           'thumm03', 'thumm05', 'thumm02', 'thumm04', '192.168.0.101', 'thumm03', 'thumm05', 'thumm02',
            #           'thumm04', '192.168.0.101', 'thumm03', 'thumm05', 'thumm02', 'thumm04', '192.168.0.101',
            #           'thumm03', 'thumm05', 'thumm02', 'thumm04']
            multi_reduces_dfs(DEFAULT_JOB_PATH + '/' + os.path.basename(python_job_local_path),
                              dfs_path,
                              slaves,
                              rou,
                              reduce_num)

            return str(time.time() - start_time)


# 用户提交job——client代码
# TODO 增加了ADMM的参数"rou"
def submit_client(python_job_local_path, file_path, dfs_path, rou, epilo_dual, epilo_prim,
                  ftype='dfs', reduce_num=1):
    settings = dfs.read_dfs_setting()
    if not os.path.exists(python_job_local_path):
        return 'job file does not exist'
    if not os.path.isfile(python_job_local_path):
        return 'job is not a file'
    if not os.access(python_job_local_path, os.R_OK):
        return 'job file can not read'
    # 提交到master
    commands.getstatusoutput('scp ' + python_job_local_path + ' '
                             + settings['master'] + ':' + DEFAULT_JOB_PATH)

    os.system('ssh ' + settings['master'] + ' python ' + DEFAULT_MAP_REDUCE_PATH
              + ' do_job ' + python_job_local_path + ' '
              + file_path + ' '
              + dfs_path + ' '
              + rou + ' '
              + ftype + ' '
              + str(reduce_num))


# TODO 增加ADMM参数"rou"，增加上次执行
# TODO 这块有个问题：hash对于相同的slave的mapid相同
def multi_maps_dfs(job_func, dfs_chunks, rou, reduce_num=1):
    """
    执行与文件chunk数相同数量的map
    :param job_func: 全部slave统一的默认的job python脚本位置
    :param dfs_chunks:所有的文件chunk 编号list
    :param admm_para_path: ADMM 的本地参数路径
    :param reduce_num: partition中需要reduce的数量
    :return:
    """
    # 执行map时，触发一个异步的执行chunk检查
    chunk_locals = dfs.read_dfs_chunk_local()
    chunk_local_paths = []
    for c in dfs_chunks:
        if c in chunk_locals.iterkeys():
            # 只取一个有效的chunk地址
            for chunk in chunk_locals[c]:
                ccc = chunk.split(':')
                if commands.getstatusoutput('ping -c1 -w1 ' + ccc[0])[0] == 0:
                    if str(commands.getstatusoutput('ssh ' + ccc[0] +
                                                    ' "[ ! -e +' + ccc[1] + ']" echo 1')[1]) != 1:
                        chunk_local_paths.append(chunk)
                        break
        else:
            # 没有数据直接终止
            sys.exit(1)
    p = Pool(len(chunk_local_paths))
    slaves = []

    for c in chunk_local_paths:
        slave_paths = c.split(':')
        slaves.append(slave_paths[0])
        map_id = hash(c)
        # if repeat_state:
        # TODO 这块就相当于固定的地址
        # last_map_data_path = str(reduce_num) + '_' + str(map_id)
        # else:
        last_map_data_path = str(reduce_num) + '_' + str(map_id)
        # TODO 没有加map任务异常的处理机制
        p.apply_async(do_map_dfs,
                      args=(job_func,
                            slave_paths[1],
                            slave_paths[0],
                            map_id,
                            rou,
                            last_map_data_path,
                            reduce_num,))

    print('mapping... ')
    p.close()
    p.join()
    print('map done')
    return slaves


# TODO 增加ADMM参数"rou"
def multi_reduces_dfs(job_func, dfs_path, slaves, rou, reduce_num):
    """
    执行与partition之后的文件数相同数量的reduce
    :param job_func: 全部slave统一的默认的job python脚本位置
    :param slaves: !!!!只在slave[0]
    :param reduce_num: 根据reduce num 和中间文件的命名规则，找到对应的文件
    :return:
    """
    # print(chunk_local_paths)
    # print(job_func)
    slaves = list(set(slaves))
    # print(slaves)
    p = Pool(reduce_num)
    # TODO 将slave[0]之外的所有slave的map输出的临时data合并到slave[0]的中间结果文件夹中
    # if reduce_num > len(slaves):
    # reduce_num = len(slaves)
    for r in range(reduce_num):
        map_data_paths = []
        for s in range(len(slaves)):
            if r != s:
                map_data_paths.append(slaves[s] + ':' + DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(r) + '*')

        for m in range(len(map_data_paths)):
            # print('ssh ' + slaves[0] + ' python /home/dsjxtjc/2018211149/mapreduce/map_reduce.py combine_reduce '
            #       + str(m))
            os.system('ssh ' + slaves[r % len(slaves)]
                      + ' python /home/dsjxtjc/2018211149/mapreduce/map_reduce.py combine_reduce '
                      + str(map_data_paths[m]) + ' '
                      + str(r))

    for d in range(reduce_num):
        slave_paths = DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d)
        # TODO 没有加reduce任务异常的处理机制，只在某个slave上执行全部的reduce task
        p.apply_async(do_reduce_dfs, args=(job_func, slave_paths, dfs_path, slaves[d % len(slaves)], rou,))
    print('reducing... ')
    p.close()
    p.join()
    print('reducing done')


# def print_result(result):
#     print(result)

# TODO 增加了ADMM参数rou
def do_map_dfs(job_func, file_path, slave, map_id, rou, last_map_data_path, reduce_num=1):
    """
    执行一个map，ssh
    :param job_func: job map 脚本位置
    :param file_path: 数据文件位置
    :param slave: slave 节点ip
    :param reduce_num: reduce 数目
    :param rou: ADMM参数rou
    :return:
    """
    # TODO 增加ADMM参数"rou"

    # TODO 把上次的z传过去
    if os.path.exists(DEFAULT_ADMM_PARA_PATH + '/z_last'):
        os.system('scp ' + DEFAULT_ADMM_PARA_PATH + '/z_last' + ' ' + slave + ':'
                  + DEFAULT_ADMM_PARA_PATH + '/z' + str(map_id))
    os.system('ssh ' + slave
              + ' python ' + job_func + ' map ' + file_path + ' ' + rou + ' ' + last_map_data_path + ' '
              + DEFAULT_ADMM_PARA_PATH + '/z' + str(
        map_id) + ' ' + str(
        reduce_num) + ' ' + str(map_id))


# def partition(data, reduce_num, map_id):
#     # print(len(data))
#     data_files = {}
#     for i in range(int(reduce_num)):
#         data_files[i] = {}
#
#     for k in data.iterkeys():
#         # partition_result[k] = hash(k) % reduce_num
#         data_files[hash(k) % int(reduce_num)][k] = data[k]
#
#     # 保存partition的属于各个reduce的中间结果文件
#     # 文件名就是reduce的编号，内容是：list((key,list(value)))
#     for d in data_files.iterkeys():
#         # print(len(data_files[d]))
#         # output = open(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d), 'wb')
#         # pickle.dump(str(data_files[d]), output)
#         # output.close()
#         iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d) + '_' + str(map_id),
#                        str(data_files[d]))
#     # TODO 一个partition只是包含部分key，写到中间结果文件
#     # iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/partition.txt', partition_result)
#     print('partition success')

# TODO ADMM partition 如果设置了多个reduce，那么所有的参数在所有的reduce上都保存一样的
def partition(data, reduce_num, map_id):
    # print(len(data))
    data_files = {}
    for i in range(int(reduce_num)):
        data_files[i] = data

    for k in data.iterkeys():
        # partition_result[k] = hash(k) % reduce_num
        data_files[hash(k) % int(reduce_num)][k] = data[k]

    # 保存partition的属于各个reduce的中间结果文件
    # 文件名就是reduce的编号，内容是：list((key,list(value)))
    for d in data_files.iterkeys():
        # print(len(data_files[d]))
        # output = open(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d), 'wb')
        # pickle.dump(str(data_files[d]), output)
        # output.close()
        iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d) + '_' + str(map_id),
                       str(data_files[d]))
    # TODO 一个partition只是包含部分key，写到中间结果文件
    # iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/partition.txt', partition_result)
    print('partition success')


"""
TODO 每个slave上的代码
"""


# TODO ADMM的map combine没有作用
def combine(data):
    # 把相同key的value组成list
    cb_data = {}
    for d in data:
        cb_data[d[0]] = []

    for d in data:
        cb_data[d[0]].append(d[1])
    if cb_data.has_key(''):
        cb_data.pop('')
    print('map combine success')
    return cb_data


# TODO 此代码在reduce task slave 上执行
# TODO ADMM的combine把所有的xi所有的ui合在一起
def combine_reduce(map_data_path, reduce_id):
    # 将map的临时文件汇总
    print('combine_reduce begin')
    files = []
    files_tmp = os.listdir(DEFAULT_INTERMEDIATE_DATA_PATH)
    for ft in files_tmp:
        if os.path.isfile(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + ft):
            if '_' in ft and ft[0:ft.rfind('_')] == str(reduce_id):
                files.append(ft)
    # 读取全部的文件
    ff_result = None
    for ff in files:
        ff_result = merge_dict(iou.read_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + ff), ff_result)
    # 创建临时文件夹，并把其他节点上的map临时文件scp过来
    ss = map_data_path.split(':')[0]
    if not os.path.exists(DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp'):
        # print('mkdir ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp')
        os.system('mkdir ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp')
    os.system('scp ' + map_data_path + ' ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp/')
    # 读取一个文件，合并并删除
    files = os.listdir(DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp/')
    for f in range(len(files)):
        ff_result = merge_dict(iou.read_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp/' + files[f]),
                               ff_result)
    # 删除临时文件
    os.system('rm ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp/*')
    # remove_mid_data(reduce_id)

    # 保存回reduce slave
    iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + reduce_id, ff_result)

    print(ss + ' reduce combine success')


def merge_dict(d1=None, d2=None):
    if d1 is None:
        return d2
    if d2 is None:
        return d1

    f_keys = set()
    for key in d1.iterkeys():
        f_keys.add(key)
    for key in d2.iterkeys():
        f_keys.add(key)

    final_result = {}
    for k in f_keys:
        final_result[k] = []

    for d in d2.iterkeys():
        final_result[d].extend(d2[d])
    for d in d1.iterkeys():
        final_result[d].extend(d1[d])
    return final_result


# TODO 增加ADMM参数"rou"
def do_reduce_dfs(job_func, file_path, dfs_path, slave, rou):
    """
    执行一个reduce，ssh
    :param job_func:
    :param file_path:
    :param slave: 节点主机ip
    :return:
    """
    # os.system('echo ' + 'ssh' + slave + 'python' + job_func + 'reduce' + file_path)
    os.system('ssh ' + slave + ' python ' + job_func + ' reduce ' + file_path + ' ' + dfs_path + ' ' + rou)


def remove_mid_data(reduce_id):
    settings = dfs.read_dfs_setting()
    for s in settings['nodes']:
        os.system('ssh ' + s + ' rm ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/' + reduce_id + '*')


# def remove_mid_data_client():


def finish(data, output_data_paths):
    iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/result', data)
    dfs.copyFromLocal_client(DEFAULT_INTERMEDIATE_DATA_PATH + '/result', output_data_paths)
    os.system('rm ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/result')
    os.system('rm ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/*')
    print('reducing success')


"""————————————————————————分割线，以下代码是ADMM框架新加的————————————————————————"""

# ADMM参数路径
DEFAULT_ADMM_PARA_PATH = '/home/dsjxtjc/2018211149/admm'
DEFAULT_MAX_ITER_NUM = 50


def get_map_admm_xiui(map_id):
    """
    从map任务所在的节点本地，读取admm参数
    :param map_id 参数文件名，和map_id同名
    :return:
    """
    # with open(DEFAULT_ADMM_PARA_PATH + '/' + str(map_id) + '.pickle', 'rb') as f:
    #     data = pickle.load(f)
    # return data
    return iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/' + str(map_id))
    # print(type(data))


def set_admm_para(para_name, data):
    """
    将当前map更新的参数写到本地文件中，以当前map_id命名的
    :param map_id 参数文件名，和map_id同名
    :return:
    """
    # with open(DEFAULT_ADMM_PARA_PATH + '/' + str(map_id) + '.pickle', 'wb') as f:
    #     pickle.dump(data, f, 0)
    # iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/' + str(map_id), data)

    # 初始化参数
    if os.path.exists(DEFAULT_ADMM_PARA_PATH + '/paras'):
        para = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/paras')
    else:
        para = {}
    para[para_name] = data
    iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/paras', data)


# TODO 此处额data就不需要combine了
def partition_admm(data, reduce_num, map_id):
    # print(len(data))
    data_files = {}
    for i in range(int(reduce_num)):
        data_files[i] = {}

    for k in data.iterkeys():
        # partition_result[k] = hash(k) % reduce_num
        data_files[hash(k) % int(reduce_num)][k] = data[k]

    # 保存partition的属于各个reduce的中间结果文件
    # 文件名就是reduce的编号，内容是：list((key,list(value)))
    for d in data_files.iterkeys():
        # print(len(data_files[d]))
        # output = open(DEFAULT_INTERMEDIATE_DATA_PATH + '/' + str(d), 'wb')
        # pickle.dump(str(data_files[d]), output)
        # output.close()
        iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/' + str(d) + '_' + str(map_id),
                       str(data_files[d]))
    # TODO 一个partition只是包含部分key，写到中间结果文件
    # iou.write_dict(DEFAULT_INTERMEDIATE_DATA_PATH + '/partition.txt', partition_result)
    print('partition success')


# ADMM 的reduce
def combine_reduce_admm(map_data_path, reduce_id):
    print('combine_reduce_admm begin')
    files = []
    files_tmp = os.listdir(DEFAULT_ADMM_PARA_PATH)
    for ft in files_tmp:
        if os.path.isfile(DEFAULT_ADMM_PARA_PATH + '/' + ft):
            if '_' in ft and ft[0:ft.rfind('_')] == str(reduce_id):
                files.append(ft)
    # 读取全部的文件
    ff_result = None
    for ff in files:
        ff_result = merge_dict(iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/' + ff), ff_result)
    # 创建临时文件夹，并把其他节点上的map临时文件scp过来
    ss = map_data_path.split(':')[0]
    if not os.path.exists(DEFAULT_ADMM_PARA_PATH + '/map_temp'):
        # print('mkdir ' + DEFAULT_INTERMEDIATE_DATA_PATH + '/map_temp')
        os.system('mkdir ' + DEFAULT_ADMM_PARA_PATH + '/map_temp')
    os.system('scp ' + map_data_path + ' ' + DEFAULT_ADMM_PARA_PATH + '/map_temp/')
    # 读取一个文件，合并并删除
    files = os.listdir(DEFAULT_ADMM_PARA_PATH + '/map_temp/')
    for f in range(len(files)):
        ff_result = merge_dict(iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/map_temp/' + files[f]),
                               ff_result)
    # 删除临时文件
    os.system('rm ' + DEFAULT_ADMM_PARA_PATH + '/map_temp/*')
    # remove_mid_data(reduce_id)

    # 保存回reduce slave
    iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/' + reduce_id, ff_result)

    print(ss + ' admm reduce combine success')


# TODO ADMM 每次reduce合并完之后
# TODO reduce计算当前的z'，发送到master
def finish_one_admm(xiui_path, z_current, output_data_paths):
    """
    admm一次迭代结束
    :param data:
    :param output_data_paths:
    :return:
    """
    settings = dfs.read_dfs_setting()
    # 当前x复制到master
    commands.getstatusoutput('scp ' + DEFAULT_ADMM_PARA_PATH + '/' + xiui_path + ' '
                             + settings['master'] + ':' + DEFAULT_ADMM_PARA_PATH + '/xiui')
    # 当前的z复制到master
    iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/z_one', z_current)
    commands.getstatusoutput('scp ' + DEFAULT_ADMM_PARA_PATH + '/z_one' + ' '
                             + settings['master'] + ':' + DEFAULT_ADMM_PARA_PATH + '/z_one')

    # master检查并控制是否继续
    commands.getstatusoutput('ssh ' + settings['master'] + ' python ' + DEFAULT_MAP_REDUCE_PATH
                             + ' wrapper ' + output_data_paths)

    # wrapper(output_data_paths)
    print('admm one reduce success')


def check_residual():
    # 当前的
    z_one = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/z_one')
    if not os.path.exists(DEFAULT_ADMM_PARA_PATH + '/z_last'):
        iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/z_last', z_one)
        os.remove(DEFAULT_ADMM_PARA_PATH + '/z_one')
        return False
    else:
        z_last = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/z_last')
        z_new = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/z_one')
        xiui = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/xiui')

        paras = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/paras')
        if paras.haskey('iter_num'):
            if paras['iter_num'] >= DEFAULT_MAX_ITER_NUM:
                state = True
                return state
            else:
                paras['iter_num'] += 1
        else:
            paras['iter_num'] = 1
        res_prim = 0.0
        for i in xiui.iterkeys():
            res_prim = res_prim + np.linalg.norm(xiui[i] - z_new, ord=np.inf) ** 2
        if paras['p'] * math.sqrt(paras['N']) * np.linalg.norm(
                z_new - z_last, ord=np.inf) <= paras['epilo_dual'] \
                and res_prim <= paras['epilo_prim']:
            state = True
        else:
            state = False
            # os.system()
        iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/z_last', z_new)
        iou.write_dict(DEFAULT_ADMM_PARA_PATH + '/paras', paras)
        os.remove(DEFAULT_ADMM_PARA_PATH + '/z_one')
        return state


# TODO 检查残差是否达到足够小的阈值，如果是将xi整合写进DFS，否继续启动新的Map和reduce
def wrapper(data_output_path):
    if check_residual():
        os.system(' cp ' + DEFAULT_ADMM_PARA_PATH + '/z_one' + ' ' + DEFAULT_ADMM_PARA_PATH + '/z_final')
        dfs.copyFromLocal_client(DEFAULT_INTERMEDIATE_DATA_PATH + '/z_final', data_output_path)
        return 'ADMM finish'
    else:
        last_job_record = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/job_record')

        do_job(last_job_record['job_path'],
               last_job_record['file_path'],
               last_job_record['dfs_path'],
               last_job_record['rou'],
               last_job_record['ftype'],
               last_job_record['reduce_num'])


# z_last = iou.read_dict(DEFAULT_ADMM_PARA_PATH + '/last_z')
# if check_residual():


if __name__ == '__main__':
    if len(sys.argv) != 0:
        if sys.argv[1] == 'init':
            print(init())
        elif sys.argv[1] == 'do_job':
            # TODO ADMM，初始提交job没有上次的map记录地址
            print(do_job(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]))
        elif sys.argv[1] == 'combine_reduce':
            combine_reduce(sys.argv[2], sys.argv[3])
        elif sys.argv[1] == 'submit':
            print(submit_client(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]))
        elif sys.argv[1] == 'wrapper':
            print(wrapper(sys.argv[2]))
        # elif sys.argv[1] == 'remove_mid_data':
        #     remove_mid_data(sys.argv[2])
        # elif sys.argv[1] == 'reduce':
        #     print(reduce(sys.argv[2]))
