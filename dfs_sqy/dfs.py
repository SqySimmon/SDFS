# coding=utf-8


import os
import socket
import time
import sys
# import json
import commands

# import subprocess

sys.path.append('/home/dsjxtjc/2018211149/')

import utils.io_utils as iou
import math
from multiprocessing import Process

# 默认的配置文件路径
DEFAULT_DFS_PATH = '/home/dsjxtjc/2018211149/dfs_sqy/dfs.py'

DEFAULT_SETTINGS_PATH = '/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/dfs_settings.txt'
DEFAULT_DFS_CHUNK_PATH = '/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/dfs_file_chunks.txt'
DEFAULT_CHUNK_LOCAL_PATH = '/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/chunk_local_file.txt'
HASH_PARTITION = '/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/hash_partition.txt'
DFS_DATA_PATH = '/home/dsjxtjc/2018211149/dfs_data'

FS_TYPE_DIRECTORY = 'directory'
FS_TYPE_FILE = 'file'


def init_dfs():
    """
    初始化DFS，设置默认为文件系统
    固定配置文件为：dfs_sqy/sqy_dfs/dfs_settings.txt
    :return:
    """
    if not os.path.exists('/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/'):
        os.makedirs('/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/')

    if not os.path.exists(DFS_DATA_PATH):
        os.makedirs(DFS_DATA_PATH)

    f = open(DEFAULT_SETTINGS_PATH, "w")
    settings = {}
    # 默认根目录是
    settings['root'] = '/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/'
    # 默认chunk size 64MB
    settings['chunk_size'] = 64
    # chunk对应的真实文件地址起始
    settings['chunk_root'] = DFS_DATA_PATH
    # 默认replication 3
    settings['replication'] = 3
    # 默认master ip 当前主机的ip
    settings['master'] = socket.gethostname()
    # 默认nodes 添加一个自己
    settings['nodes'] = [socket.gethostbyname(socket.getfqdn(socket.gethostname()))]
    # 文件系统
    settings['attributes'] = ('path', 'size(kb)', 'type', 'time')
    # 文件系统目录系统，目录带有子目录
    # 如果是目录，最后一个对应的是子目录，如果是文件，最后一个对应的是chunk索引
    settings['dfs_file_system'] = {'name': '/',
                                   'size': 0,
                                   'type': FS_TYPE_DIRECTORY,
                                   'date': time.asctime(time.localtime(time.time())),
                                   'rfs': {}}
    f.write(str(settings))
    f.close()

    # 创建文件——chunk对应表
    f = open(DEFAULT_DFS_CHUNK_PATH, "w")
    chunks = {}
    f.write(str(chunks))
    f.close()

    # 创建chunk node目录对应表
    f = open(DEFAULT_CHUNK_LOCAL_PATH, "w")
    f.write(str({}))
    f.close()

    # 创建hash 轮询顺序表
    f = open(HASH_PARTITION, "w")
    f.write(str([socket.gethostbyname(socket.getfqdn(socket.gethostname()))]))
    f.close()

    # 创建数据文件夹
    if not os.path.exists(DFS_DATA_PATH):
        os.makedirs(DFS_DATA_PATH)


def set_chunk(chunk_size):
    if int(chunk_size) <= 0:
        return 'please input positive number'
    settings = read_dfs_setting()
    settings['chunk_size'] = int(chunk_size)
    iou.write_dict(DEFAULT_DFS_PATH, settings)
    return 'success'


def set_replication(chunk_size):
    if int(chunk_size) <= 0:
        return 'please input positive number'
    settings = read_dfs_setting()
    settings['replication'] = int(chunk_size)
    iou.write_dict(DEFAULT_DFS_PATH, settings)
    return 'success'


def read_dfs_setting():
    return iou.read_dict(DEFAULT_SETTINGS_PATH)


def read_dfs_dfs_chunk():
    return iou.read_dict(DEFAULT_DFS_CHUNK_PATH)


def read_dfs_chunk_local():
    return iou.read_dict(DEFAULT_CHUNK_LOCAL_PATH)


def read_dfs_hash_partition():
    return iou.read_dict(HASH_PARTITION)


def set_dfs_master_ip(master_ip):
    settings = read_dfs_setting()
    settings['master'] = master_ip
    iou.write_dict(DEFAULT_SETTINGS_PATH, settings)
    return 'set master success'


def add_dfs_node_ip(node_ip):
    settings = read_dfs_setting()
    if node_ip in settings['nodes']:
        return 'node exist'
    settings['nodes'].append(node_ip)
    iou.write_dict(DEFAULT_SETTINGS_PATH, settings)
    # 添加到hash
    hash_partition = iou.read_dict(HASH_PARTITION)
    hash_partition.append(node_ip)
    iou.write_dict(HASH_PARTITION, hash_partition)
    # 创建数据文件目录
    # print()
    commands.getstatusoutput('ssh ' + node_ip + ' mkdir ' + DFS_DATA_PATH)
    return 'add node success'


def set_dfs_root(path='/home/dsjxtjc/2018211149/dfs_sqy/sqy_dfs/metadata/'):
    settings = read_dfs_setting()
    settings['root'] = path
    iou.write_dict(DEFAULT_SETTINGS_PATH, settings)


def init_dfs_file_system():
    settings = read_dfs_setting()


# list目录
def ls(path):
    settings = read_dfs_setting()
    fs = settings['dfs_file_system']
    if path is None or path[0:1] != '/':
        return 'please input right path'
    else:
        if len(path) == 1:
            fs = settings['dfs_file_system']['rfs']
            result = ''
            for i in fs.keys():
                result = result + fs[i]['name'] + '\t' + fs[i]['type'] \
                         + '\t' + str(fs[i]['size']) + '\t' + fs[i]['date'] + '\n'
            return result
        else:
            return make_path_attributes_str(fs, path)


# file文件信息
def gfile(path):
    settings = read_dfs_setting()
    fs = settings['dfs_file_system']
    if path is None or path[0:1] != '/':
        return 'please input right file path'
    else:
        if len(path) == 1:
            return 'please input right file path'
        else:
            return make_file_attributes_str(fs, path)


# 查找并组成目录下的所有文件+目录
def make_path_attributes_str(fs, path):
    # TODO 显示目录下的所有文件以及目录的属性
    paths = path.split('/')
    paths.pop(0)
    for p in paths:
        if fs['type'] != FS_TYPE_DIRECTORY:
            return 'please input directory'
        if p not in fs['rfs'].keys():
            return p + ' does not exist'
        else:
            fs = fs['rfs'][p]
    if fs['type'] != FS_TYPE_DIRECTORY:
        return 'please input directory'
    else:
        result = ''
        for i in fs['rfs'].keys():
            result = result + fs['rfs'][i]['name'] + '\t' + fs['rfs'][i]['type'] \
                     + '\t' + str(fs['rfs'][i]['size']) + '\t' + fs['rfs'][i]['date'] + '\n'
        return result


# 查找并返回文件属性
def make_file_attributes_str(fs, path):
    paths = path.split('/')
    paths.pop(0)
    i = 0
    for i in range(len(paths)):

        if paths[i] not in fs['rfs'].keys():
            return paths[i] + ' does not exist'
        else:
            fs = fs['rfs'][paths[i]]
    if fs['type'] != FS_TYPE_FILE:
        return 'please input file path'
    if paths[i] != fs['name']:
        return 'please input right file name'
    else:
        return fs['name'] + '\t' + fs['type'] + '\t' + str(fs['size']) + '\t' + fs['date']


# 查找并创建文件夹
def mkdir(path):
    settings = read_dfs_setting()
    fs = settings['dfs_file_system']
    if path is None or path[0:1] != '/':
        return 'please input right path'
    else:
        if len(path) == 1:
            return 'dir exist'
        else:
            paths = path.split('/')
            paths.pop(0)
            for i in range(len(paths)):
                if i != len(paths) - 1:
                    if paths[i] not in fs['rfs'].keys():
                        return paths[i] + ' does not exist'
                    else:
                        fs = fs['rfs'][paths[i]]
                else:
                    if paths[i] in fs['rfs'].keys():
                        return 'dir exist'
                    else:
                        fs['rfs'][paths[i]] = {'name': paths[i],
                                               'size': 0,
                                               'type': FS_TYPE_DIRECTORY,
                                               'date': time.asctime(time.localtime(time.time())),
                                               'rfs': {}}
                        iou.write_dict(DEFAULT_SETTINGS_PATH, settings)
                        return 'mkdir success'


# 创建文件
def touch(path, fsize=0):
    files = iou.read_dict(DEFAULT_DFS_CHUNK_PATH)
    if path in files.keys():
        return 'file exist'
    settings = read_dfs_setting()
    fs = settings['dfs_file_system']
    if path is None or path[0:1] != '/':
        return 'please input right path'
    else:
        if len(path) == 1:
            return 'please input right file path'
        else:
            paths = path.split('/')
            paths.pop(0)
            for i in range(len(paths)):
                if i != len(paths) - 1:
                    if paths[i] not in fs['rfs'].keys():
                        return paths[i] + ' does not exist'
                    else:
                        fs = fs['rfs'][paths[i]]
                else:
                    if paths[i] in fs['rfs'].keys():
                        return 'file exist'
                    else:
                        fs['rfs'][paths[i]] = {'name': paths[i],
                                               'size': fsize,
                                               'type': FS_TYPE_FILE,
                                               'date': time.asctime(time.localtime(time.time()))}
                        # 创建本地文件，再copy
                        # os.mknod(paths[i])
                        # os.system('')
                        # partition(path, 0, paths[i])
                        # os.remove(paths[i])
                        iou.write_dict(DEFAULT_SETTINGS_PATH, settings)
                        # 写到文件chunk对应表
                        files[path] = []
                        iou.write_dict(DEFAULT_DFS_CHUNK_PATH, files)
                        return 'make file success'


def partition(dfs_file_path, fsize):
    """
    namenode代码，根据文件大小，返回划分的chunk name, 对应的在datanode的地址，
    """
    fsize = int(fsize)
    settings = read_dfs_setting()
    file_chunks = iou.read_dict(DEFAULT_DFS_CHUNK_PATH)
    chunk_locals = iou.read_dict(DEFAULT_CHUNK_LOCAL_PATH)
    # 如果原来有，提示错误
    if dfs_file_path in file_chunks.keys():
        # if delete(dfs_file_path) != 'success':
        return 0
        # return 'delete old'

    touch(dfs_file_path, fsize)

    # chunk 分编号
    file_chunks[dfs_file_path] = []
    for i in range(int(math.ceil(float(fsize) / settings['chunk_size']))):
        # 用时间做hash生成不会重复的chunk编号
        # chunk 编号 list 指定了chunk顺序
        file_chunks[dfs_file_path].append(hash(time.time() + i))

    iou.write_dict(DEFAULT_DFS_CHUNK_PATH, file_chunks)

    hash_partition = iou.read_dict(HASH_PARTITION)
    for c in file_chunks[dfs_file_path]:
        # chunk 文件名就是编号
        chunk_locals[c] = []
        # 根据replication 添加冗余的chunk地址
        for i in range(settings['replication']):
            chunk_locals[c].append(dfs_hash(hash_partition, c))

    iou.write_dict(DEFAULT_CHUNK_LOCAL_PATH, chunk_locals)
    iou.write_dict(HASH_PARTITION, hash_partition)
    # 以字符串的形式返回
    return str({'file_chunks': file_chunks, 'chunk_locals': chunk_locals})


# hash 节点服务器地址，轮询
def dfs_hash(hash_partition, chunk):
    # settings = read_dfs_setting()
    # hash_partition = iou.read_dict(HASH_PARTITION)
    # nodes = []
    for i in hash_partition:
        if commands.getstatusoutput('ping -c1 -w1 ' + i)[0] == 0:
            # nodes.append(i + ':/home/dsjxtjc/2018211149/')
            hash_partition.insert(0, hash_partition[len(hash_partition) - 1])
            hash_partition.pop(len(hash_partition) - 1)
            return str(i) + ':/home/dsjxtjc/2018211149/dfs_data/' + str(chunk)


def copyFromLocal_client(local_file_path, dfs_file_path):
    # 客户端代码，将本地文件拷贝到DFS
    # 从本地的配置文件读取到master的ip
    settings = read_dfs_setting()
    # 获取本地文件的大小属性
    fsize = os.path.getsize(local_file_path)
    fsize = int(math.ceil(fsize / float(1024 * 1024)))
    # 调用namenode段代码获取chunk分配
    part_result = commands.getstatusoutput('ssh ' + settings['master'] +
                                           ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py partition '
                                           + dfs_file_path + ' ' + str(fsize))[1]
    if part_result == '0' or part_result == 0:
        return 'file exist'
    part_result = eval(part_result)
    commands.getstatusoutput('mkdir ' + os.path.dirname(local_file_path) + '/tmp')
    commands.getstatusoutput('split -b ' + str(settings['chunk_size'] * 1024 * 1024) + ' '
                             + local_file_path + ' ' + os.path.dirname(local_file_path)
                             + '/tmp/')
    files = os.listdir(os.path.dirname(local_file_path) + '/tmp/')
    files.sort()
    result = 0
    for i in range(len(files)):
        for l in part_result['chunk_locals'][part_result['file_chunks'][dfs_file_path][i]]:
            rr = commands.getstatusoutput('scp '
                                          + os.path.dirname(local_file_path)
                                          + '/tmp/'
                                          + str(files[i])
                                          + ' ' + l)[0]
            result = result + rr
    commands.getstatusoutput('rm -rf ' + os.path.dirname(local_file_path) + '/tmp/')
    commands.getstatusoutput('ssh ' + settings['master'] +
                             ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py copyFromLocal_finish '
                             + dfs_file_path + ' ' + str(result))

    if result == 0:
        # return gfile(dfs_file_path)
        return os.system('ssh ' + settings['master'] +
                         ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py gfile ' + dfs_file_path)
    else:
        return 'defeat'


def delete(dfs_file_path):
    """
    namebode代码，删除文件
    :param dfs_file_path:
    :return:
    """
    settings = read_dfs_setting()
    paths = dfs_file_path.split('/')
    paths.pop(0)
    i = 0
    fs = settings['dfs_file_system']
    for i in range(len(paths)):
        if i != len(paths) - 1:
            if paths[i] not in fs['rfs'].keys():
                return (0, paths[i] + ' does not exist')
            else:
                fs = fs['rfs'][paths[i]]
    if paths[i] not in fs['rfs'].keys():
        return (0, 'file does not exist')
    else:
        # 删除chunk
        file_chunks = iou.read_dict(DEFAULT_DFS_CHUNK_PATH)
        if dfs_file_path in file_chunks.keys():
            chunk_locals = iou.read_dict(DEFAULT_CHUNK_LOCAL_PATH)
            for k in file_chunks[dfs_file_path]:
                for j in chunk_locals[k]:
                    local_path = j.split(':')
                    commands.getstatusoutput('ssh ' + str(local_path[0]) + ' rm ' + local_path[1])
                chunk_locals.pop(k)
            file_chunks.pop(dfs_file_path)
            iou.write_dict(DEFAULT_DFS_CHUNK_PATH, file_chunks)
            iou.write_dict(DEFAULT_CHUNK_LOCAL_PATH, chunk_locals)
        # 删除目录树
        fs['rfs'].pop(paths[i])
        iou.write_dict(DEFAULT_SETTINGS_PATH, settings)
        return 'success'


def delete_client(dfs_file_path):
    """
    delete 客户端代码
    """
    settings = read_dfs_setting()
    return commands.getstatusoutput('ssh ' + settings['master'] +
                                    ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py delete '
                                    + dfs_file_path)[1]


def copyFromLocal_finish(dfs_file_path, state):
    """
    namenode代码，用于向namenode确认上传成功
    """
    # 上传不成功就删除跟这个文件有关的信息
    if int(state) != 0:
        delete(dfs_file_path)


def chunk_reorganization(dfs_file_path):
    """
    namenode代码，chunk重组，返回原始顺序的chunk地址list
    """
    # 触发文件丢失检查
    p = Process(target=check_chunk, args=(dfs_file_path,))
    p.start()
    file_chunks = iou.read_dict(DEFAULT_DFS_CHUNK_PATH)
    chunk_locals = iou.read_dict(DEFAULT_CHUNK_LOCAL_PATH)
    # 保存的chunk在各个节点上的位置，可能是在不同的机器上的
    chunk_local_result = []
    # 顺序遍历dfs_chunk表中的chunk，是按文件原始顺序的list
    for i in file_chunks[dfs_file_path]:
        # 标识当前数据块是否完整，默认直到节点replication的节点都没有宕机，文件存在
        state = False
        # print(chunk_locals[i])
        for j in chunk_locals[i]:
            local_path = j.split(':')
            # print(local_path[0])
            # 找到没宕机的节点上的chunk地址
            # print(commands.getstatusoutput('ping -c1 -w1 ' + str(local_path[0]))[0] == 0)
            if int(commands.getstatusoutput('ping -c1 -w1 ' + local_path[0])[0]) == 0:
                # 检查文件是否存在
                if int(commands.getstatusoutput('ssh '
                                                + local_path[0] + ' test -e '
                                                + local_path[1])[0]) == 0:
                    state = True
                    chunk_local_result.append(j)
                    break
        # 如果某一chunk在所有的datanode上都找不到，返回错误标识
        if not state:
            # print(chunk_local_result)
            return 0
    # 返回按照顺序的chunk datanode地址，key就是文件名
    return {os.path.basename(dfs_file_path): chunk_local_result}


def copyToLocal_client(dfs_file_path, local_file_path):
    # TODO 从DFS到本地，涉及到数据重组
    """client代码"""
    # 从本地的配置文件读取到master的ip
    settings = read_dfs_setting()
    # print(dfs_file_path)
    # print('ssh ' + settings['master'] +
    #       ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs_sqy.py chunk_reorganization '
    #       + dfs_file_path)
    chunk_result = commands.getstatusoutput('ssh ' + settings['master'] +
                                            ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py chunk_reorganization '
                                            + dfs_file_path)[1]
    # print(chunk_result)
    if chunk_result == 0:
        return 'file lost'
    else:
        state = []
        chunk_result = eval(chunk_result)
        chunks = chunk_result[os.path.basename(dfs_file_path)]
        # 先创建文件
        state.append(commands.getstatusoutput('> ' + local_file_path)[0])
        for c in chunks:
            # commands.getstatusoutput('mkdir ' + local_file_path + '/tmp')
            state.append(commands.getstatusoutput('scp ' + c + ' ' + os.path.dirname(local_file_path))[0])
            # 追加到末尾
            # print(os.path.dirname(local_file_path) + '/' + os.path.basename(c))
            # print(local_file_path)
            state.append(commands.getstatusoutput('cat '
                                                  + os.path.dirname(local_file_path)
                                                  + '/' + os.path.basename(c)
                                                  + ' >> ' + local_file_path)[0])
            # 删除chunk块
            state.append(commands.getstatusoutput('rm ' +
                                                  os.path.dirname(local_file_path) + '/'
                                                  + os.path.basename(c))[0])
        if sum(state) == 0:
            return 'success'


def get_dfs_chunk_list():
    settings = read_dfs_setting()
    return settings['chunks']


def has_path(path):
    roots_files = read_dfs_setting()
    return roots_files
    # return os.path.isdir(path)


def has_file(path):
    return os.path.isfile(path)


# namenode代码
def check_chunk(file_path):
    """
    检查chunk对应的datanode地址的完整性
    :return:
    """
    all_dfs_chunks = read_dfs_dfs_chunk()
    dfs_chunks = all_dfs_chunks[file_path]
    chunk_locals = read_dfs_chunk_local()
    # 全部的丢失的列表
    tran_chunks = []
    for c in dfs_chunks:
        # 第一个元素代表当前chunk未丢失的一个datanode地址，其后的为丢失的datanode地址
        atran_chunk = ['']
        # lost_chunk_paths = []
        if chunk_locals.has_key(c):
            # 只取一个有效的chunk地址
            for chunk in chunk_locals[c]:
                ccc = chunk.split(':')
                if commands.getstatusoutput('ping -c1 -w1 ' + ccc[0])[0] == 0:
                    if str(commands.getstatusoutput('ssh ' + ccc[0] +
                                                    ' "[ ! -e +' + ccc[1] + ']" echo 1')[1]) == 1:
                        atran_chunk.append(chunk)
                    else:
                        atran_chunk[0] = chunk
        if len(atran_chunk) != 1 and (atran_chunk[0] != '' or atran_chunk[0] is not None):
            tran_chunks.append(atran_chunk)

    if len(tran_chunks) == 0:
        return 'no chunk loss'
    else:
        for tc in tran_chunks:
            for atc in range(1, len(tc)):
                commands.getstatusoutput('scp ' + tc[0] + ' ' + tc[atc])
        return 'chunk loss restore finish'


def check_chunk_client(master, file_path):
    def remote_check(dc):
        os.system('ssh ' + master + ' python ' + DEFAULT_DFS_PATH + ' check_chunk ' + dc)

    p = Process(target=remote_check, args=(file_path,))
    # print('Child process will start.')
    p.start()
    # return


if __name__ == '__main__':

    if len(sys.argv) != 0:
        # client代码
        if sys.argv[1] == 'ls':
            print(ls(sys.argv[2]))
        elif sys.argv[1] == 'gfile':
            print(gfile(sys.argv[2]))
        elif sys.argv[1] == 'mkdir':
            print(mkdir(sys.argv[2]))
        elif sys.argv[1] == 'touch':
            print(touch(sys.argv[2]))
        elif sys.argv[1] == 'copyFromLocal':
            print(copyFromLocal_client(sys.argv[2], sys.argv[3]))
        elif sys.argv[1] == 'rm':
            print(delete_client(sys.argv[2]))
        elif sys.argv[1] == 'copyToLocal':
            print(copyToLocal_client(sys.argv[2], sys.argv[3]))

        # namenode代码
        elif sys.argv[1] == 'init':
            init_dfs()
        elif sys.argv[1] == 'set_chunk_size':
            set_chunk(sys.argv[2])
        elif sys.argv[1] == 'set_replication':
            set_replication(sys.argv[2])
        elif sys.argv[1] == 'add_dfs_node_ip':
            print(add_dfs_node_ip(sys.argv[2]))
        elif sys.argv[1] == 'copyFromLocal_finish':
            print(copyFromLocal_finish(sys.argv[2], sys.argv[3]))
        elif sys.argv[1] == 'partition':
            print(partition(sys.argv[2], sys.argv[3]))
        elif sys.argv[1] == 'delete':
            print(delete(sys.argv[2]))
        elif sys.argv[1] == 'chunk_reorganization':
            print(chunk_reorganization(sys.argv[2]))
        elif sys.argv[1] == 'set_master':
            print(set_dfs_master_ip(sys.argv[2]))
        elif sys.argv[1] == 'check_chunk':
            print(check_chunk(sys.argv[2]))
    pass
