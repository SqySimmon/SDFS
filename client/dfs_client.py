# coding=utf-8
"""
TODO 客户端封装的代码，封装ssh执行
客户端远程执行的代码，远程与namenode交互
"""
import os
import socket
import time
import sys
# import json
import commands

sys.path.append('/home/dsjxtjc/2018211149/')

import utils.io_utils as iou
import dfs_sqy.dfs as dfs

DEFAULT_DFS_PATH = '/home/dsjxtjc/2018211149/dfs_sqy/dfs.py'


# list目录——客户端代码
def ls(path):
    settings = dfs.read_dfs_setting()
    return os.system('ssh ' + settings['master'] + ' python ' + DEFAULT_DFS_PATH + ' ls ' + path)


# file文件——客户端代码
def gfile(path):
    settings = dfs.read_dfs_setting()
    return os.system('ssh ' + settings['master'] + ' python ' + DEFAULT_DFS_PATH + ' gfile ' + path)


# mkdir——客户端代码
def mkdir(path):
    settings = dfs.read_dfs_setting()
    return os.system('ssh ' + settings['master'] + ' python ' + DEFAULT_DFS_PATH + ' mkdir ' + path)


# touch——客户端代码
def touch(path):
    settings = dfs.read_dfs_setting()
    return os.system('ssh ' + settings['master'] + ' python ' + DEFAULT_DFS_PATH + ' touch ' + path)


def copyFromLocal(local_file_path, dfs_file_path):
    return dfs.copyFromLocal_client(local_file_path, dfs_file_path)


def copyToLocal(dfs_file_path, local_file_path):
    return dfs.copyToLocal_client(dfs_file_path, local_file_path)


def rm(path):
    settings = dfs.read_dfs_setting()
    return os.system('ssh ' + settings['master'] + ' python /home/dsjxtjc/2018211149/dfs_sqy/dfs.py delete '
                     + path)


if __name__ == '__main__':
    # print socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    # init_dfs()
    # print(sys.argv)
    if sys.argv[1] == 'ls':
        print(ls(sys.argv[2]))
    elif sys.argv[1] == 'gfile':
        print(gfile(sys.argv[2]))
    elif sys.argv[1] == 'mkdir':
        print(mkdir(sys.argv[2]))
    elif sys.argv[1] == 'touch':
        print(touch(sys.argv[2]))
    elif sys.argv[1] == 'copyFromLocal':
        print(copyFromLocal(sys.argv[2], sys.argv[3]))
    elif sys.argv[1] == 'copyToLocal':
        print(copyToLocal(sys.argv[2], sys.argv[3]))
    elif sys.argv[1] == 'rm':
        print(rm(sys.argv[2]))
