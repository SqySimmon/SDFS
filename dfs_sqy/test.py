import sys

sys.path.append('/home/dsjxtjc/2018211149/')

import file_system as fs


def set_default():
    pf_chunks = {}
    chunk_paths = {}

    f = open('../dfs_settings.txt', "w")
    settings = {}
    settings['root'] = '.'
    # settings['roots_files'] = pf_chunks
    # settings['chunks'] = chunk_paths
    f.write(str(settings))
    f.close()


# def set_default2():

# set_default()

# print(0x11)
# print(0x11 + 1)
import os
import commands
import time
# os.system('ping -c1 -w1 szcluster.mmlab.top')
# print(commands.getstatusoutput('ping -c1 -w1 szcluster.mmlab.top'))
# print(commands.getstatusoutput('ssh thumm01 test -e ~/dfs/df'))
print(eval('[1,1]'))
print(eval('[1,1]')[0])
# print(str(int(time.time())))

# print(time.time())