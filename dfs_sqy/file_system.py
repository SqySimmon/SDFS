# coding=utf-8
#  文件类型
FS_TYPE_DIRECTORY = 'DIRECTORY'
FS_TYPE_FILE = 'FILE'

# DFS 运行就读取到内存的文件系统
FILE_SYSTEM = None


# 树状文件系统文件
class TreeFile:
    def __init__(self):
        # 类型标识：文件
        self.kind = FS_TYPE_FILE
        # 名称
        self.name = None
        # 大小
        self.size = 0
        # 文件对应的chunks索引
        self.chunk_list_index = []


# 树状文件系统目录
class TreeDirectory(TreeFile):
    def __init__(self):
        TreeFile.__init__(self)
        # 子目录或者文件
        self.child_tds = {}
        # 类型标识：目录
        self.kind = FS_TYPE_DIRECTORY
        # 文件对应的chunks索引
        self.chunk_list_index = None


if __name__ == '__main__':
    # dict 可以存不同类型的value！！！
    # a = {}
    # a['123'] = 2
    # a['456'] = {}
    #
    # print a
    pass
