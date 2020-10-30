# coding=utf-8


def read_dict(path):
    f = open(path)
    a = f.read()
    data = eval(a)
    f.close()
    return data


def write_dict(path, dict_data):
    f = open(path, "w")
    f.write(str(dict_data))
    f.close()
