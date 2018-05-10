# -*- coding: utf-8 -*-
__author__ = 'Allen'

from dbdb.binary_tree import BinaryTree
from dbdb.physical import Storage

#DBDB有两个成员变量：_storage与_tree，_storage封装了数据库文件和对数据库文件的基本操作，
# _tree是二叉树数据结构对象，DBDB接口的实现主要是将二叉树的操作封装为Python词典的键值操作。

class DBDB(object):

    def __init__(self,f):
        self._storage = Storage(f)#_storage在DBDB中只完成一个功能：检查文件有没有关闭
        self._tree = BinaryTree(self._storage)

    def _assert_not_close(self):
        if self._storage.closed:
            raise ValueError('Database closed.')

    def close(self):
        self._storage.close()

    def commit(self):
        self._assert_not_close()
        self._tree.commit()

    #实现__getitem__、__setitem__、__delitem__、__contains__等函数，
    # 就能像操作词典一样操作DBDB对象了
    def __getitem__(self, key):
        '''通过 dbdb[key] 获取键值'''
        self._assert_not_close()
        return self._tree.get(key)

    def __setitem__(self, key, value):
        '''通过 dbdb[key] = value 设置键值'''
        self._assert_not_close()
        return self._tree.set(key,value)

    def __delitem__(self, key):
        '''通过 del dbdb[key] 删除键值'''
        self._assert_not_close()
        return self._tree.pop(key)

    def __contains__(self, key):
        '''通过 key in dbdb 来判断键在不在数据库中'''
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __len__(self):
        return len(self._tree)