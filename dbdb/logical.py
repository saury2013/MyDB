# -*- coding: utf-8 -*-
__author__ = 'Allen'

#ValueRef 是指向数据库中二进制数据对象的Python对象，是对数据库中数据的引用。

class ValueRef(object):

    def __init__(self,referent=None,address=0):
        self._referent = referent#就是它引用的值
        self._address = address#就是该值在文件中的位置

    def prepare_to_store(self,storage):
        '''存储之前要做的事情，此处不实现'''
        pass

    @staticmethod
    def referent_to_string(referent):
        #值的处理很简单，只要将utf-8格式的字节串解码就可以了
        return referent.encode('utf-8')

    @staticmethod
    def string_to_referent(string):
        return string.decode('utf-8')

    @property
    def address(self):
        return self._address

    def get(self,storage):
        '''获取引用'''
        if self._referent is None and self._address:
            #根据地址到文件取读取该引用的数据
            self._referent = self.string_to_referent(storage.read(self._address))
        return self._referent

    def store(self,storage):
        '''存储引用'''
        # 引用对象不为空而地址为空说明该引用对象还未被存储过
        if self._referent is not None and not self._address:
            self.prepare_to_store(storage)
            #把引用数据写入到文件，并得到写入的地址
            self._address = storage.write(self.referent_to_string(self._referent))

#LogicalBase 类提供了逻辑更新（比如 get，set 以及 commit）的抽象接口，
# 它同时负责管理存储对象的锁以及对内部节点的解引用。

class LogicalBase(object):
    node_ref_class = None
    value_ref_class = ValueRef

    def __init__(self,storage):
        self._storage = storage
        self._refresh_tree_ref()

    def commit(self):
        self._tree_ref.store(self._storage)
        self._storage.commit_root_address(self._tree_ref.address)

    def _refresh_tree_ref(self):
        '''不可变二叉树每次添加删除结点都会生成一个新的二叉树
        新的二叉树的根节点地址会重写到superblock文件开头
        这个方法就是更新根节点地址'''
        self._tree_ref = self.node_ref_class(
            address = self._storage.get_root_address()
        )

    def _follow(self,ref):
        '''获取Ref所引用的具体对象'''
        return ref.get(self._storage)#这里的get是ValueRef的get方法

    def get(self,key):
        '''获取键值'''
        # 如果数据库文件没有上锁，则更新对树的引用
        if not self._storage.locked:
            self._refresh_tree_ref()
            # _get 方法将在子类中实现
        return self._get(self._follow(self._tree_ref),key)

    def set(self,key,value):
        '''设置键值'''
        # 如果数据库文件没有上锁，则更新对树的引用
        #这里不用self._storage.locked判断是不管是否锁定，这里都要加锁
        if self._storage.lock():
            self._refresh_tree_ref()
            # _insert 方法将在子类中实现
        self._tree_ref = self._insert(
            self._follow(self._tree_ref),
            key,
            self.value_ref_class(value)
        )

    def pop(self,key):
        '''删除键值'''
        if self._storage.lock():
            self._refresh_tree_ref()
            # _delete 方法将在子类中实现
        self._tree_ref = self._delete(
            self._follow(self._tree_ref),key
        )

    def __len__(self):
        if not self._storage.locked:
            self._refresh_tree_ref()
        root = self._follow(self._tree_ref)
        if root:
            return root.length
        else:
            return 0
