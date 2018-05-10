# -*- coding: utf-8 -*-
__author__ = 'Allen'

import pickle
from dbdb.logical import LogicalBase,ValueRef

#实现二叉树中的节点
#BinaryNode由对左右节点的引用，键，对值的引用，以及长度组成，
# 这里的长度是指该节点及其子节点组成的子树的节点数
class BinaryNode(object):
    @classmethod
    def from_node(cls,node,**kwargs):
        '''工厂方法，该函数会根据读入的节点与更新节点的参数生成一个新节点并返回（记住数据结构不可变）'''
        length = node.length
        if 'left_ref' in kwargs:
            length += kwargs['left_ref'].length - node.left_ref.length
        if 'right_ref' in kwargs:
            length += kwargs['right_ref'].length - node.right_ref.length

        return cls(
            left_ref=kwargs.get('left_ref',node.left_ref),
            key=kwargs.get('key',node.key),
            value_ref=kwargs.get('value_ref',node.value_ref),
            right_ref=kwargs.get('right_ref',node.right_ref),
            length=length,
        )

    def __init__(self, left_ref, key, value_ref, right_ref, length):
        self.left_ref = left_ref
        self.key = key
        self.value_ref = value_ref
        self.right_ref = right_ref
        self.length = length

    def store_refs(self,storage):
        #在存储BinaryNodeRef的时候会触发先序遍历，直到访问ValueRef（相当于叶子节点）时递归才会停止。
        self.value_ref.store(storage)
        self.left_ref.store(storage)
        self.right_ref.store(storage)

#BinaryNodeRef 是 ValueRef 的子类，实现对二叉树节点的引用
#包装节点，解析节点内容
class BinaryNodeRef(ValueRef):
    def prepare_to_store(self,storage):
        '''在存储引用的对象前的勾子函数，在处理值的时候我们并不需要做预处理，但是在处理节点的时候这一步就有必要了。'''
        if self._referent:
            self._referent.store_refs(storage)

    @property
    def length(self):
        if self._referent is None and self._address:
            raise RuntimeError('Asking for BinaryNodeRef length of unloaded node')
        if self._referent:
            return self._referent.length
        else:
            return 0

    @staticmethod
    def referent_to_string(referent):
        '''格式化描述这个结点，存储在文件中的结点就是这段描述'''
        return pickle.dumps({
            'left':referent.left_ref.address,
            'key':referent.key,
            'value':referent.value_ref.address,
            'right':referent.right_ref.address,
            'length':referent.length,
        })

    @staticmethod
    def string_to_referent(string):
        '''根据结点描述，实例化出结点对象'''
        d = pickle.loads(string)
        return BinaryNode(
            BinaryNodeRef(address=d['left']),#左子树实例化成结点引用对象
            d['key'],
            ValueRef(address=d['value']),#value则实例化成值引用对象
            BinaryNodeRef(address=d['right']),#右子树实例化成结点引用对象
            d['length'],
        )

class BinaryTree(LogicalBase):
    node_ref_class = BinaryNodeRef

    def _get(self,node,key):

        while node is not None:
            if key < node.key:
                node = self._follow(node.left_ref)
            elif node.key < key:
                node = self._follow(node.right_ref)
            else:
                return self._follow(node.value_ref)
        raise KeyError

    def _insert(self,node,key,value_ref):

        if node is None:
            # 创建一个新节点
            new_node = BinaryNode(
                self.node_ref_class(),
                key,
                value_ref,
                self.node_ref_class(),
                1
            )
        elif key < node.key:
            # 以原有节点为基础创建新节点，也就是被更新的节点会克隆一个新节点
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._insert(self._follow(node.left_ref),key,value_ref)
            )
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._insert(self._follow(node.right_ref),key,value_ref)
            )
        else:
            new_node = BinaryNode.from_node(node,value_ref=value_ref)
        # 返回对节点的引用，address为None说明该新节点还未被存储。
        return self.node_ref_class(referent=new_node)

    def _delete(self,node,key):

        if node is None:
            raise KeyError
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._delete(self._follow(node.left_ref),key)
            )
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._delete(self._follow(node.right_ref), key)
            )
        else:
            left = self._follow(node.left_ref)
            right = self._follow(node.right_ref)
            if left and right:
                # 使用左子树的最大节点作为新的节点，同时删除左子树中的最大节点
                repalcement = self._find_max(left)
                left_ref = self._delete(self._follow(node.left_ref),repalcement.key)
                new_node = BinaryNode(
                    left_ref,
                    repalcement.key,
                    repalcement.value_ref,
                    node.right_ref,
                    left_ref.length + node.right_ref.length + 1,
                )
            elif left:
                #如果存在左子节点则直接返回对左子节点的引用
                return node.left_ref
            else:
                return node.right_ref
        return self.node_ref_class(referent=new_node)

    def _find_max(self,node):
        while True:
            next_node = self._follow(node.right_ref)
            if next_node is None:
                return node
            node = next_node
