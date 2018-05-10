# -*- coding: utf-8 -*-
__author__ = 'Allen'

import os
import struct
import portalocker #pip install portalocker

class Storage(object):
    SUPERBLOCK_SIZE = 4096#超级块，在这里由它保存整个数据库文件的一些基本信息
    INTEGER_FORMAT = "!Q"# "Q" 表示无符号长整形，"!" 表示网络流的字节序，也就是大端字节序
    INTEGER_LENGTH = 8#统一整数的大小为8个字符

    def __init__(self,f):
        self._f = f
        self.locked = False
        self._ensure_superblock()

    def _ensure_superblock(self):
        '''初始化超级块'''
        self.lock()
        self._seek_end()#将游标移动至文件末尾
        end_address = self._f.tell()#获取当前游标位置，这里同时也是文件大小
        if end_address < self.SUPERBLOCK_SIZE:
            # 如果文件大小小于超级块大小那么必须为超级块分配足够的空间
            # 写入一串二进制零
            self._f.write(b'\x00' * (self.SUPERBLOCK_SIZE - end_address))
        self.unlock()

    def lock(self):
        '''
        给文件加锁
        :return:
        '''
        if not self.locked:
            portalocker.lock(self._f,portalocker.LOCK_EX)
            self.locked = True
            return True
        else:
            return False

    def unlock(self):
        '''
        解锁
        :return:
        '''
        if self.locked:
            self._f.flush()
            portalocker.unlock(self._f)
            self.locked = False

    def _seek_end(self):
        #file.seek(offset,whence=0),offset开始的偏移量，
        # whence给offset参数的一个定义，0代表从文件开头算起，1代表从当前位置算起，2代表从文件末尾算起
        #os.SEEK_END = 2
        self._f.seek(0,os.SEEK_END)

    def _seek_superblock(self):
        #文件游标指向开头，也就是超级块开始处
        self._f.seek(0)

    def _bytes_to_integer(self,integer_bytes):
        # 字节转换整数
        return struct.unpack(self.INTEGER_FORMAT,integer_bytes)[0]

    #因为 Python 的整数类型不是固定长的，所以我们需要用到struct模块先将 Python 整数打包成 8 个字节，再写入到文件中去
    def _integer_to_bytes(self,integer):
        # 整数转换字节
        return struct.pack(self.INTEGER_FORMAT,integer)

    def _read_integer(self):
        return self._bytes_to_integer(self._f.read(self.INTEGER_LENGTH))

    def _write_integer(self,integer):
        self.lock()
        self._f.write(self._integer_to_bytes(integer))

    def read(self,address):
        '''读数据'''
        self._f.seek(address)
        length = self._read_integer()#先读取文件长度
        data = self._f.read(length)#再根据长度读数据
        return data

    def write(self,data):
        '''写数据'''
        self.lock()
        self._seek_end()
        object_address = self._f.tell()#获取当前文件游标指针位置
        self._write_integer(len(data))#开头写入这段数据的长度
        self._f.write(data)#接着写入数据
        return object_address#返回新写入数据的起始地址

    def get_root_address(self):
        '''获取文件根地址'''
        self._seek_superblock()#移动游标指针位置到文件最开头
        root_address = self._read_integer()#根地址是读取文件开头的整数
        return root_address

    def commit_root_address(self,root_address):
        '''将根地址写入文件开头'''
        self.lock()
        self._f.flush()
        self._seek_superblock()
        self._write_integer(root_address)
        self._f.flush()
        self.unlock()

    def close(self):
        '''关闭文件'''
        self.unlock()
        self._f.close()

    @property
    def closed(self):
        '''判断文件是否关闭'''
        return self._f.closed