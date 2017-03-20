#!/usr/bin/env python
from __future__ import print_function

import sys

sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated



class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)
    def top(self):
        return self.items[len(self.items)-1]
    def print(self):
        print(self.items)

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        cfg = SyncObjConf(dynamicMembershipChange = True)
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, cfg)
        self.__data = {}
        self.q_array = {}
        self.qid = 0
        self.q_table = {}
    
    @replicated
    def qcreate(self,label):
        #print("Inside qcrea")
        self.q_array[label] = Queue()
        self.q_table[self.qid] = self.q_array[label]
        self.qid = self.qid + 1
        return (self.qid - 1)
    
    @replicated
    def qid_get(self,label):
        key1 = self.q_array[label];
        key2 = self.q_table.keys()[self.q_table.values().index(key1)]
        return key2
    
    @replicated
    def push(self,qid,item):
        self.q_table[qid].enqueue(item)

    @replicated
    def pop(self,qid):
        val =  self.q_table[qid].dequeue() 
        return val

    
    def qtop(self,qid):
        self.q_table[qid].print()
        return self.q_table[qid].top()

    def qsize(self,qid):
        self.q_table[qid].print()
        return self.q_table[qid].size()


_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...')
        sys.exit(-1)

    selfAddr = sys.argv[1]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = sys.argv[2:]

    global _g_kvstorage
    _g_kvstorage = KVStorage(selfAddr, partners)

    def get_input(v):
        if sys.version_info >= (3, 0):
            return input(v)
        else:
            return raw_input(v)


    while True:
        print("Leader - ",_g_kvstorage._getLeader())
        cmd = get_input(">> ").split()
        if not cmd:
            continue
        elif cmd[0] == 'qcreate':
            print("elif",cmd[1])
            _g_kvstorage.qcreate(int(cmd[1]))
        elif cmd[0] == 'push':
            print(_g_kvstorage.push(int(cmd[1]),int(cmd[2])))
        elif cmd[0] == 'pop':
            print(_g_kvstorage.pop(int(cmd[1])))
        elif cmd[0] == 'qid':
            print(_g_kvstorage.qid_get(int(cmd[1])))
        elif cmd[0] == 'qtop':
            print(_g_kvstorage.qtop(int(cmd[1])))
        elif cmd[0] == 'qsize':
            print(_g_kvstorage.qsize(int(cmd[1])))
        else:
            print('Wrong command')

if __name__ == '__main__':
    main()
