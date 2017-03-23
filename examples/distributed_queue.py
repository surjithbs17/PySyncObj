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
        return item

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
        self.pop_variable = 0
        self.pop_flag = False
    
    @replicated
    def qcreate(self,label):
        #print("Inside qcrea")
        self.q_array[label] = Queue()
        self.q_table[self.qid] = self.q_array[label]
        self.qid = self.qid + 1
        return (self.qid - 1)
    
    
    def qid_get(self,label):
        try:
            key1 = self.q_array[label];
            key2 = self.q_table.keys()[self.q_table.values().index(key1)]
            #print(key1,key2)
            return key2
        except KeyError:
            print("Not valid Index")

    @replicated
    def push(self,qid,item):
        try:
            val = self.q_table[qid].enqueue(item)
            print("Pushed - ", val)
        except KeyError:
            print("Not valid Index")

    @replicated
    def pop(self,qid):
        try:
            val =  self.q_table[qid].dequeue() 
            #self.pop_variable = val;
            #print(val,self.pop_variable)
            #self.pop_flag = True
            print("Popped - " , val )
        except KeyError:
            print("Not valid Index")
    
    '''
    def get_pop_variable(self):
        print(self.pop_variable)
        if self.pop_flag:
            val = self.pop_variable;
            self.pop_flag = False
            return val;
        else:
            pass
    '''
    def qtop(self,qid):
        try:
            #self.q_table[qid].print()
            return self.q_table[qid].top()
        except:
            print("Not valid Index")        
    
    def qsize(self,qid):
        try:
            self.q_table[qid].print()
            return self.q_table[qid].size()
        except:
            print("Not valid Index")


_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...')
        sys.exit(-1)

    selfAddr = sys.argv[1]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = sys.argv[2:]
    print(partners)

    global _g_kvstorage
    _g_kvstorage = KVStorage(selfAddr, partners)

    def get_input(v):
        if sys.version_info >= (3, 0):
            return input(v)
        else:
            return raw_input(v)


    while True:
        print("Leader - ",_g_kvstorage._getLeader())
        cmd = get_input('').split()
        if not cmd:
            continue
        elif cmd[0] == 'qcreate':
            #print("elif",cmd[1])
            _g_kvstorage.qcreate(int(cmd[1]))
        elif cmd[0] == 'push':
            _g_kvstorage.push(int(cmd[1]),int(cmd[2]))
            #print("Pushed ", int(cmd[2]))
        elif cmd[0] == 'pop':
            _g_kvstorage.pop(int(cmd[1]))
            #print("Popped Value - ",_g_kvstorage.get_pop_variable())
        elif cmd[0] == 'qid':
            print("Qid - ",_g_kvstorage.qid_get(int(cmd[1])))
        elif cmd[0] == 'qtop':
            print("Top of the Q - ",_g_kvstorage.qtop(int(cmd[1])))
        elif cmd[0] == 'qsize':
            print("Size of Q - ",_g_kvstorage.qsize(int(cmd[1])))
        else:
            print('Wrong command')

if __name__ == '__main__':
    main()
