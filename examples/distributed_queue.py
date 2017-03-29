#!/usr/bin/env python
from __future__ import print_function

import sys
import socket
sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated


UDP_IP="localhost"
UDP_PORT = 0 
sock = 0
addr = 0


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
        #global addr 
        #print("Addr ",addr)
        #reply = "Q created - %d"%(self.qid - 1)
        #sock.sendto(reply,addr)
        return (self.qid - 1)
    
    
    def qid_get(self,label):
        global addr 
        try:
            key1 = self.q_array[label];
            key2 = self.q_table.keys()[self.q_table.values().index(key1)]
            #print(key1,key2)
            sock.sendto(str(key2),addr)
            return key2
        except KeyError:
            print("Not valid Index")
            sock.sendto("Not Valid Index",addr)

    @replicated
    def push(self,qid,item):
        global addr 
        try:
            val = self.q_table[qid].enqueue(item)
            print("Pushed - ", val)
            #reply = "Pushed %d"%(val)
            #sock.sendto(reply,addr)
        except KeyError:
            print("Not valid key")
        except IndexError:
            print("Not valid Index")
            #sock.sendto("Not Valid Index",addr)
    
    @replicated
    def pop(self,qid):
        global addr 
        try:
            val =  self.q_table[qid].dequeue() 
            #self.pop_variable = val;
            #print(val,self.pop_variable)
            #self.pop_flag = True
            print("Popped - " , val )
            #reply = "Popped - %d"%(val)
            #sock.sendto(reply,addr)
        except KeyError:
            print("Not valid key")
        except IndexError:
            print("Not valid Index")
        

            #sock.sendto("Not Valid Index",addr)
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
        global addr 
        try:
            #self.q_table[qid].print()
            val = self.q_table[qid].top()
            reply = "Top - %d"%(val)
            #print reply
            sock.sendto(reply,addr)
            return val
        except:
            print("Not valid Index")
            sock.sendto("Not Valid Index",addr)   

    def qsize(self,qid):
        global addr 
        try:
            self.q_table[qid].print()
            val = self.q_table[qid].size()
            reply = "Qsize - %d"%(val)
            sock.sendto(reply,addr)
            return val
        except:
            print("Not valid Index")
            sock.sendto("Not Valid Index",addr)

_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...')
        sys.exit(-1)

    selfAddr = sys.argv[1]
    [lo_ip,lo_port] = selfAddr.split(':')
    if selfAddr == 'readonly':
        selfAddr = None

    partners = sys.argv[2:]
    print(partners)

    global _g_kvstorage
    
    global UDP_PORT
    global sock
    UDP_PORT = (int(lo_port)+2000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    sock.bind((UDP_IP,UDP_PORT))

    print("UDP Client Port ",UDP_PORT)
    _g_kvstorage = KVStorage(selfAddr, partners)

    '''
    def get_input(v):
        if sys.version_info >= (3, 0):
            return input(v)
        else:
            return raw_input(v)
    '''
    while True:
        global addr
        data,temp = sock.recvfrom(1024)
        print("Addr ",temp)
        addr = temp
        print("Leader - ",_g_kvstorage._getLeader())
        cmd = data.split(' ')
        if not cmd:
            continue
        elif cmd[0] == 'qcreate':
            #print("elif",cmd[1])
            _g_kvstorage.qcreate(int(cmd[1]))
            sock.sendto("Q Created ",addr)
        elif cmd[0] == 'push':
            _g_kvstorage.push(int(cmd[1]),int(cmd[2]))
            sock.sendto("Pushed data",addr)
            #print("Pushed ", int(cmd[2]))
        elif cmd[0] == 'pop':
            _g_kvstorage.pop(int(cmd[1]))
            sock.sendto("Popped Data",addr)
            #print("Popped Value - ",_g_kvstorage.get_pop_variable())
        elif cmd[0] == 'qid':
            print("Qid - ",_g_kvstorage.qid_get(int(cmd[1])))
        elif cmd[0] == 'qtop':
            print("Top of the Q - ",_g_kvstorage.qtop(int(cmd[1])))
        elif cmd[0] == 'qsize':
            print("Size of Q - ",_g_kvstorage.qsize(int(cmd[1])))
        else:
            print('Wrong command')
            sock.sendto("Wrong Command",addr)

if __name__ == '__main__':
    main()
