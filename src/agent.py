#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Date:  2017年3月29日
@Author: libin
'''

from socket import *
import select  
import sys

proxySrvIP = "serverip"
cabalsrvIP = "192.168.1.128"
proxy_listen_port = [28101, 28121, 28111, 28112, 28113]

#测试  python -m SimpleHTTPServer 38101

class Agent:  
    def __init__(self):  
        """
                程序初始化时需要先连接proxyServer，等待client的消息过来之后，再连接cabal服务器
        """
        try:
            self.route = {}
            self.inputs = []
            
            for port in proxy_listen_port:
                sock_fd = socket(AF_INET, SOCK_STREAM) 
                sock_fd.connect((proxySrvIP, port)) 
                self.inputs.append(sock_fd)
        except Exception: 
            print 'exception catched, can not connect to srv : %s'%(proxySrvIP)
            sys.exit()

    def work(self):
        print 'agent start work...'
        try:
            while 1:  
                readable, _, _ = select.select(self.inputs, [], [])  
                for self.sock in readable:  
                    data = self.sock.recv(8096)  
                    if not data:  
                        self.on_quit()  
                    else:  
                        self.forward(data)
                        
        except Exception, e:
            print "Get exception: ", str(e)  
            
            
    def forward(self, data):
        sock_addr = self.sock.getpeername()
#         print "recv data :%s from addr: "%data, sock_addr
#        
        try:
            if self.sock not in self.route.keys():
                print "create route for cabal server port: %d..."%(sock_addr[1]+10000)
                cabalSock = socket(AF_INET, SOCK_STREAM)
                cabalPort = sock_addr[1] + 10000
                cabalSock.connect((cabalsrvIP, cabalPort))
                self.inputs.append(cabalSock)  
                self.route[self.sock] = cabalSock  
                self.route[cabalSock] = self.sock 
                
            self.route[self.sock].send(data) 
        
        except Exception, e: 
            print str(e)
            print 'exception catched, can not connect to srv : %s'%(cabalsrvIP)
            sys.exit()

    #退出时 需要删掉前后端链接
    def on_quit(self):  
        for s in self.sock, self.route[self.sock]:  
            self.inputs.remove(s)  
            del self.route[s]  
            
            addr = s.getpeername()
            print "closed connection : ", addr
            s.close() 
            
            #退出之后重新连接proxy，保证下次继续使用
            if addr[1] in proxy_listen_port:
                print "reconnect to %s:%d"%(proxySrvIP, addr[1])
                sock_fd = socket(AF_INET, SOCK_STREAM)
                sock_fd.connect((proxySrvIP, addr[1]))
                self.inputs.append(sock_fd)
        
                   
if __name__ == '__main__':  
    try:  
        Agent().work()
    except KeyboardInterrupt:  
        sys.exit(1) 
