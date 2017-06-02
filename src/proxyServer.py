#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Date:  2017年3月29日
@Author: libin
'''

#2017-04-01  新增心跳机制，防止agent长时间未发送数据被防火墙断开

import socket  
import select  
import sys  
import logging
import time
  
proxy_listen_port_list = [('',38101), ('',38121),('',38111),('',28101),('',28121),('',28111)]  #其中38101和38121是提供给client连接的，28101和28121是提供给agent连接的
front_listen_port = [38101, 38121, 38111, 38112, 38113]

###############################映射关系#######################################

#    client-->proxy(38101)                  client-->proxy(38121)  client-->proxy(38111)
#    agent-->proxy(28101)                   agent-->proxy(28121)   agent-->proxy(28111)
#    client --> proxy --> agent  形成一条链路 

############################################################################  
class Proxy:  
    def __init__(self):  
        self.proxy = []
        self.inputs = []
        self.idel_sock = []
        self.route = {}
            
        for port in front_listen_port + [ x-10000 for x in front_listen_port ]:
            sock_fd = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
            sock_fd.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            sock_fd.bind(('',port)) 
            sock_fd.listen(10)
  
            self.inputs.append(sock_fd)
            self.proxy.append(sock_fd)            
  
    def serve_forever(self):  
        logging.info('[%s] %s' % (time.ctime(),'Proxy start work...'))
        while 1: 
            try:
                readable, _, _ = select.select(self.inputs, [], [])  
                for self.sock in readable:  
                    if self.sock in self.proxy:  
                        self.on_join(self.sock)  
                    else:  
                        data = self.sock.recv(8096)  
                        if not data:  
                            self.on_quit()  
                        else:  
                            self.forward_data(data)
                        
            except Exception, e:
                logging.warning('[%s] %s' % (time.ctime(),str(e)))
                self.on_quit()
  
    def forward_data(self, data):
        #如果没有建立映射，还收到了消息，只有可能是来自agent的心跳，直接丢弃
        if self.sock not in self.route.keys() and self.sock in self.idel_sock:
            addr = self.sock.getpeername()
            logging.info('[%s] Received heart beat from %s : %d' % (time.ctime(),addr[0], addr[1]))
            return 
            
        self.route[self.sock].send(data) 
        
    def get_idel_sock(self, back_port):
        ret_sock = -1
        if len(self.idel_sock) == 0:
            return ret_sock
        
        for fw_sock in self.idel_sock:
            fw_addr = fw_sock.getsockname()
            if fw_addr[1] == back_port - 10000:
                self.idel_sock.remove(fw_sock)
                ret_sock = fw_sock
                break
    
        return ret_sock
    
    def on_join(self, listen_sock):  
        client, addr = listen_sock.accept()  
        logging.info('[%s] %s : %d connected...' % (time.ctime(),addr[0], addr[1]))
        #如果是client连接，先查看是否有可用的agent
        listen_addr = listen_sock.getsockname()
        if listen_addr[1] in front_listen_port:
            forward = self.get_idel_sock(listen_addr[1])
            if forward < 0 :
                logging.warning('[%s] %s' % (time.ctime(),'No available agent, close connection!'))
                client.close() 
                return
            else:#建立映射
                self.inputs.append(client)  
                self.route[client] = forward  
                self.route[forward] = client 
        #如果是agent连接，加入资源池
        else:
            self.inputs.append(client)
            self.idel_sock.append(client)
            
    #退出时 需要删掉前后端链接
    def on_quit(self):  
        self.inputs.remove(self.sock)
        logging.info('[%s] closed connection...' % (time.ctime()))
        self.sock.close()  
        
        #加上下面这个判断是为了防止agent主动断开，而proxy未从资源池里面删除，导致数据发不出来
        if self.sock in self.idel_sock:
            self.idel_sock.remove(self.sock)               
        
        if self.sock in self.route.keys():
            #删除映射关系
            s = self.route[self.sock]
            self.inputs.remove(s)  
            del self.route[s]  
            addr = s.getpeername()
            logging.info('[%s] %s : %d closed connection...' % (time.ctime(),addr[0], addr[1]))
            s.close() 
            
            #这里不可能到达，但还是加上，防止错误
            if s in self.idel_sock:
                self.idel_sock.remove(s) 
            
       
  
if __name__ == '__main__':  
    try:  
        logging.basicConfig(filename='proxyServer.log', filemode="a+", level=logging.DEBUG)
        Proxy().serve_forever()#代理服务器监听的地址  
    except KeyboardInterrupt:  
        sys.exit(1)  
    