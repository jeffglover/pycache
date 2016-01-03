#!/usr/bin/env python2
'''
Created on Jan 3, 2016

@author: ddorroh,jglover
'''

import zmq
import argparse
import cPickle as pickle


class Node(dict):
    
    def __init__(self, *args, **kwargs):
        super(Node, self).__init__(*args, **kwargs)
        self.__dict__ = self
    
    def __getitem__(self,name):
        names = name.split('.')
        if len(names) > 1:
            return self[names[0]]['.'.join(names[1:])]
        else:
            return super(Node, self).__getitem__(name)
    
    def __setitem__(self, name,value):
        names = name.split('.')
        if len(names) > 1:
            self[names[0]]['.'.join(names[1:])] = value
        else:
            super(Node, self).__setitem__(name,value)


class Service(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.uri)
        self.store = Node()
        
    def mainloop(self):
        print("Service:mainloop: Starting...")
        
        runloop = True
        while runloop:
            #  Wait for next request from client
            message = pickle.loads(self.socket.recv())
            print("Service:mainloop: recieved {message}".format(message=message))
            
            return_data = ''
            if message['command'] == "get":
                return_data = pickle.dumps(self.get(message['key']))
                
            elif message['command'] == "set":
                return_data = pickle.dumps(self.set(message['key'], message['value']))
            else:
                print("Service:mainloop: Unknown command {command}".format(command=message['command']))
        
            #  Send reply back to client
            self.socket.send(return_data)
            
    def get(self, key=None):
        if key is None:
            return self.store
        else:
            return self.store[key]
    
    def set(self, key, value):
        self.store[key] = value
        return True

class Consumer(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        print("Consumer: Connecting to service {uri}".format(**kwargs))
        self.socket.connect(self.uri)
        
    def get(self, key=None):
        message = pickle.dumps({'command' : 'get', 'key' : key})
        self.socket.send(message)
        return pickle.loads(self.socket.recv())
    
    def set(self, key, value):
        message = pickle.dumps({'command' : 'set', 'key' : key, 'value' : value})
        self.socket.send(message)
        return pickle.loads(self.socket.recv())


def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Track Fitting Jobs")
    parser.add_argument("-u", "--uri", dest="uri", help="URI Bind String", type=str, required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    print args
    
    service = Service(uri = args.uri)
    service.mainloop()
    

if __name__ == '__main__':
    main()
