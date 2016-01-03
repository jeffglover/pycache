#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover,ddorroh
'''

import zmq
import argparse
import yamlio
import jsonio


class Consumer(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        print("Consumer: Connecting to service {uri}".format(**kwargs))
        
        self.socket.connect(self.uri)
        
    def get(self, name, query=None):
        if query is None:
            message = jsonio.JSONMessage(command='get', name=name)
        else:
            message = jsonio.JSONMessage(command='get',query=query, name=name)
        
        self.socket.send_string(message.dumps())
        return self.deserialize_func(self.socket.recv_string())
    
    def set(self, name, data):
        message = jsonio.JSONMessage(command='set', name=name, data=self.serialize_func(data))
        self.socket.send_string(message.dumps())
        
        self.socket.recv_string()
        

def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Consumer")
    parser.add_argument("-c", "--config", dest="config", help="YAML config file", type=yamlio.read_yaml, required=True)
    parser.add_argument("-d", "--document", dest="document", help="YAML Document", type=str, required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    print(args)
    
    config = args.config[args.document]
    
    consumer = Consumer(**config)
    try:
        df = consumer.get(name=config['name'], query=config['query'])
    except KeyError:
        df = consumer.get(name=config['name'])
    print(df.describe())

if __name__ == '__main__':
    main()
