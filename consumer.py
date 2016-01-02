#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover
'''

import zmq
import argparse
import pandas as pd
import yamlio

class Consumer(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        print("Connecting to hello world server...")
        self.socket.connect(self.uri)
        
    def get(self, *args, **kwargs):
        self.socket.send(b"get")
        
        return self.deserialize_func(self.socket.recv())

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

    consumer = Consumer(**args.config[args.document])
    df = consumer.get()    
    print(df.describe())

if __name__ == '__main__':
    main()