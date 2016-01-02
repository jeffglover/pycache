#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover
'''

import zmq
import argparse
import pickle
import pandas as pd

def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Track Fitting Jobs")
    parser.add_argument("-s", "--scan", dest="scan", help="Scan", type=int, required=True)

    return parser.parse_args()

def main():
#     args = parse_args()
#     print(args)

    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")
    
    socket.send(b"get")
    
    df = pickle.loads(socket.recv())
    
    print(df.describe())

if __name__ == '__main__':
    main()