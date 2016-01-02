#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover
'''

import time
import zmq
import argparse

import pandas as pd
import pickle

def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Track Fitting Jobs")
    parser.add_argument("-c", "--csv", dest="df", help="CSV", type=pd.read_csv, required=True)

    return parser.parse_args()

def main():
    args = parse_args()
#     print(args)
    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")
    
    while True:
        #  Wait for next request from client
        message = socket.recv()
        print("Received request: %s" % message)
        try:
            command, arg = message.split()
            scan_number= int(arg)
        except ValueError:
            command = message
        
        return_df = None
        if command == "get":
            return_df = args.df
        else:
            print("not sure")
    
        #  Send reply back to client
        socket.send(pickle.dumps(return_df))

if __name__ == '__main__':
    main()