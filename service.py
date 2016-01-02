#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover
'''

import zmq
import argparse
import pandas as pd
import yamlio

class Service(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.uri)
        
    def mainloop(self):
        while True:
            #  Wait for next request from client
            command = self.socket.recv()
            
            return_df = None
            if command == "get":
                return_df = self.df
            else:
                print("not sure")
        
            #  Send reply back to client
            self.socket.send(self.serialize_func(return_df))

def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Track Fitting Jobs")
    parser.add_argument("-c", "--config", dest="config", help="YAML config file", type=yamlio.read_yaml, required=True)
    parser.add_argument("-s", "--csv", dest="df", help="CSV", type=pd.read_csv, required=True)
    parser.add_argument("-d", "--document", dest="document", help="YAML Document", type=str, required=True)
    return parser.parse_args()

def main():
    args = parse_args()
#     print(args)

    service = Service(df=args.df, **args.config[args.document])
    service.mainloop()

if __name__ == '__main__':
    main()