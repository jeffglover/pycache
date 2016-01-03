#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover,ddorroh
'''

import zmq
import argparse
import pandas as pd
import yamlio
import jsonio
import timers

class Service(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.uri)
        
        with timers.timewith("Service:__init__:serialize"):
            self.serialized_data = self.serialize_func(self.df)
        
    def mainloop(self):
        print("Service:mainloop: Starting...")
        
        while True:
            #  Wait for next request from client
            message = jsonio.JSONMessage(json_message=self.socket.recv())
            
            print("Service:mainloop: recieved {message}".format(message=message))
            
            return_data = ''
            if message.command == "get":
                try:
                    return_data = self.serialize_func(self.df.query(message.query))
                except AttributeError: # No query member in message
                    return_data = self.serialized_data
                except Exception as e: # Bad query
                    print 'Service:mainloop: Bad query: {exception}'.format(exception=e.message)
            else:
                print("Service:mainloop: Unknown command {command}".format(command=message.command))
        
            #  Send reply back to client
            self.socket.send(return_data)

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