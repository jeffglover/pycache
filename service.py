#!/usr/bin/env python2
'''
Created on Jan 2, 2016

@author: jglover,ddorroh
'''

import zmq
import argparse
import yamlio
import json
import pandas as pd
import msgpackio
import timers
from urlparse import urlparse
from socket import gethostname

class Service(object):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.uri)
        self.serialized_data = {}
        try:
            with timers.timewith("Service:__init__:serialize"):
                for key in self.data:
                    self.serialized_data[key] = self.serialize_func(self.data[key])
        except AttributeError:
            self.data = {}
            print("Service:__init__: init with no data")
            
        try:
            info = self.get_info()
            with open(self.dump_path, 'w') as json_fp:
                json.dump(info, json_fp)
            print("Service:info: dump to {file}".format(file=self.dump_path))
        except:
            pass
        
    def mainloop(self):
        print("Service:mainloop: Starting...")
        
        runloop = True
        while runloop:
            #  Wait for next request from client
            message = msgpackio.MessagePackMessage(msgpack_message=self.socket.recv())
            
            return_data = ''
            if message.command == "get":
                return_data = self.get_data(message)
            elif message.command == "set":
                return_data = msgpackio.MessagePackMessage(result=self.set_data(message)).dumps()
            elif message.command == "del":
                return_data = msgpackio.MessagePackMessage(result=self.del_data(message)).dumps()
            elif message.command == "append":
                return_data = msgpackio.MessagePackMessage(result=self.append_data(message)).dumps()
            elif message.command == 'append_all':
                return_data = msgpackio.MessagePackMessage(result=self.append_all_data(message)).dumps()
            elif message.command == 'info':
                return_data = msgpackio.MessagePackMessage(self.info(message)).dumps()
            elif message.command == 'stop':
                runloop = False
                return_data = msgpackio.MessagePackMessage(result=True).dumps()
            else:
                print("Service:mainloop: Unknown command {command}".format(command=message.command))
        
            #  Send reply back to client
            self.socket.send(return_data)
            
    def get_data(self, message):
        print("Service:get_data: recieved {message}".format(message=message))
        return_data = ''
        try:
            return_data = self.serialize_func(self.data[message.name].query(message.query))
                
        except AttributeError: # No query member in message
            try:
                return_data = self.serialized_data[message.name]
            except KeyError: # self.serialized_data[message.name] is not set
                self.serialized_data[message.name] = self.serialize_func(self.data[message.name])
                return_data = self.serialized_data[message.name]
                
        except Exception as e: # Bad query
            print 'Service:mainloop: Bad query: {exception}'.format(exception=e.message)
            
        return return_data
    
    def set_data(self, message):
        print("Service:set_data: recieved command = {command}, name = {name}, data_size = {size}"
                .format(command=message.command, name=message.name, size=len(message.data)))
        self.serialized_data[message.name] = message.data
        self.data[message.name] = self.deserialize_func(message.data)
        
        return True
    
    def append_data(self, message):
        print("Service:append_data: recieved command = {command}, name = {name}, data_size = {size}"
                .format(command=message.command, name=message.name, size=len(message.data)))
        
        # the cached seralized data is invalid
        try:
            del self.serialized_data[message.name]
        except KeyError:
            pass
        
        new_data = self.deserialize_func(message.data)
        
        try:
            self.data[message.name] = pd.concat( [self.data[message.name], new_data] )
            print("Service:append_data: append to existing")
        except KeyError:
            self.data[message.name] = new_data
            print("Service:append_data: create new")
            
        return True
    
    def append_all_data(self, message):
        print("Service:append_all_data: recieved {message}".format(message=message))
        
        # the cached seralized data is invalid
        try:
            del self.serialized_data[message.name]
        except KeyError:
            pass
        
        self.data[message.name] = pd.concat( self.data.values() )
            
        return True
    
    
    def del_data(self, message):
        print("Service:del_data: recieved {message}".format(message=message))
        return_respone = False
        
        # the cached serailzed_data might not be there
        try:
            del self.serialized_data[message.name]
        except KeyError:
            pass
        
        try:
            del self.data[message.name]
            return_respone = True
        except:
            print("Service:del_data: failed to delete")
            
        return return_respone
    
    def info(self, message):
        print("Service:info: recieved {message}".format(message=message))
        info = self.get_info()
        
        try:
            with open(message.dump_path, 'w') as json_fp:
                json.dump(info, json_fp)
            print("Service:info: dump to {file}".format(file=message.dump_path))
        except:
            pass
        
        return info
    
    def get_info(self):
        url_info = urlparse(self.uri)
        
        info = {}
        info['data_keys'] = self.data.keys()
        info['hostname'] = gethostname()
        info['port'] = url_info.port
        info['scheme'] = url_info.scheme
        info['connect_uri'] = "{scheme}://{hostname}:{port}".format(**info)
        
        return info

def parse_args():
    """
    Use argparse module. Santize options and return the parser.
    
    :return:
        args
    """
    parser = argparse.ArgumentParser(description="Configure Track Fitting Jobs")
    parser.add_argument("-c", "--config", dest="config", help="YAML config file", type=yamlio.read_yaml, required=True)
    parser.add_argument("-d", "--document", dest="document", help="YAML Document", type=str, required=True)
    return parser.parse_args()

def main():
    with timers.timewith("main: parse_args"):
        args = parse_args()
#     print(args)

    service = Service(**args.config[args.document])
    service.mainloop()

if __name__ == '__main__':
    main()