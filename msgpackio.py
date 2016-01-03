'''
Created on Jan 2, 2016

@author: ddorroh,jglover
'''

import cStringIO
import pandas as pd
import msgpack

def dumps(df):
    output = cStringIO.StringIO()
    df.to_msgpack(output)
    return output.getvalue()

def loads(buf):
    input_buf = cStringIO.StringIO(buf)
    return pd.read_msgpack(input_buf)

class MessagePackMessage(object):
    def __init__(self,**kwargs):
        try: # constructing from a json string
            self.__dict__.update(msgpack.loads(kwargs['msgpack_message']))
        except KeyError:
            self.__dict__.update(kwargs)
            
    def __repr__(self, *args, **kwargs):
        return self.__dict__.__repr__()

    def dumps(self,**kwargs):
        return msgpack.dumps(self.__dict__,**kwargs)
