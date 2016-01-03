'''
Created on Jan 2, 2016

@author: jglover,ddorroh
'''


import json

class JSONMessage(object):
    def __init__(self,**kwargs):
        try: # constructing from a json string
            self.__dict__.update(json.loads(kwargs['json_message']))
        except KeyError:
            self.__dict__.update(kwargs)
            
    def __repr__(self, *args, **kwargs):
        return self.__dict__.__repr__()

    def dumps(self):
        return json.dumps(self.__dict__)
