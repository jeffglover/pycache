'''
Created on Jan 2, 2016

@author: ddorroh,jglover
'''

import time

class timewith():
    '''
    :see: https://zapier.com/engineering/profiling-python-boss/
    '''
    def __init__(self, name=''):
        self.name = name
        self.start = time.time()

    @property
    def elapsed(self):
        return time.time() - self.start

    def checkpoint(self, name=''):
        print '{timer} {checkpoint} took {elapsed} seconds'.format(
            timer=self.name,
            checkpoint=name,
            elapsed=self.elapsed,
            ).strip()
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.checkpoint('finished')
