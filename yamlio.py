'''
Created on Jan 2, 2016

@author: jglover
'''

import yaml
import substitute

def read_yaml(path):
    '''
    Read YAML file from file path.
    :return: 
        dictionary of the yaml configuration
    '''
    with open(path,'r') as read_f:
        return yaml.load(read_f)

def read_and_substitute_yaml(path):
    '''
    Read YAML file from file path and canonicalize variables by recursive substitution.
    
    :return: 
        dictionary of the yaml configuration
    '''
    yaml_config = read_yaml(path)
    for key,value in yaml_config.iteritems():
        if isinstance(value, str):
            yaml_config[key] = substitute(value,**yaml_config)
    return yaml_config
