
'''
String substitution methods.
:since: Dec 23, 2015
:author: Dustin Dorroh <dustin.dorroh@decisionsciencescorp.com>
'''

import string

def substitute(s,**kwargs):
    '''
    Recursive string.Template substitution. 
    Canonicalize a string by recursive substitution of variables in a dict.
    
    :params:
        s: 
            str object of the template
        **kwargs: 
            dictionary holding the variables to substitute into the template. 
    :return: 
        str object of the completely substituted input s 
    '''
    sub_s = string.Template(s).substitute(kwargs)
    if s == sub_s:
        return s
    else:
        return substitute(sub_s,**kwargs)
