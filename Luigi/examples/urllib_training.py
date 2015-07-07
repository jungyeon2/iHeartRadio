__author__ = 'jungyeonyoon'

import urllib2


from urllib2 import urlopen
from simplejson import loads
content = loads(urlopen('http://graph.facebook.com/2439131959').read())
print content
