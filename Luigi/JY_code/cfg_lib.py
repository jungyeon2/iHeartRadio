__author__ = 'jungyeonyoon'

import ConfigParser


## Read return cfg's section items in dictionary type
def read_config(self, section_name, filename):

    config = ConfigParser.ConfigParser()
    config.read(filename)

    return dict(config.items(section_name))

