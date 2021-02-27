#!/usr/bin/python
from configparser import ConfigParser

def get_config(filename, section):
    return ConfigurationParser(filename).get_items(section)

class ConfigurationParser:
    def __init__(self, filename):
        self.parser = ConfigParser()
        self.parser.read(filename)
    
    def get_items(self, section):
        try:
            items = {k: v for k, v in self.parser.items(section)}
        except:
            raise Exception(f'Section {section} not found')
        return items