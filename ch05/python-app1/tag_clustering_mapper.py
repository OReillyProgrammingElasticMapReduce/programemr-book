#!/usr/bin/env python
# encoding: utf-8
"""
    tag_clustering.py
    
    Created by Hilary Mason on 2011-02-18.
    Copyright (c) 2011 Hilary Mason. All rights reserved.
"""

import csv
import sys

import numpy
from Pycluster import *

class TagClustering(object):
    
    def __init__(self):
        self.load_link_data()
    
    def load_link_data(self):
        for line in sys.stdin:
            print line.rstrip()

if __name__ == '__main__':
        t = TagClustering()