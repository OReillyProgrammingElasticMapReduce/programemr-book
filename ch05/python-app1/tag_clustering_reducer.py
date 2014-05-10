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
        tag_data = self.load_link_data()
        all_tags = []
        all_urls = []
        for url,tags in tag_data.items():
            all_urls.append(url)
            all_tags.extend(tags)
            
        all_tags = list(set(all_tags)) # list of all tags in the space
            
        numerical_data = [] # create vectors for each item
        for url,tags in tag_data.items():
            v = []
            for t in all_tags:
                if t in tags:
                    v.append(1)
                else:
                    v.append(0)
            numerical_data.append(tuple(v))
        data = numpy.array(numerical_data)

        # cluster the items
        # 20 clusters, city block distance, 20 iterations
        labels, error, nfound = kcluster(data, nclusters=6, dist='b', npass=20)

        # print out the clusters
        clustered_urls = {}
        clustered_tags = {}
        i = 0
        for url in all_urls:
            clustered_urls.setdefault(labels[i], []).append(url)
            clustered_tags.setdefault(labels[i], []).extend(tag_data[url])
            i += 1

        tag_list = {}
        for cluster_id,tags in clustered_tags.items():
            tag_list[cluster_id] = list(set(tags))

        for cluster_id,urls in clustered_urls.items():
            print tag_list[cluster_id]
            print urls
                
    def load_link_data(self):
        data = {}
                    
        r = csv.reader(sys.stdin)
        for row in r:
            data[row[0]] = row[1].split(',')
                    
        return data


if __name__ == '__main__':
    t = TagClustering()
