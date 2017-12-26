#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 16:08:03 2017

@author: joshuakaron
"""

# wiki graph main
from neo4j.v1 import GraphDatabase
from scraper import scrape
import time

t1 = time.time()

# connect to database
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Wiki"))

session = driver.session()
tx = session.begin_transaction()

# number of nodes
N = 0

# define root url
root = '/wiki/Pandemic'

def add_links(root):
# call scraper
    result = scrape(root)
    
    title = result[0]
    links = result[1]
    
    # remove links to itself
    if root in links:
        links.remove(root)
    
    
    #temp_titles = result[2]
    
    
    # initialize root node
    #MERGE_NODE = 'MERGE (n:Page {url:"'+root+'"}) set n.title="'+title+'",n.crawled="True";'
    
    #tx.run(MERGE_NODE)
    
    
    
    # add first level link nodes
    MERGE_LINKS = """WITH """+str(links)+""" AS links
                     UNWIND links as l
                     MERGE (n:Page {url:'"""+root+"""'})
                     """+'SET n.title="'+title+'",n.crawled="True"'+"""
                     MERGE (m:Page {url:l})
                     ON CREATE SET m.crawled = 'False'
                     merge (n)-[r:Linked]->(m);"""
    tx.run(MERGE_LINKS)
    tx.commit()
    
    N = len(links)
    return N

N += add_links(root)

to_crawl = ['','']
while N < 100 and len(to_crawl)>0:
    # get the links of all the nodes where crawled='False'
    tx = session.begin_transaction()
    NOT_CRAWLED = "match (n:Page {crawled:'False'}) return n.url limit 100"
    to_crawl = tx.run(NOT_CRAWLED)
    
    for node in to_crawl:
        root = node.values()[0]
        if tx.closed():
            tx = session.begin_transaction()
        N += add_links(root)

delta_t = (time.time() - t1)/3600.0
print(str(N)+' Nodes were crawled in '+str(delta_t)+' hours')           