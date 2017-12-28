#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: jaykaron
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



def add_links(root, tx):
# call scraper
    result = scrape(root)

    title = result[0]
    links = result[1]

    # remove links to itself
    if root in links:
        links.remove(root)

    # add first level link nodes
    MERGE_LINKS = """WITH """+str(links)+""" AS links
                     UNWIND links as l
                     MERGE (n:Page {url:'"""+root+"""'})
                     """+'SET n.title="'+title+'",n.crawled="True"'+"""
                     MERGE (m:Page {url:l})
                     ON CREATE SET m.crawled = 'False'
                     merge (n)-[r:Linked]->(m);"""
    if tx.closed():
        tx = session.begin_transaction()
    tx.run(MERGE_LINKS)
    tx.commit()

    N = len(links)
    return N


def breadthGraph(root, target_depth, tx):
    N = add_links(root, tx)

    current_depth = 1
    while current_depth < target_depth:
        current_depth +=1
        print(current_depth)
        # get the links of all the nodes where crawled='False'
        tx = session.begin_transaction()

        to_crawl = ['','']
        NOT_CRAWLED = "match (n:Page {crawled:'False'}) return n.url"
        to_crawl = tx.run(NOT_CRAWLED)

        for node in to_crawl:
            new_url = node.values()[0]
            # if tx.closed():
            #     tx = session.begin_transaction()
            N += add_links(new_url, tx)


    delta_t = (time.time() - t1)/3600.0
    print(str(N)+' Nodes were crawled in '+str(delta_t)+' hours')


# define root url
root = '/wiki/Science'
breadthGraph(root, 3, tx)
