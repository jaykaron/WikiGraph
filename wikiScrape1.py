#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 24 15:21:40 2017

@author: joshuakaron
"""

# web crawl a wiki page
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

titles = []
links = []
classes = []

# titles that we arnt interseted in - we might have to add to this as we go
bad_titles = ['Discussion about edits from this IP address [n]',
             'A list of edits made from this IP address [y]',
             'View the content page [c]',
             'Discussion about the content page [t]',
             'Visit the main page',
             'Visit the main page [z]',
             'Guides to browsing Wikipedia',
             'Featured content – the best of Wikipedia',
             'Find background information on current events',
             'Load a random article [x]',
             'Guidance on how to use and edit Wikipedia',
             'Find out about Wikipedia',
             'About the project, what you can do, where to find things',
             'A list of recent changes in the wiki [r]',
             'List of all English Wikipedia pages containing links to this page [j]',
             'Recent changes in pages linked from this page [k]',
             'Upload files [u]',
             'A list of all special pages [q]',
             'Wikipedia:About',
             'Wikipedia:General disclaimer',
             'Help:Category',
             'Wikipedia:FAQ/Categorization',
             'View the category page [c]',
             'Wikipedia:Citation needed',
             'International Standard Book Number (identifier)',
             'Wikipedia:Stub',
             'Help:Maintenance template removal',
             'Geographic coordinate system',
             'Category:All articles lacking reliable references',
             'Category:Coordinates on Wikidata',
             'Category:Articles lacking reliable references from March 2008',
             'Category:All stub articles',
             'Category:Wikipedia articles needing style editing from August 2017',
             'Category:All articles needing style editingt']

# these are to exclude thinks like when the begining of a page says for ___ click here
bad_parents = ['hatnote',
               'navigation-not-searchable']

bad_grandparents = ['mbox-text-span']


url = 'https://en.wikipedia.org/wiki/Ad_Lib,_Inc.'
source_code = requests.get(url)
plain_text = source_code.text
soup = BeautifulSoup(plain_text)

for link in soup.find_all('a'):
    t = link.get('title')
    l = link.get('href')
    
    # filter links with no title
    if t != None and l != None:
        # filter liks that are not wiki
        if l[0:5] == '/wiki':
            # filter generic wiki stuff
            if t[0:13] != 'Edit section:' and t[0:8] != 'Template':
                # filter out bad titles
                if t not in bad_titles:
                    if link.parent.get('class') != bad_parents:
                        if link.parent.parent.get('class') != bad_grandparents:
                            titles.append(t)
                            links.append(l)

