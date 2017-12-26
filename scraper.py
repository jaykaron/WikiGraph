#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 24 15:21:40 2017

@author: joshuakaron
"""

# web crawl a wiki page
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import time

def scrape(url):
    url2 = 'https://en.wikipedia.org'+url
    
    # avoid getting blocked by a website by sending too many requests
    source_code = ''
    while source_code == '':
        try:
            source_code = requests.get(url2)
        except:
            time.sleep(5)
            
        
    plain_text = source_code.text

    strainer_title = SoupStrainer(id="firstHeading")
    title = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_title).get_text()

    strainer_paragraphs = SoupStrainer('p')
    soup = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_paragraphs)
    links = []
    #temp_titles = []

    for link in soup.find_all('a'):
        t = link.get('title')
        l = link.get('href')

        if link.parent.name == 'p':
            # filter out links w/o title
            if t is not None:
                # filter out external links
                if l[0:5] == '/wiki':
                    links.append(l)
                    #temp_titles.append(t)
                else:
                    pass
                    #print("external: " + l)
            else:
                pass
                #print("no title")
        else:
            pass
            #print("non <p> parent")
    return(title, links)

#result = scrape("https://en.wikipedia.org/wiki/1948_Arab%E2%80%93Israeli_War")
#print(result[0])
#print(result[1])

