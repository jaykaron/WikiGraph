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


def scrape(url):
    source_code = requests.get(url)
    plain_text = source_code.text

    strainer_title = SoupStrainer(id="firstHeading")
    title = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_title).get_text()

    strainer_paragraphs = SoupStrainer('p')
    soup = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_paragraphs)
    links = []

    for link in soup.find_all('a'):
        t = link.get('title')
        l = link.get('href')

        if link.parent.name == 'p':
            # filter out links w/o title
            if t is not None:
                # filter out external links
                if l[0:5] == '/wiki':
                    links.append(l)
                else:
                    print("external: " + l)
            else:
                print("no title")
        else:
            print("non <p> parent")
    return(title, links)

result = scrape("https://en.wikipedia.org/wiki/1948_Arab%E2%80%93Israeli_War")
print(result[0])
print(result[1])
