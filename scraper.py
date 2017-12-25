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

    for a_tag in soup.find_all('a'):
        a_title = a_tag.get('title')
        href = a_tag.get('href')

        if a_tag.parent.name == 'p':
            # filter out links w/o title
            if a_title is not None:
                # filter out external links
                if href[0:5] == '/wiki':
                    links.append(href)
                else:
                    print("external: " + href)
            else:
                print("no title")
        else:
            print("non <p> parent")
    return(title, links)


result = scrape("https://en.wikipedia.org/wiki/1948_Arab%E2%80%93Israeli_War")
print(result[0])
print(result[1])
