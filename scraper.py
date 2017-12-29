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

plain_texts = {}

def scrape(url_end, title_only=False):
    url_complete = 'https://en.wikipedia.org' + url_end

    soup = ""

    if url_end not in plain_texts.keys():
        # print("getting " + url_end)
        # avoid getting blocked by a website by sending too many requests
        source_code = ''
        while source_code == '':
            try:
                source_code = requests.get(url_complete)
            except:
                time.sleep(5)

        plain_text = source_code.text

        title_start = plain_text.find('<h1 id="firstHeading" class="firstHeading" lang="en">')
        title_start += len('<h1 id="firstHeading" class="firstHeading" lang="en">')
        title_end = plain_text[title_start:].find("</h1>")
        title = plain_text[title_start : title_start + title_end]

        strainer_paragraphs = SoupStrainer('p')
        soup = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_paragraphs)
        plain_texts[url_end] = (title, soup)
    else:
        old = plain_texts[url_end]
        title = old[0]
        soup = old[1]

    if title_only:
        return (title, [])

    links = set()
    for link in soup.find_all('a'):
        t = link.get('title')
        l = link.get('href')

        if link.parent.name == 'p':
            # filter out links w/o title
            if t is not None:
                # filter out external links
                if l[0:5] == '/wiki':
                    hash_loc = l.find('#')
                    if hash_loc != -1:
                        l = l[0:hash_loc]
                    l_title=scrape(l, title_only=True)[0]
                    links.add(l_title)
                    # links.add(l)
                else:
                    pass
                    # print("external: " + l)
            else:
                pass
                # print("no title " + l)
        else:
            pass
            # print("non <p> parent")

    # remove references to itself
    links -= {url_end}

    # return links as a list
    return(title, list(links))
