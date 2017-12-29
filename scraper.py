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

soup_archive = {}
title_archive = {}

def get_url_title(url_end):
    if url_end in title_archive.keys():
        return title_archive[url_end]

    # get website
    url_complete = 'https://en.wikipedia.org' + url_end
    source_code = ''
    while source_code == '':
        try:
            source_code = requests.get(url_complete)
        except:
            time.sleep(5)
    plain_text = source_code.text

    # get title
    title_start = plain_text.find('<h1 id="firstHeading" class="firstHeading" lang="en">')
    title_start += len('<h1 id="firstHeading" class="firstHeading" lang="en">')
    title_end = plain_text[title_start:].find("</h1>")
    title = plain_text[title_start : title_start + title_end]

    # save title
    title_archive[url_end] = title

    # save soup
    strainer_paragraphs = SoupStrainer('p')
    soup = BeautifulSoup(plain_text, "html.parser", parse_only=strainer_paragraphs)
    soup_archive[title] = soup

    return title

def scrape(title):

    soup = soup_archive[title]

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
                    l_title = get_url_title(l)
                    links.add(l_title)
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
    links -= {title}

    # return links as a list
    return(list(links))
