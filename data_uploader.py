import json
import os
from neo4j.v1 import GraphDatabase
import sys


class Uploader():
    """docstring for Uploader."""

    def __init__(self, number, driver, replaced_urls):
        self.number = number
        self.driver = driver
        self.replaced_urls = replaced_urls
        self.running = False

    @staticmethod
    def escape_single_quotes(title):
        """ a recursive method to replace single quotes (') with escaped ones (\')"""

        loc = title.find("'")

        # base case, no quotes
        if loc == -1:
            return title

        # recursive, replace with \' and recurse on end of string
        return title[0:loc] + "\\'" + Uploader.escape_single_quotes(title[loc + 1:])


    def upload(self, data):
        pageid = data["pageid"]
        url = data["url"]
        title = data["title"]
        links = data["links"]
        with self.driver.session() as session:
            # check for pre-existing pageid
            preexisting_full = session.run("OPTIONAL MATCH (n:P {id:%s}) RETURN n" % pageid)
            if preexisting_full.peek()[0] != None:   # pageid already in graph
                # note that there is another url
                self.replaced_urls[url] = preexisting_full.peek()[0]["url"]
                print("%s (id: %s) already exists in graph" % (title, pageid))
                return

            for i in range(len(links)):
                try:
                    links[i] = self.replaced_urls[links[i]]
                except Exception as e:
                    pass

            escaped_title = Uploader.escape_single_quotes(title)
            root = session.run("MERGE (n:P {url:'%s'}) SET n.id = %s SET n.title = '%s' RETURN n" % (url, pageid, escaped_title))
            add_links = session.run(""" OPTIONAL MATCH (root:P {id:%s})
                                    WITH root, %s AS urls
                                    UNWIND urls AS u
                                    MERGE (n:P {url:u})
                                    merge (root)-[:L]->(n)
                                    """ % (pageid, links))


    def start(self):
        self.running = True
        files = os.listdir("scrapedData/")

        for file_name in files:
            if file_name[0:3] != "<%d>" % self.number:
                continue
            data = json.load(open("scrapedData/" + file_name))
            try:
                self.upload(data)
                os.remove("scrapedData/" + file_name)
            except Exception as e:
                print("error on file: " + file_name)
                print(e)
        self.close()

    def close(self):
        self.running = False
