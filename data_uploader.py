import json
import os
from neo4j.v1 import GraphDatabase
import pickle
import sys

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Wiki"))
session = driver.session()

def upload(data):
    pageid = data["pageid"]
    url = data["url"]
    title = data["title"]
    links = data["links"]
    with driver.session() as session:
        # check for pre-existing pageid
        preexisting_full = session.run("OPTIONAL MATCH (n:P {id:%s}) RETURN n" % pageid)
        if preexisting_full.peek()[0] != None:   # pageid already in graph
            # note that there is another url
            replaced_urls[url] = preexisting_full.peek()[0]["url"]
            print("%s (id: %s) already exists in graph" % (title, pageid))
            return

        for i in range(len(links)):
            try:
                links[i] = replaced_urls[links[i]]
            except Exception as e:
                pass

        root = session.run("MERGE (n:P {url:'%s'}) SET n.id = %s SET n.title = '%s' RETURN n" % (url, pageid, title.replace("'", "`"))
        add_links = session.run(""" OPTIONAL MATCH (root:P {id:%s})
                                WITH root, %s AS urls
                                UNWIND urls AS u
                                MERGE (n:P {url:u})
                                merge (root)-[:L]->(n)
                                """ % (pageid, links))


files = os.listdir("scrapedData/")


replaced_urls = {}      # for pages with multiple urls
try:
    replaced_urls = pickle.load(open("replaced_urls.p", "rb"))
except Exception as e:
    pass

n = int(sys.argv[1])
if n == 0:
    n = len(files)

for i in range(n):
    if i == len(files):
        break
    file_name = files[i]
    if file_name[0:2] == "__":
        continue
    data = json.load(open("scrapedData/" + file_name))
    try:
        upload(data)
        os.remove("scrapedData/" + file_name)
    except Exception as e:
        print("error on file: " + file_name)
        print(e)

pickle.dump(replaced_urls, open("replaced_urls.p", "wb"))
