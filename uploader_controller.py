import pickle
import sys
import data_uploader
from neo4j.v1 import GraphDatabase
from multiprocessing.dummy import Pool as ThreadPool 

NUMBER_OF_UPLOADERS = 5

replaced_urls = {}      # for pages with multiple urls
try:
    replaced_urls = pickle.load(open("replaced_urls.p", "rb"))
except Exception as e:
    pass

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Wiki"))

uploaders = []
for i in range(NUMBER_OF_UPLOADERS):
    uploaders.append(data_uploader.Uploader(i, driver, replaced_urls))
    try:
        threading.start_new_thread(uploaders[i].start())
    except Exception as e:
        print(e)
