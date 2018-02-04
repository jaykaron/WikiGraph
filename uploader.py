import sys, os, pickle, json, time
from neo4j.v1 import GraphDatabase
from multiprocessing.dummy import Pool as ThreadPool

NUMBER_OF_THREADS = 5
INTIAL_DIRECTORY = "scrapedData"
FINAL_DIRECTORY = "uploadedData"

def heaper(num_of_heaps):
    heaps = []
    for i in range(num_of_heaps):
        heaps.append([])

    files = os.listdir(INTIAL_DIRECTORY)
    files.remove("__stats__.txt")

    for i in range(len(files)):
        heaps[i % num_of_heaps].append(files[i])

    return heaps

def upload_many(files, nodes=True):
    while files:
        file_name = files.pop()
        data = json.load(open(INTIAL_DIRECTORY + file_name))
        try:
            if nodes:
                upload_node(data)
            else:
                upload_links(data)
        except Exception as e:
            print("Error on file %s" % file_name)
            print(e)
            files.append(file_name)
        else:
            if not nodes:
                os.rename(INTIAL_DIRECTORY+file_name, FINAL_DIRECTORY+file_name)

def upload_many_nodes(files):
    upload_many(files, nodes=True)

def upload_many_links(files):
    upload_many(files, nodes=False)


def upload_node(data):
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

        escaped_title = title.replace("'", "`")
        add_root = session.run("MERGE (n:P {url:'%s'}) SET n.id = %s SET n.title = '%s'" % (url, pageid, escaped_title))

def upload_links(data):
    pageid = data["pageid"]
    url = data["url"]
    title = data["title"]
    links = data["links"]

    for i in range(len(links)):
        try:
            links[i] = replaced_urls[links[i]]
        except Exception as e:
            pass

    with driver.session() as session:
        add_links = session.run(""" OPTIONAL MATCH (root:P {id:%s})
                                WITH root, %s AS urls
                                UNWIND urls AS u
                                MERGE (n:P {url:u})
                                merge (root)-[:L]->(n)
                                """ % (pageid, links))

start_time = time.time()

replaced_urls = {}      # for pages with multiple urls
try:
    replaced_urls = pickle.load(open("replaced_urls.pckl", "rb"))
except Exception as e:
    pass

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Wiki"))

heaps = heaper(NUMBER_OF_THREADS)

pool = ThreadPool(NUMBER_OF_THREADS)

pool.map(upload_many_nodes, heaps)

total_time = time.time() - start_time
print("Node upload time: %s seconds" % total_time)

# start_time = time.time()
# pool.map(upload_many_links, heaps)
#
# total_time = time.time() - start_time
# print("Link upload time: %s seconds" % total_time)

with open("replaced_urls.pckl", "wb") as f:
    pickle.dump(replaced_urls, f)
