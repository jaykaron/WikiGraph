import sys, os, pickle, json, time
import data_uploader
from neo4j.v1 import GraphDatabase
from multiprocessing.dummy import Pool as ThreadPool

NUMBER_OF_THREADS = 5

def sort_files(n_of_piles):
    sorted_files = []
    for i in range(n_of_piles):
        sorted_files.append([])

    files = os.listdir("scrapedData/")
    for file_name in files:
        if file_name[0] != "<":
            continue
        number = int(file_name[1])
        sorted_files[number].append(file_name)
    return sorted_files

def upload_many(files):
    while files:
        file_name = files.pop()
        data = json.load(open("scrapedData/" + file_name))
        try:
            upload(data)
        except Exception as e:
            print("Error on file %s" % file_name)
            print(e)
        else:
            # delete file
            pass

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

        escaped_title = escape_single_quotes(title)
        root = session.run("MERGE (n:P {url:'%s'}) SET n.id = %s SET n.title = '%s' RETURN n" % (url, pageid, escaped_title))
        add_links = session.run(""" OPTIONAL MATCH (root:P {id:%s})
                                WITH root, %s AS urls
                                UNWIND urls AS u
                                MERGE (n:P {url:u})
                                merge (root)-[:L]->(n)
                                """ % (pageid, links))

def escape_single_quotes(title):
    """ a recursive method to replace single quotes (') with escaped ones (\')"""

    loc = title.find("'")

    # base case, no quotes
    if loc == -1:
        return title

    # recursive, replace with \' and recurse on end of string
    return title[0:loc] + "\\'" + Uploader.escape_single_quotes(title[loc + 1:])

start_time = time.time()

replaced_urls = {}      # for pages with multiple urls
try:
    replaced_urls = pickle.load(open("replaced_urls.p", "rb"))
except Exception as e:
    pass

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Wiki"))

sorted_files = sort_files(NUMBER_OF_THREADS)

pool = ThreadPool(NUMBER_OF_THREADS)

pool.map(upload_many, sorted_files)

total_time = time.time() - start_time
print("Total time: %s seconds" % total_time)
