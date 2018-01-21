import scrapy
import json
import pickle
import time


class linkSpider(scrapy.Spider):
    name = "links"
    results_dir = "results"
    start_time = time.time()
    custom_settings = {
        #"name": "links",

        "DEPTH_LIMIT": 1,

        # Breadth First Settings
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": 'scrapy.squeues.PickleFifoDiskQueue',
        "SCHEDULER_MEMORY_QUEUE": 'scrapy.squeues.FifoMemoryQueue',

        # Less logs, default: DEBUG
        "LOG_LEVEL": 'INFO',

        #"JOBDIR":"crawls"
    }

    start_urls = [
        'https://en.wikipedia.org/wiki/V%C3%A9rtesacsa'
    ]

    # visited = []
    # try:
    #     with open(self.results_dir + '/visited.txt', 'rb') as v_file:
    #         visited = pickle.load(v_file)
    # except Exception as e:
    #     pass
    # print("start visited: " + str(visited))


    def parse(self, response):
        # check pageid if already done, return empty
        # change title to concat of all texts
        title_segments = response.xpath('//h1[@id="firstHeading"]//text()').extract()
        title = ""
        for seg in title_segments:
            title = title + seg

        pageid = response.xpath('//script')[1].re(r'(?<=ArticleId":)\d*')[0]

        # if pageid in self.visited:
        #     return
        # else:
        #    self.visited.append(pageid)

        all_links_in_main = response.xpath('//div[@id="mw-content-text"]//a[starts-with(@href, "/wiki")]')

        reference_links_hrefs = response.xpath('//div[@class="navbox"]//a').extract()
        infobox_links = response.xpath('//table[contains(@class, "infobox")]//a').extract()
        unwanted_links = reference_links_hrefs + infobox_links

        good_links = set()

        for a in all_links_in_main:
            a_title = a.xpath('./@title').extract_first()
            a_href = a.xpath('./@href').extract_first()

            try:
                unwanted_links.remove(a.extract())
                continue
            except Exception as e:
                pass

            if a_title is not None:
                # filter out other namespaces
                colon_loc = a_href.find(":")
                if colon_loc != -1:
                    namespaces = ["User", "Wikipedia", "File", "MediaWiki", "Template", "Template_talk", "Help", "Category", "Portal", "Book", "Special", "Draft", "Module", "Gadget"]
                    if a_href[6:colon_loc] in namespaces:
                        # print("different namespace: " + l)
                        continue

                # remove anchored links
                hash_loc = a_href.find('#')
                if hash_loc != -1:
                    a_href = a_href[0:hash_loc]
                good_links.add(a_href)

            else:
                pass
                # print("no title")

        data = {
            "title": title,
            'pageid': pageid,
            "url": response.url,
            "links": list(good_links),
            "number_of_links": len(good_links)
        }

        # save file by id
        filename = self.results_dir + '/%s.json' % pageid
        with open(filename, 'w') as f:
            json.dump(data, f)

        for l in good_links:
            next_page = response.urljoin(l)
            yield scrapy.Request(next_page, callback=self.parse)

    def closed(self, reason):
    #     print(self.visited)
    #     with open(self.results_dir + "/visited.txt", 'w+b') as v_file:
    #         pickle.dump(self.visited, v_file)

        total_time = time.time() - self.start_time
        with open(self.results_dir + "/__stats__.txt", 'w') as f:
            f.write("total time: %f seconds" % total_time)
