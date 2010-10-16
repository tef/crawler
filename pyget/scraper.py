"""A bulk downloader"""

from __future__ import with_statement

import os
import os.path
import logging

from collections import deque
from urllib2 import urlopen, URLError
from urlparse import urlparse
from htmlparser import LinkParser, HTMLParseError

class Scraper(object):
    def __init__(self, urls, output_directory=None, limit=None):
        self.unread_queue = deque(((0,url) for url in urls))
        self.unread_set = set(urls)

        self.roots = [os.path.split(url)[0] for url in urls]
        self.read = {}
        self.limit = limit
        self.excluded = []

        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory


    def start(self):
        while self.unread_queue:
            (depth, first) = self.unread_queue.popleft()

            (data, links) = self.fetch(first)
            self.read[first] = data
            self.unread_set.remove(first)

            if data:
                self.write_file(first, data)

                depth+=1
                if self.limit is None or depth > self.limit:
                    for url in links:
                        if self.will_follow(url):
                             if url not in self.read and url not in self.unread_set:
                                logging.info("Will visit %s" %url)
                                self.unread_queue.append((depth,url))
                                self.unread_set.add(url)
                             else:
                                logging.debug("Have visited %s" %url)
                        else:
                              self.excluded.append(url)
                              logging.debug("Excluding %s" %url)
                else:
                    self.excluded.extend(links)

    def will_follow(self, url):
        return any(url.startswith(root) for root in self.roots)


    def fetch(self, url):
        logging.info("fetching %s"%url)
        try:
            response = urlopen(url)
            content_type = response.info()['Content-Type']
            data = response.read()

            if content_type.find("html") >= 0:
                links = self.extract_links(url, data)
            else:
                logging.debug("skipping extracting links for %s:"%url)
                links = ()


            return (data, links)
        except URLError, ex:
            logging.warn("Can't fetch url: %s error:%s"%(url,ex))
            return (None, ())

    def extract_links(self, url, data):
        links = ()
        try:
            html = LinkParser()
            html.feed(data)
            html.close()
            links = html.get_abs_links(url)

        except HTMLParseError,ex:
            logging.warning("failed to extract links for %s, %s"%(url,ex))

        return links

    def get_file_name(self, url):
        data = urlparse(url)
        path = data.path[1:] if data.path.startswith("/") else path
        filename = os.path.join(self.output+"/", data.netloc, path)
        if filename.endswith("/"):
            filename = filename+"index.html";

        return filename

    def write_file(self, url, data):
        filename = self.get_file_name(url)
        create_necessary_dirs(filename)
        logging.debug("Creating file: %s"%filename)
        with open(filename,"wb") as foo:
            foo.write(data)
            pass



    def wait(self):
        pass

def create_necessary_dirs(filename):
    dir = os.path.dirname(filename)
    if not os.path.exists(dir):
        os.makedirs(dir)
