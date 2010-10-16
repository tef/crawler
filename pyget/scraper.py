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
    def __init__(self, urls, output_directory=None):
        self.unread = deque(urls)
        self.roots = urls
        self.read = {}
        self.excluded = []

        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory


    def start(self):
        while self.unread:
            first = self.unread.popleft()

            (data, links) = self.fetch(first)
            if data:
                self.write_file(first, data)

                self.read[first] = data
                for url in links:
                    if self.will_follow(url):
                         if url not in self.read:
                            logging.info("Will visit %s" %url)
                            self.unread.append(url)
                         else:
                            logging.info("Have visited %s" %url)
                    else:
                          self.excluded.append(url)
                          logging.info("Excluding %s" %url)


    def will_follow(self, url):
        return False


    def fetch(self, url):
        logging.debug("fetching %s"%url)
        try:
            response = urlopen(url)
            content_type = response.info()['Content-Type']
            data = response.read()

            if content_type.find("html") >= 0:
                links = self.extract_links(url, data)
            else:
                logging.debug("skipping extracting links for %s:"%url)
                links = None


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
