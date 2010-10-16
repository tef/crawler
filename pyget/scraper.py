"""A bulk downloader"""

from __future__ import with_statement

import os
import os.path
import logging

from collections import deque
from urllib2 import urlopen, URLError
from urlparse import urlparse


class Scraper(object):
    def __init__(self, urls, output_directory=None):
        self.unread = deque(urls)
        self.read = {}
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
                    if url not in self.read:
                        self.unread.append()




    def fetch(self, url):
        logging.debug("fetching %s"%url)
        try:
            data = urlopen(url)
            links = self.extract_links(url, data)
            return (data, links)
        except URLError, ex:
            logging.warn("Can't fetch url: %s error:%s"%(url,ex))
            return (None, ())



    def write_file(self, url, data):
        filename = self.get_file_name(url)
        create_necessary_dirs(filename)
        logging.debug("Creating file: %s"%filename)
        with open(filename,"w") as foo:
            foo.writelines(data.readlines())
            pass


    def extract_links(self, url, data):
        return ()


    def get_file_name(self, url):
        data = urlparse(url)
        path = data.path[1:] if data.path.startswith("/") else path
        filename = os.path.join(self.output+"/", data.netloc, path)
        if filename.endswith("/"):
            filename = filename+"index.html";

        return filename

    def wait(self):
        pass





def create_necessary_dirs(filename):
    dir = os.path.dirname(filename)
    if not os.path.exists(dir):
        os.makedirs(dir)



