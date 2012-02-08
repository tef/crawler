"""A bulk downloader"""
from __future__ import with_statement


import logging
import threading
import os
import re
import os.path

from urlparse import urlparse
from threading import Thread,Condition,Lock
from collections import deque, namedtuple
from contextlib import contextmanager

from .htmlparser import LinkParser, HTMLParseError

import requests

Link = namedtuple('Link','url depth ')

def scrape(scraper, queue, pool_size):
    pool = [scraper(queue=queue, name="scraper-%d"%name) for name in range(pool_size)]
    for p in pool:
        p.start()

    for p in pool:
        p.join()

    read, excluded = queue.visited, queue.excluded

    logging.info("completed read: %d, excluded %d urls"%(len(read), len(excluded)))

class Scraper(Thread):
    def __init__(self, queue, output_directory, **args):
        self.queue = queue 
        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory
        Thread.__init__(self, **args)

    def run(self):
        while self.queue.active():
            with self.queue.consume_top() as top:
                if top:
                    (depth, url) = top
                    self.scrape(url, depth)
        logging.info(self.getName()+" exiting")

    def scrape(self, url, depth):
        logging.info(self.getName()+" getting "+url)
        (data, links) = self.fetch(url)

        if data:
            self.save_url(self.output,url, data)
            if links:
                self.queue.enqueue(Link(lnk, depth+1) for lnk in links)


    def fetch(self, url):
        """Returns a tuple of ("data", [links])"""
        logging.debug("fetching %s"%url)
        try:
            response = requests.get(url)
            content_type = response.headers['Content-Type']
            data = response.text

            if content_type.find("html") >= 0:
                links = self.extract_links(url, data)
            else:
                logging.debug("skipping extracting links for %s:"%url)
                links = ()

            return (data, links)
        except requests.exceptions.RequestException as ex:
            logging.warn("Can't fetch url: %s error:%s"%(url,ex))
            return (None, ())

    def extract_links(self,url, data):
        links = ()
        try:
            html = LinkParser()
            html.feed(data)
            html.close()
            links = html.get_abs_links(url)

        except HTMLParseError,ex:
            logging.warning("failed to extract links for %s, %s"%(url,ex))

        return links

    def save_url(self,output_dir, url, data):
        try:
            filename = get_file_name(output_dir, url)
            create_necessary_dirs(filename)
            logging.debug("Creating file: %s"%filename)
            with open(filename,"wb") as foo:
                foo.write(data)
        except StandardError, e:
            logging.warn(e)




class ScraperQueue(object):
    """
    Created with an initial set of urls to read, a set of roots to constrain
    all links by, and a recursion limit, this is a queue of yet to be read urls.

    There are three basic methods consume_top, active and enqueue

    """
    def __init__(self, urls, roots, limit):
        self.unread_set = set()
        self.unread_queue = deque()
        self.visited = set()

        self.roots=roots
        self.limit=limit
        self.excluded = set()
        self.active_consumers=0

        self.update_lock = Lock()
        self.waiting_consumers = Condition()

        self.enqueue(Link(u, 0) for u in urls)

    def active(self):
       """Returns True if there are items waiting to be read,
          or False if the queue is empty, and no-one is currently processing an item

          If someone is processing an item, then it blocks until it can return True or False
          as above
       """
       if self.unread_queue:
           return True
       elif self.active_consumers == 0:
           return False
       else:
           self.waiting_consumers.acquire()
           while (not self.unread_queue) and self.active_consumers > 0:
               logging.debug(threading.currentThread().getName()+" .... waiting on" + str(self.active_consumers) + str(self.unread_queue));
               self.waiting_consumers.wait()
           logging.debug(threading.currentThread().getName()+"awake" + str(self.unread_queue) +"  " + str(self.active_consumers))
           self.waiting_consumers.release()
           return bool(self.unread_queue)


    def enqueue(self, links):
        """Updates the queue with the new links at a given depth"""
        with self.update_lock:
            for url,depth in links:
                 if self.will_follow(url) and (self.limit is None or depth < self.limit):
                     self.unread_set.add(url)
                     self.unread_queue.append((depth,url))

                 else:
                     self.excluded.add(url)
                     logging.debug("Excluding %s" %url)


    def consume_top(self):
        """Because we need to track when consumers are active (and potentially
           adding things to the queue), we use a contextmanager to take
           the top of the queue:

           i.e  with queue.consume_top() as top: ...

           Will return None if the queue is currently empty
        """
        @contextmanager
        def manager():
            if not self.unread_queue:
                yield None
            else:
                out=None
                with self.update_lock:
                    if self.unread_queue:
                        self.active_consumers+=1
                        out =self.unread_queue.popleft()
                yield out
                # error handling
                if out:
                    with self.update_lock:
                        url = out[1]
                        self.visited.add(url)
                        self.unread_set.remove(url)
                        self.active_consumers-=1
                    self.wake_up_consumers()

        return manager()



    def will_follow(self, url):
        if url not in self.unread_set and url not in self.visited:
            return any(url.startswith(root) for root in self.roots)
        return False



    def wake_up_consumers(self):
        # if there is data to be processed or nothing happening
        if self.unread_queue or self.active_consumers == 0:
            logging.debug("Waking up consumers because " +("unread" if self.unread_queue else "finished"))
            self.waiting_consumers.acquire()
            self.waiting_consumers.notifyAll()
            self.waiting_consumers.release()

# todo strip this out in lieu of warcs
re_strip = re.compile(r"[^\w\d\-_]")

def clean_query(arg):
    if arg:
        return re.sub(re_strip,"_", arg)
    else:
        return ""
def get_file_name(output_dir, url):
    data = urlparse(url)
    path = data.path[1:] if data.path.startswith("/") else path
    if path.endswith("/"):
        path = path+"index.html";

    path = path + "."+clean_query(data.query) if data.query else path

    filename = os.path.join(output_dir+"/", data.netloc, path)

    return filename

def create_necessary_dirs(filename):
    dir = os.path.dirname(filename)
    if not os.path.exists(dir):
        os.makedirs(dir)

