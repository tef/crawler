"""A bulk downloader"""
from __future__ import with_statement


import logging
import threading
import os
import re
import os.path

from datetime import datetime
from urlparse import urlparse
from threading import Thread,Condition,Lock
from collections import deque, namedtuple
from contextlib import contextmanager
from uuid import uuid4

from .htmlparser import LinkParser, HTMLParseError

import requests
try:
    from hanzo.warctools import warc
except:
    WarcRecord = None

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
    def __init__(self, queue, output_directory, name, **args):
        self.queue = queue 
        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory
        Thread.__init__(self, name=name,  **args)

    def run(self):
        if not os.path.exists(self.output):
            os.makedirs(self.output)
        filename = os.path.join(self.output, self.getName()+".warc")
        logging.debug("Creating file: %s"%filename)
        with open(filename,"wb") as fh:
            while self.queue.active():
                with self.queue.consume_top() as top:
                    if top:
                        (depth, url) = top
                        self.scrape(url, depth, fh)
            logging.info(self.getName()+" exiting")

    def scrape(self, url, depth, fh):
        logging.info(self.getName()+" getting "+url)
        try:
            response = requests.get(url)

        except requests.exceptions.RequestException as ex:
            logging.warn("Can't fetch url: %s error:%s"%(url,ex))
            return 

        links = self.extract_links(response)

        if links:
            self.queue.enqueue(Link(lnk, depth+1) for lnk in links)

        self.write(response, fh)


    def extract_links(self,response):
        links = ()
        content_type = response.headers['Content-Type']
        if content_type.find('html') > -1:
            try:
                html = LinkParser()
                html.feed(response.text)
                html.close()
                links = html.get_abs_links(response.url)

            except HTMLParseError,ex:
                logging.warning("failed to extract links for %s, %s"%(url,ex))

        else:
            logging.debug("skipping extracting links for %s:"%response.url)
            
        return links

    def write(self,response, fh):
        
        request=response.request
        request_id = "<uin:uuid:%s>"%uuid4()
        response_id = "<uin:uuid:%s>"%uuid4()
        date = warc.warc_datetime_str(datetime.utcnow())

        request_raw = ["%s %s HTTP/1.1"%(request.method, request.full_url)]
        request_raw.extend("%s: %s"%(k,v) for k,v in request.headers.iteritems())
        content = request._enc_data
        request_raw.extend([("Content-Length: %d"%len(content)),"",content])
        request_raw = "\r\n".join(str(s) for s in request_raw)

        response_raw = ["HTTP/1.1 %d -"%(response.status_code)]
        response_raw.extend("%s: %s"%(k,v) for k,v in response.headers.iteritems())
        content=response.content
        response_raw.extend([("Content-Length: %d"%len(content)),"",content])
        response_raw = "\r\n".join(str(s) for s in response_raw)

        requestw = warc.make_request(request_id, date, request.url, ('application/http;msgtype=request', request_raw), response_id)
        responsew = warc.make_response(response_id, date, response.url, ('application/http;msgtype=response', response_raw), request_id)

        requestw.write_to(fh)
        responsew.write_to(fh)




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

