#!/usr/bin/python
"""An example website downloader, similar in functionality to
a primitive wget. needs hanzo-warc-tools and requests """  

import logging
import threading
import os
import re
import os.path
import sys

from urlparse import urlparse, urlunparse
from HTMLParser import HTMLParser, HTMLParseError
from optparse import OptionParser
from datetime import datetime
from threading import Thread,Condition,Lock
from collections import deque, namedtuple
from contextlib import contextmanager
from uuid import uuid4

# third party deps
import requests
from hanzo.warctools import warc

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

parser = OptionParser(usage="%prog [options] url (url ...)")

parser.add_option("-o", "--output-directory", dest="output_directory",
                       help="write downloaded files to this directory")
parser.add_option("-l", "--limit", dest="recursion_limit")
parser.add_option("-L", "--log-level", dest="log_level")
parser.add_option("-P", "--prefix", dest="roots", action="append",
                        help="download urls if they match this prefix, can be given multiple times")

parser.add_option("--pool", dest="pool_size")

parser.set_defaults(output_directory=None, recursion_limit=None, pool_size=4, log_level="info", roots = [])




Link = namedtuple('Link','url depth ')


class Scraper(Thread):
    def __init__(self, queue, output_directory, name, **args):
        self.queue = queue 
        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory
        Thread.__init__(self, name=name,  **args)
        self.session = requests.session()

    def run(self):
        if not os.path.exists(self.output):
            os.makedirs(self.output)
        filename = os.path.join(self.output, self.getName()+".warc")
        logging.debug("Creating file: %s"%filename)
        with open(filename,"ab") as fh:
            while self.queue.active():
                with self.queue.consume_top() as link:
                    if link:
                        self.scrape(link, fh)
            logging.info(self.getName()+" exiting")

    def scrape(self, link, fh):
        (depth, url) = link
        logging.info(self.getName()+" getting "+url)
        try:
            response = self.session.get(url)

        except requests.exceptions.RequestException as ex:
            logging.warn("Can't fetch url: %s error:%s"%(url,ex))
            return 

        links = self.extract_links(response, depth)

        if links:
            self.queue.enqueue(links)

        self.write(response, fh)


    def extract_links(self,response, depth):
        links = ()
        content_type = response.headers['Content-Type']
        if content_type.find('html') > -1:
            try:
                html = LinkParser()
                html.feed(response.text)
                html.close()
                links = html.get_abs_links(response.url, depth)

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





def attr_extractor(*names):
        def _extractor(attrs):
            return [value for key,value in attrs if key in names and value]
        return _extractor

def meta_extractor(attrs):
    content = [value for key,value in attrs if key =="content" and value]
    urls = []
    for value in content:
        for pair in value.split(";"):
            bits = pair.split("=",2)
            if len(bits)>1 and bits[0].lower()=="url":
                urls.append(bits[1].strip())
    return urls
                


class LinkParser(HTMLParser):
    def __init__(self):
        self.links = []
        HTMLParser.__init__(self)
        self.base = None

        self.tag_extractor = {
            "a": attr_extractor("href"),
            "applet": attr_extractor("code"),
            "area": attr_extractor("href"),
            "bgsound": attr_extractor("src"),
            "body": attr_extractor("background"),
            "embed": attr_extractor("href","src"),
            "fig": attr_extractor("src"),
            "form": attr_extractor("action"),
            "frame": attr_extractor("src"),
            "iframe": attr_extractor("src"),
            "img": attr_extractor("href","src","lowsrc"),
            "input": attr_extractor("src"),
            "link": attr_extractor("href"),
            "layer": attr_extractor("src"),
            "object": attr_extractor("data"),
            "overlay": attr_extractor("src"),
            "script": attr_extractor("src"),
            "table": attr_extractor("background"),
            "td": attr_extractor("background"),
            "th": attr_extractor("background"),

            "meta": meta_extractor,
            "base": self.base_extractor,
        }

    def base_extractor(self, attrs):
        base = [value for key,value in attrs if key == "href" and value]
        if base:
            self.base = base[-1]
        return ()

    def handle_starttag(self, tag, attrs):
        extractor = self.tag_extractor.get(tag, None)
        if extractor:
            self.links.extend(extractor(attrs))


    def get_abs_links(self, url, base_depth):
        if self.base:
            url = self.base
        full_urls = []
        root = urlparse(url)
        root_dir = os.path.split(root.path)[0]
        for link in self.links:
            parsed = urlparse(link)
            if not parsed.netloc: # does it have no protocol or host, i.e relative
                if parsed.path.startswith("/"):
                    parsed = root[0:2] + parsed[2:5] + (None,)
                else:
                    dir = root_dir
                    path = parsed.path
                    while True:
                        if path.startswith("../"):
                            path=path[3:]
                            dir=os.path.split(dir)[0]
                        elif path.startswith("./"):
                            path=path[2:]
                        else:
                            break

                    parsed = root[0:2] + (os.path.join(dir, path),) + parsed[3:5] + (None,)
                new_link = urlunparse(parsed)
                logging.debug("relative %s -> %s"%(link, new_link))
                link=new_link

            else:
                logging.debug("absolute %s"%link)
            full_urls.append(link)
        return [Link(link, base_depth+1) for link in full_urls]

def scrape(scraper, queue, pool_size):
    pool = [scraper(queue=queue, name="scraper-%d"%name) for name in range(pool_size)]
    for p in pool:
        p.start()

    for p in pool:
        p.join()

    read, excluded = queue.visited, queue.excluded

    logging.info("completed read: %d, excluded %d urls"%(len(read), len(excluded)))

def main(argv):
    (options, urls) = parser.parse_args(args=argv[1:])
    logging.basicConfig(level=LEVELS[options.log_level])

    if len(urls) < 1:
        parser.error("missing url(s)")

    queue = ScraperQueue(
        urls,
        roots = options.roots if options.roots else [os.path.split(url)[0] for url in urls],
        limit=max(0,int(options.recursion_limit)) if options.recursion_limit else None,
    )
    
    def scraper(**args):
        return Scraper(
            output_directory=options.output_directory,
            **args
            )

    scrape(
        queue = queue,
        scraper=scraper,
        pool_size=max(1,int(options.pool_size)),
    )

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

