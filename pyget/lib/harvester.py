"""A bulk downloader"""
from __future__ import with_statement


import os.path
import logging
import threading

from threading import Thread,Condition,Lock
from collections import deque
from contextlib import contextmanager

from http import fetch
from store import save_url

class Harvester(Thread, object):
    """A harvester thread. Initialized with a base set of urls and an output directory,
    will start scraping with start()

    It creates a queue for the urls, and spawns a number of sub-threads
    to consume from the queue, and update it with found links

    All sub-tasks exit when the queue is empty and all sub-tasks are waiting

    """

    def __init__(self, urls, output_directory=None, limit=None, pool_size=4):
        Thread.__init__(self)

        roots = [os.path.split(url)[0] for url in urls]
        self.queue = ScraperQueue(urls, roots, limit)

        if not output_directory:
            output_directory = os.getcwd()
        self.output = output_directory

        self.pool_size = pool_size

    def run(self):
        class Scraper(Thread):
            def run(thread):
                while self.queue.active():
                    with self.queue.consume_top() as top:
                        if top:
                            (depth, url) = top
                            logging.info(thread.getName()+" getting "+url)
                            (data, links) = fetch(url)

                            if data:
                                save_url(self.output,url, data)
                                if links:
                                    self.queue.enqueue(links, depth+1)
                logging.info(thread.getName()+" exiting")
        pool = [Scraper(name="scraper-%d"%name) for name in range(self.pool_size)]
        for p in pool:
            p.start()

        for p in pool:
            p.join()






class ScraperQueue(object):
    """
    Created with an initial set of urls to read, a set of roots to constrain
    all links by, and a recursion limit, this is a queue of yet to be read urls.

    There are three basic methods consume_top, active and enqueue

    """
    def __init__(self, urls, roots, limit):
        self.unread_set = set(urls)
        self.unread_queue = deque(((0,url) for url in self.unread_set))
        self.visited = set()

        self.roots=roots
        self.limit=limit
        self.excluded = set()
        self.active_consumers=0

        self.update_lock = Lock()
        self.waiting_consumers = Condition()

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
           return bool(self.unread_queue) or self.active_consumers > 0


    def enqueue(self, links, depth):
        """Updates the queue with the new links at a given depth"""
        if self.limit is None or depth < self.limit:
            with self.update_lock:
                for url in links:
                     if self.will_follow(url):
                                 self.unread_set.add(url)
                                 self.unread_queue.append((depth,url))

                     else:
                         self.excluded.add(url)
                         logging.debug("Excluding %s" %url)
        else:
             self.excluded.update(links)


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
                with self.update_lock:
                    self.active_consumers+=1
                    (depth, url) =self.unread_queue.popleft()
                yield (depth, url)

                with self.update_lock:
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
            logging.debug("Waking up consumers because" +("unread" if self.unread_queue else "finished"))
            self.waiting_consumers.acquire()
            self.waiting_consumers.notifyAll()
            self.waiting_consumers.release()
