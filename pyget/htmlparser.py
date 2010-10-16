"""Simplified html parser that extracts urls from a document"""

import logging
import os.path

from urlparse import urlparse, urlunparse
from HTMLParser import HTMLParser, HTMLParseError

__all__ = ["LinkParser", "HTMLParseError"]

def attr_extractor(name):
        def _extractor(attrs):
            return [value for key,value in attrs if key == name and value]
        return _extractor


class LinkParser(HTMLParser):
    def __init__(self):
          self.links = []
          HTMLParser.__init__(self)


    tag_extractor = {
        "a": attr_extractor("href"),
        "link": attr_extractor("href"),
        "img": attr_extractor("src"),
        "script": attr_extractor("src"),
        "iframe": attr_extractor("src"),
        "frame": attr_extractor("src"),
        "embed": attr_extractor("src"),
    }



    def handle_starttag(self, tag, attrs):
        extractor = self.tag_extractor.get(tag, None)
        if extractor:
            self.links.extend(extractor(attrs))


    def get_abs_links(self, url):
        full_urls = []
        root = urlparse(url)
        for link in self.links:
            parsed = urlparse(link)
            if not parsed.netloc: # does it have no protocol or host, i.e relative
                if parsed.path.startswith("/"):
                    parsed = root[0:2] + parsed[2:]
                else:
                    parsed = root[0:2] + (os.path.join(root.path, parsed.path),) + parsed[3:]
                new_link = urlunparse(parsed)
                logging.debug("relative %s -> %s"%(link, new_link))
                link=new_link

            else:
                logging.debug("absolute %s"%link)
            full_urls.append(link)
        return full_urls
