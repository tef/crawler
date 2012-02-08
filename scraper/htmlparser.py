"""Simplified html parser that extracts urls from a document"""

import logging
import os.path

from urlparse import urlparse, urlunparse
from HTMLParser import HTMLParser, HTMLParseError

__all__ = ["LinkParser", "HTMLParseError"]

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


    def get_abs_links(self, url):
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
        return full_urls
