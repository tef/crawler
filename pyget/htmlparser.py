"""Simplified html parser that extracts urls from a document"""

from HTMLParser import HTMLParser, HTMLParseError


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
        return self.links
