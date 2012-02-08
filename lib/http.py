import logging

import requests
from htmlparser import LinkParser, HTMLParseError


def fetch(url):
    """Returns a tuple of ("data", [links])"""
    logging.debug("fetching %s"%url)
    try:
        response = requests.get(url)
        content_type = response.headers['Content-Type']
        data = response.text

        if content_type.find("html") >= 0:
            links = extract_links(url, data)
        else:
            logging.debug("skipping extracting links for %s:"%url)
            links = ()

        return (data, links)
    except requests.exceptions.RequestException as ex:
        logging.warn("Can't fetch url: %s error:%s"%(url,ex))
        return (None, ())

def extract_links(url, data):
    links = ()
    try:
        html = LinkParser()
        html.feed(data)
        html.close()
        links = html.get_abs_links(url)

    except HTMLParseError,ex:
        logging.warning("failed to extract links for %s, %s"%(url,ex))

    return links
