#!/usr/bin/env python2.5
"""An example website downloader, similar in functionality to
a primitive wget. It has one third party dependency (requests), and
is written for Python 2.5. MIT License"""

import sys
import os.path
import logging

from optparse import OptionParser

from scraper import scrape, Scraper, ScraperQueue

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

