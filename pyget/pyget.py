#!/usr/bin/env python2.5
"""An example website downloader, similar in functionality to
a primitive wget"""

import sys
import logging

from optparse import OptionParser

from scraper import Scraper



parser= None # created later

def main(argv):
    logging.basicConfig(level=logging.DEBUG)
    (options, urls) = parser.parse_args(args=argv[1:])

    if len(urls) < 1:
        parser.error("missing url(s)")

    s = Scraper(urls, output_directory=options.output_directory)

    s.start()

    s.wait()

    return 0


def create_parser():
    parser = OptionParser(usage="%prog [options] url (url ...)")

    parser.add_option("-o", "--output-directory", dest="output_directory",
                          help="write downloaded files to this directory")
    parser.add_option("-l", "--limit", dest="recursion_limit")

    parser.set_defaults(output_directory=None, recursion_limit=None)

    return parser


parser = create_parser()

if __name__ == '__main__':
    sys.exit(main(sys.argv))

