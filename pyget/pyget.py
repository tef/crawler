#!/usr/bin/env python2.5
"""An example website downloader, similar in functionality to
a primitive wget. It has no third party dependencies, and
is written for Python 2.5"""

import sys
import logging

from optparse import OptionParser

from scraper import Scraper

parser = OptionParser(usage="%prog [options] url (url ...)")

parser.add_option("-o", "--output-directory", dest="output_directory",
                       help="write downloaded files to this directory")
parser.add_option("-l", "--limit", dest="recursion_limit")

parser.set_defaults(output_directory=None, recursion_limit=None)

def main(argv):
    logging.basicConfig(level=logging.INFO)
    (options, urls) = parser.parse_args(args=argv[1:])

    if len(urls) < 1:
        parser.error("missing url(s)")

    s = Scraper(
        urls,
        output_directory=options.output_directory,
        limit=options.recursion_limit,
    )

    s.start()

    s.join()

    read, excluded = s.read, s.excluded

    print "completed read: %d, excluded %d urls"%(len(read), len(excluded))

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))

