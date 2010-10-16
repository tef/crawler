#!/usr/bin/env python2.5
"""An example website downloader, similar in functionality to
a primitive wget. It has no third party dependencies, and
is written for Python 2.5"""

import sys
import logging

from optparse import OptionParser

from lib import Harvester

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

parser.add_option("--pool", dest="pool_size")

parser.set_defaults(output_directory=None, recursion_limit=None, pool_size=4, log_level="info")

def main(argv):
    (options, urls) = parser.parse_args(args=argv[1:])
    logging.basicConfig(level=LEVELS[options.log_level])

    if len(urls) < 1:
        parser.error("missing url(s)")

    s = Harvester(
        urls,
        output_directory=options.output_directory,
        limit=max(0,int(options.recursion_limit)) if options.recursion_limit else None,
        pool_size=max(1,int(options.pool_size)),
    )

    s.start()

    s.join()


    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))

