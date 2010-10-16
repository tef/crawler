from __future__ import with_statement

import logging
import os
import re

import os.path

from urlparse import urlparse


def save_url(output_dir, url, data):
    try:
        filename = get_file_name(output_dir, url)
        create_necessary_dirs(filename)
        logging.info("Creating file: %s"%filename)
        with open(filename,"wb") as foo:
            foo.write(data)
    except StandardError, e:
        logging.warn(e)

re_strip = re.compile(r"[^\w\d\-_]")

def clean_query(arg):
    if arg:
        return re.sub(re_strip,"_", arg)
    else:
        return ""


def get_file_name(output_dir, url):
    data = urlparse(url)
    path = data.path[1:] if data.path.startswith("/") else path
    if path.endswith("/"):
        path = path+"index.html";

    path = path + "."+clean_query(data.query) if data.query else path

    filename = os.path.join(output_dir+"/", data.netloc, path)

    return filename

def create_necessary_dirs(filename):
    dir = os.path.dirname(filename)
    if not os.path.exists(dir):
        os.makedirs(dir)
