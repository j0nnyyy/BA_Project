# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 09:16:40 2018

@author: fischpet
"""

import gzip
import json
import lxml.etree as etree
from datetime import datetime
import time

#infile = 'C:\\Users\\fischpet\\Forschung\\playground\\enwiki-20180520-stub-meta-history1.xml.gz'

#infile_test = 'C:\\Users\\fischpet\\Forschung\\playground\\testxml1.xml'

#infile = 'enwiki-20180520-stub-meta-history1.xml.gz'
infile = 'xml1.xml.gz'
json_filename = 'XML_JSON.json'

skipped_tags = ['{http://www.mediawiki.org/xml/export-0.10/}format',
                '{http://www.mediawiki.org/xml/export-0.10/}text',
                '{http://www.mediawiki.org/xml/export-0.10/}sha1',
                '{http://www.mediawiki.org/xml/export-0.10/}model',
                '{http://www.mediawiki.org/xml/export-0.10/}minor',
                '{http://www.mediawiki.org/xml/export-0.10/}comment']


def convert_timestamp(iso):
    local = datetime.strptime(iso, '%Y-%m-%dT%H:%M:%SZ').timestamp()
    utc = local - time.timezone
    return int(utc)

def parse_xml(filename):
    f = gzip.open(filename, 'r')
    context = etree.iterparse(f, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    for event, elem in context:
        tag_page = {}
        rev_count = 0
        rev_array = []
        for child in elem:
            if child.tag == '{http://www.mediawiki.org/xml/export-0.10/}revision':
                rev_count += 1
                rev_data = {}
                for rev in child:
                    if rev.tag == '{http://www.mediawiki.org/xml/export-0.10/}contributor':
                        user_data = {}
                        for user in rev:
                            user_data[extract_localpart(user.tag)] = user.text
                    elif rev.tag == '{http://www.mediawiki.org/xml/export-0.10/}timestamp':
                        rev_data[extract_localpart(rev.tag)] = convert_timestamp(rev.text)
                    elif rev.tag not in skipped_tags:
                        rev_data[extract_localpart(rev.tag)] = rev.text
                rev_data['contributor'] = user_data
                rev_array.append(rev_data)
            elif child.tag == '{http://www.mediawiki.org/xml/export-0.10/}title':
                tag_page['title'] = child.text
            elif child.tag == '{http://www.mediawiki.org/xml/export-0.10/}id':
                tag_page['id'] = child.text
        tag_page['revision'] = rev_array
        save_to_json(tag_page)
        # It's safe to call clear() here because no descendants will be accessed
        del rev_array
        rev_data.clear()
        user_data.clear()
        tag_page.clear()
        elem.clear()
        # Also eliminate now-empty references from the root node to <Title>
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def extract_localpart(qname):
    namespace_sep_pos = qname.find('}')
    if namespace_sep_pos >= 0:
        localname = qname[namespace_sep_pos+1:]
    else:
        localname = qname
    return localname


def save_to_json(data):
    with open(json_filename, mode='a', encoding='utf-8') as json_file:
        json.dump(data, json_file)
        json_file.write("\n")


parse_xml(infile)
print('DONE')