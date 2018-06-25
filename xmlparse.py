# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 09:16:40 2018

@author: fischpet
"""

import gzip
import json
import lxml.etree as ET

#infile = 'C:\\Users\\fischpet\\Forschung\\playground\\enwiki-20180520-stub-meta-history1.xml.gz'

#infile_test = 'C:\\Users\\fischpet\\Forschung\\playground\\testxml1.xml'

#infile = 'enwiki-20180520-stub-meta-history1.xml.gz'
#infile = 'enwiki-20180520-stub-meta-history1.xml.gz'
infile = '111.xml.gz'

json_filename = 'json.json'

skipped_tags = ['format', 'text', 'sha1', 'model', 'minor']


def parse_xml(filename):
    f = gzip.open(filename, 'r')
    context = ET.iterparse(f, events=('end',), tag='page')
    wikiData = []
    for event, elem in context:
        tag_page = {}
        rev_count = 0
        rev_array = []
        for child in elem:
            if child.tag == 'revision':
                rev_count += 1
                rev_data = {}
                for rev in child:
                    if rev.tag == 'contributor':
                        user_data = {}
                        for user in rev:
                            user_data[user.tag] = user.text
                    elif rev.tag not in skipped_tags:
                        rev_data[rev.tag] = rev.text
                rev_data['contributor'] = user_data
                rev_array.append(rev_data)
            elif child.tag == 'title':
                tag_page[child.tag] = child.text
            elif child.tag == 'id':
                tag_page[child.tag] = child.text
        #print('revisons =', rev_array)
        tag_page['revision'] = rev_array
        print('page', tag_page)
        save_to_json(tag_page)
        print(str(rev_count)+' Revisions')
        wikiData.append(tag_page)
        # It's safe to call clear() here because no descendants will be accessed
        elem.clear()

        # Also eliminate now-empty references from the root node to <Title>
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    return wikiData


def save_to_json(data):
    with open(json_filename, mode='a', encoding='utf-8') as json_file:
        json.dump(data, json_file)
        json_file.write("\n")


wikiData = parse_xml(infile)
print('wikiData', wikiData)
#save_to_json(wikiData)