#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
(c) 2017 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on 05 August 2017 10:56 CDT (-0500)
'''

import os
import urllib.request
import logging
import xml.etree.ElementTree as etree

import pdb


def main():
    # setup logging
    logging.basicConfig(level=logging.INFO)
    logging.info('Checking for earlier version of file')
    # check to see if previous version of file exists
    if not os.path.isfile('master_ioc-names_xml.xml'):
        logging.info('File not found, downloading...')
        # download the xml version of the IOC list
        urllib.request.urlretrieve('http://www.worldbirdnames.org/master_ioc-names_xml.xml', 'master_ioc-names_xml.xml')
    else:
        logging.info('File found.  Using existing file.')
    tree = etree.parse('master_ioc-names_xml.xml')
    root = tree.getroot()
    logging.info("Processing {}, version {}, year {}".format(root.tag, root.attrib["version"], root.attrib["year"]))
    main = root.find('list')
    name_lengths = {'order':0, 'family':0, 'genus':0, 'species':0, 'auth':0, 'common':0, 'breeding':0}
    with open('output_species.csv', 'w') as outfile:
        names = {'order':'', 'family':'', 'genus':'', 'species':'', 'auth':'', 'common':'', 'breeding':'', 'extinct':''}
        for order in main.findall('order'):
            names['order'] = order.find('latin_name').text.strip().title()
            for family in order.findall('family'):
                names['family'] = family.find('latin_name').text.strip()
                for genus in family.findall('genus'):
                    names['genus'] = genus.find('latin_name').text.strip()
                    for species in genus.findall('species'):
                        names['species'] = species.find('latin_name').text.strip()
                        names['auth'] = species.find('authority').text.replace(",", "").strip("()").strip()
                        names['common'] = species.find('english_name').text.strip()
                        names['breeding'] = species.find('breeding_regions').text.strip()
                        try:
                            if species.attrib["extinct"] == "yes":
                                names['extinct']=True
                        except KeyError:
                                names['extinct']=False
                        #pdb.set_trace()
                        outfile.write("""{order},{family},{genus},{species},"{auth}",{common},"{breeding}",{extinct}\n""".format(**names))
                        for k in ['order','family','genus','species','auth','common','breeding']:
                            if len(names[k]) > name_lengths[k]:
                                name_lengths[k] = len(names[k])

        print(name_lengths)

            #pdb.set_trace()
    #print(order.tag, order.attrib)



if __name__ == '__main__':
    main()
