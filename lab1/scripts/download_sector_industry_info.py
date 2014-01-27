#!/usr/bin/env python

import os
import urllib2
import xml.etree.ElementTree as ET

url = '''http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.sectors%3B&diagnostics=false&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'''

if __name__=='__main__':
	u = urllib2.urlopen(url)
	a = u.read()
	root = ET.fromstring(a)
	out_file = open(os.path.abspath('../data/industry.csv'),'w')
	out_file.write('\t'.join(['Sector_Name','Industry_Name','Industry_ID']))
	out_file.write('\n')
	sectors = None
	for child in root:
		if child.tag=='results':
			sectors = child
	for sector in sectors:
		sector_name = sector.get('name')
		for industry in sector:
			industry_name = industry.get('name')
			industry_name = industry_name.replace(",",";")
			industry_id   = industry.get('id')
			out_file.write('\t'.join([sector_name,industry_name,industry_id]))
			out_file.write('\n')
	out_file.close()
