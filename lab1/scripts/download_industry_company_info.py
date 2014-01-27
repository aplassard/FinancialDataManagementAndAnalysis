#!/usr/bin/env python

import os
import urllib2
import xml.etree.ElementTree as ET

url_template = '''http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.industry%20where%20id%3D%22{id}%22%3B&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'''
prev_file = os.path.abspath('../data/industry.csv')
out_file  = os.path.abspath('../data/company.csv')

if __name__=='__main__':
	f = open(prev_file,'r')
	out = open(out_file,'w')
	out.write("Industry_ID\tCompany_Name\tCompany_Symbol\n")
	header = f.readline()
	for line in f:
		line = line.strip().split("\t")
		industry_id = line[2]
		print line[2]
		url = url_template.format(id=line[2])
		u = urllib2.urlopen(url)
		a = u.read()
		root = ET.fromstring(a)
		for child in root:
			if child.tag=='results':
				results = child
		for child in results:
			companies = child
		for company in companies:
			company_symbol = company.get('symbol')
			company_name   = company.get('name')
			if company_symbol.find('.')==-1:
				try:
					out.write('\t'.join([industry_id,company_name,company_symbol]))
					out.write('\n')
				except:
					print "Couldn't write %s %s" % (company_name,company_symbol)
	out.close()
