#!/usr/bin/env python

import os
import time
import urllib2

url_template = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=snd1oghl1vj1r'
prev_file = os.path.abspath('../data/company.csv')

if __name__=='__main__':
	f = open(prev_file,'r')
	t = int(time.time())
	o = open('../data/%s.csv'%t,'w')
	o.write(','.join(['stock_symbol','date','opening','closing','low','high','volume','market_cap','p/e']))
	o.write('\n')
	f.readline() # The header
	for line in f:
		line = line.strip().split("\t")
		stock = line[2]
		print stock
		url = url_template % stock
		u = urllib2.urlopen(url)
		a = u.read()
		a = a.strip().split(',')
		stock_name = a[1].strip('"')
		date = a[2].strip('"')
		opening = a[3].strip('"')
		low = a[4].strip('"')
		high = a[5].strip('"')
		close = a[6].strip('"')
		volume = a[7].strip('"')
		market_cap = a[8].strip('"')
		p_e = a[9].strip('"')
		o.write(",".join([stock,date,opening,close,low,high,volume,market_cap,p_e]))
		o.write('\n')
#		time.sleep(0.1)
	o.close()


