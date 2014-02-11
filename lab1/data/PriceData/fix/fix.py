#!/usr/bin/env python
from sys import argv

if __name__=='__main__':
    f = open(argv[1],'r')
    o = open(argv[2],'w')
    years = ['2009','2010','2011','2012','2013','2014']
    line = f.readline()
    o.write(line)
    for line in f:
        keep = False
        for y in years:
            if line.startswith(y):
                keep=True
        if keep:
            o.write(line)
    o.close()
    f.close()

