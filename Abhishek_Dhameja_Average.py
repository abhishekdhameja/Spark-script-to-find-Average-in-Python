from __future__ import print_function

import sys
import os
from operator import add
import re

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Abhishek_Dhameja_Average.py <in> <op>", file=sys.stderr)
        exit(-1)

    sc=SparkContext(appName='AvgNumFlights')

    records=sc.textFile(sys.argv[1]).map(lambda line: line.split(','))\
            .map(lambda line: (line[0],line[3],line[18]))\
            .filter(lambda (x,y,z): x!="id")\
            .map(lambda (x,y,z):(y,z))\
            .map(lambda (x,y): (x.replace("'",""),y))\
            .map(lambda (x,y): (x.replace("-",""),y))\
            .map(lambda (x,y): (re.sub(r'[^\x00-\x7F]+','', x),y))\
            .map(lambda (x,y): (re.sub(r'[^\w\s]',' ',x),y))\
            .map(lambda (x,y): (x.replace("_"," "),y))\
            .map(lambda (x,y):(x.strip(),y))\
            .filter(lambda (x,y): x!="")\
            .map(lambda (x,y):(x.lower(),int(y)))\
            .aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))\
            .map(lambda (x, (y, z)): (x, z,float(y)/z))


    directory=sys.argv[2]
    if not os.path.exists(directory):
        os.mkdir(directory)
    f=open(os.path.join(directory,'Abhishek_Dhameja_task2.txt'),'w')
    output = records.sortByKey(True).collect()

    for (key,count,value) in output:
        f.write("%s\t%i\t%.3f" % (key,count,value)+"\n")

    f.close()