#!/usr/bin/python

import requests
import json
import random
import sys

url = 'http://localhost:9200/test'
indexingRoute = '/_mapping/_doc'
postDataRoute = '/_doc'

headers = {"Content-type": "application/json"}
schema = {
    "properties": {
        "colA": {"type": "text"},
        "colB": {"type": "keyword"},
        "colC": {"type": "text"},
        "colD": {"type": "long"},
        "colE": {"type": "double"},
        "date": {"type": "date"}
    }
}

def genRandStr(letters="abc", len=3):
    return "".join(random.choice(letters) for i in range(len))

def padZero(s, length=2):
    return '0'*(length-len(s)) + s

def genDate(precision="d", startYear=2010):
    dateStr = ""
    if precision in ["y", "M", "d", "h", "m", "s", "ms"]:
        dateStr = str(startYear + random.randint(0, 5))
    if precision in ["M", "d", "h", "m", "s", "ms"]:
        dateStr = dateStr + "-" + padZero(str(random.randint(1, 12)))
    if precision in ["d", "h", "m", "s", "ms"]:
        dateStr = dateStr + "-" + padZero(str(random.randint(1, 28)))
    if precision in ["h", "m", "s", "ms"]:
        dateStr = dateStr + "T" + padZero(str(random.randint(0, 23)))
    if precision in ["m", "s", "ms"]:
        dateStr = dateStr + ":" + padZero(str(random.randint(0, 59)))
    if precision in ["s", "ms"]:
        dateStr = dateStr + ":" + padZero(str(random.randint(0, 59)))
    if precision in ["ms"]:
        dateStr = dateStr + "." + padZero(str(random.randint(0, 999)), 3)
    return dateStr

def genPayload(fields, missingPercent=20):
    payload = {}
    for k, v in fields.iteritems():
        if random.randint(1, 100) > missingPercent:
            payload[k] = v
    return payload

def insertData(nRows, missingPercent):
    for i in range(nRows):
        colA = genRandStr()
        colB = genRandStr("ab", 2)
        colC = colA + " " + genRandStr() + " " + genRandStr()
        colD = random.randint(0, 20)
        colE = random.uniform(0, 20)
        date = genDate('s')

        payload = genPayload({"colA": colA, "colB": colB, "colC": colC, "colD": colD, "colE": colE, "date": date}, missingPercent)
        resp = requests.post(url + postDataRoute, data=json.dumps(payload), headers=headers)
        if resp == None or resp.status_code != 201:
            print("cannot insert data: {}: {}\n".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
            print(i, json.dumps(payload))
            exit(1)
    print("successfully insert {} documents (rows)".format(nRows))

def putMapping():
    resp = requests.put(url + indexingRoute, data=json.dumps(schema), headers=headers)
    if resp == None or resp.status_code != 200:
        print("cannot put mapping: {}: {}".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
        exit(1)
    print("successfully put mapping")

def deleteIndex():
    resp = requests.delete(url)
    if resp == None or resp.status_code not in [200, 202, 204]:
        print("cannot delete index")
        exit(1)
    print("successfully delete index")

def createIndex():
    resp = requests.put(url)
    if resp == None or resp.status_code != 200:
        print("cannot create index")
    print("successfully create index")

nRows = 200
missingPercent = 20
if len(sys.argv) > 4:
    print ("too many arguments")
    exit(1)
if len(sys.argv) > 2:
    nRows = int(sys.argv[2])
if len(sys.argv) > 3:
    missingPercent = int(sys.argv[3])
for i in range(len(sys.argv[1])):
    if i == 0:
        if sys.argv[1][i] != '-':
            print("invalid argument")
            exit(1)
    else:
        if sys.argv[1][i] == 'i':
            insertData(nRows, missingPercent)
        elif sys.argv[1][i] == 'd':
            deleteIndex()
        elif sys.argv[1][i] == 'm':
            putMapping()
        elif sys.argv[1][i] == 'c':
            createIndex()
        else:
            print("invalid argument")
            exit(1)






