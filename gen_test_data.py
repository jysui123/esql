#!/usr/bin/python

import requests
import json
import random
import sys

nRows = 100
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

def insertData():
    for i in range(nRows):
        colA = genRandStr()
        colB = genRandStr("ab", 2)
        colC = colA + " " + genRandStr() + " " + genRandStr()
        colD = random.randint(0, 20)
        colE = random.uniform(0, 20)
        date = "2016"

        payload = {"colA": colA, "colB": colB, "colC": colC, "colD": colD, "colE": colE, "date": date}
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
    if resp == None:
        print("cannot create index")
    print("successfully create index")


for i in range(len(sys.argv[1])):
    if i == 0:
        if sys.argv[1][i] != '-':
            print("invalid argument")
            exit(1)
    else:
        if sys.argv[1][i] == 'i':
            insertData()
        elif sys.argv[1][i] == 'd':
            deleteIndex()
        elif sys.argv[1][i] == 'm':
            putMapping()
        elif sys.argv[1][i] == 'c':
            createIndex()
        else:
            print("invalid argument")
            exit(1)






