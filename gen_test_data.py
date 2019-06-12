#!/usr/bin/python

import requests
import json
import random
import sys

url = 'http://localhost:9200/'
indexingRoute = '/_mapping/_doc'
postDataRoute = '/_doc'

headers = {"Content-type": "application/json"}
# type keyword is often used for aggregation and sorting. It can only searched by its exact value
# type text is often used for text analysis, e.g. search a word in the field
schema = {
    "properties": {
        "colA": {"type": "keyword"},
        "colB": {"type": "keyword"},
        "colC": {"type": "keyword"},
        "colD": {"type": "long"},
        "colE": {"type": "double"},
        "ExecutionTime": {"type": "long"},
        "DomainID": {"type": "keyword"},
        "runID": {"type": "keyword"}
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

def insertData(tableName, nRows, missingPercent):
    for i in range(nRows):
        payload = {}
        payload['colA'] = genRandStr()
        payload['colB'] = genRandStr("ab", 2)
        payload['colC'] = payload['colA'] + " " + genRandStr() + " " + genRandStr()
        payload['colD'] = random.randint(0, 20)
        payload['colE'] = random.uniform(0, 20)
        payload['exeTime'] = random.randint(-100, 200)
        payload['domainID'] = genRandStr("123", 1)
        payload['runID'] = genRandStr('abcdefghijklmnopqrstuvwxyz', 8)

        payload = genPayload(payload, missingPercent)
        resp = requests.post(url+tableName + postDataRoute, data=json.dumps(payload), headers=headers)
        if resp == None or resp.status_code != 201:
            print("cannot insert data: {}: {}\n".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
            print(i, json.dumps(payload))
            exit(1)
    print("successfully insert {} documents (rows)".format(nRows))

def putMapping(tableName):
    resp = requests.put(url+tableName + indexingRoute, data=json.dumps(schema), headers=headers)
    if resp == None or resp.status_code != 200:
        print("cannot put mapping: {}: {}".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
        exit(1)
    print("successfully put mapping")

def deleteIndex(tableName):
    resp = requests.delete(url+tableName)
    if resp == None or resp.status_code not in [200, 202, 204]:
        print("cannot delete index")
        # exit(1)
    print("successfully delete index")

def createIndex(tableName):
    resp = requests.put(url+tableName)
    if resp == None or resp.status_code != 200:
        print("cannot create index")
    print("successfully create index")

nRows = 200
missingPercent = 20
tableNum = 1
if len(sys.argv) > 5:
    print ("too many arguments")
    exit(1)
if len(sys.argv) > 2:
    tableNum = int(sys.argv[2])
if len(sys.argv) > 3:
    nRows = int(sys.argv[3])
if len(sys.argv) > 4:
    missingPercent = int(sys.argv[4])
for table in range(tableNum):
    tableName = 'test' + str(table)
    for i in range(len(sys.argv[1])):
        if i == 0:
            if sys.argv[1][i] != '-':
                print("invalid argument")
                exit(1)
        else:
            if sys.argv[1][i] == 'i':
                insertData(tableName, nRows, missingPercent)
            elif sys.argv[1][i] == 'd':
                deleteIndex(tableName)
            elif sys.argv[1][i] == 'm':
                putMapping(tableName)
            elif sys.argv[1][i] == 'c':
                createIndex(tableName)
            else:
                print("invalid argument")
                exit(1)






