import requests
import json
import random

nRows = 100
url = 'http://localhost:9200/test'
indexingRoute = '/_mapping/_doc'
postDataRoute = '/_doc'

headers = {"Content-type": "application/json"}
schema = {
    "properties": {
        "colA": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "colB": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "colC": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "colD": {"type": "long"},
        "colE": {"type": "double"},
        "date": {"type": "date"}
    }
}

def genRandStr(letters="abc", len=3):
    return "".join(random.choice(letters) for i in range(len))

for i in range(nRows):
    colA = genRandStr()
    colB = genRandStr("ab", 2)
    colC = colA + " " + genRandStr() + " " + genRandStr()
    colD = random.randint(0, 100000)
    colE = random.uniform(1, 100)
    date = "2016"

    payload = {"colA": colA, "colB": colB, "colC": colC, "colD": colD, "colE": colE, "date": date}
    resp = requests.post(url + postDataRoute, data=json.dumps(payload), headers=headers)
    if resp == None or resp.status_code != 200:
        print("cannot insert data: {}: {}\n".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
        print(i, json.dumps(payload))
        exit()

resp = requests.put(url + indexingRoute, data=json.dumps(schema), headers=headers)
if resp == None or resp.status_code != 200:
    print("cannot setup schema: {}: {}".format(resp.status_code, requests.status_codes._codes[resp.status_code]))
    exit()






