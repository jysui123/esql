import requests
import json
import unittest

class TestGeneratedDSL(unittest.TestCase):
    urlSQL = 'http://localhost:9200/_xpack/sql/translate'
    url = 'http://localhost:9200/test/_search?size=100'
    headers = {"Content-type": "application/json"}
    sqlFileName = 'sqls.txt'
    dslFileName = 'dsls.txt'

    def test_dsl(self):
        sqls = []
        dsls = []
        # read sql queries and generated dsl queries
        with open(self.sqlFileName) as f:
            sqls = f.readlines()
        with open(self.dslFileName) as f:
            dsls = f.readlines()
        self.assertEqual(len(sqls), len(dsls), 'number of sql test cases ({}) and generated dsl queries ({}) not match'.format(len(sqls), len(dsls)))
        # query local elastic server and check whether the results are identical
        for i in range(len(sqls)):
            sqlQueryPayload = {"query": sqls[i]}
            officialDsl = requests.get(self.urlSQL, data=json.dumps(sqlQueryPayload), headers=self.headers)
            officialRes = requests.get(self.url, data=officialDsl, headers=self.headers)
            res = requests.get(self.url, data=json.loads(json.dumps(dsls[i])), headers=self.headers)
            # convert responses to json
            officialRes = officialRes.json() if officialRes and officialRes.status_code == 200 else None
            res = res.json() if res and res.status_code == 200 else None
            self.assertNotEqual(officialRes, None, 'dsl query {} failed'.format(i+1))
            self.assertNotEqual(res, None, 'dsl query {} failed'.format(i+1))
            self.assertEqual(officialRes['hits']['total'], res['hits']['total'], 'number of result query {} not match\n\tget {}, expected {}'.format(i+1, res['hits']['total'], officialRes['hits']['total']))
            # check all the row id matches
            officialIds = []
            ids = []
            for i in range(officialRes['hits']['total']):
                officialIds.append(officialRes['hits']['hits'][i]['_id'])
            for i in range(res['hits']['total']):
                ids.append(res['hits']['hits'][i]['_id'])
            # print(officialIds)
            # print("\n******\n")
            officialIds = sorted(officialIds)
            ids = sorted(ids)
            for i in range(len(ids)):
                self.assertEqual(ids[i], officialIds[i], 'document id of query {} not match'.format(i+1))


if __name__ == '__main__':
    unittest.main()