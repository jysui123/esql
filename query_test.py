import requests
import json
import unittest

class TestGeneratedDSL(unittest.TestCase):
    urlSQL = 'http://localhost:9200/_xpack/sql/translate'
    url = 'http://localhost:9200/test/_search'
    # urlFull = 'http://localhost:9200/test/_search?size=200'
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
            # * LIMIT is not tested since the order is not specified
            # url = self.urlDefault if 'LIMIT' in sqls[i] or 'limit' in sqls[i] else self.urlFull
            url = self.url
            sqlQueryPayload = {"query": sqls[i]}
            officialDsl = requests.get(self.urlSQL, data=json.dumps(sqlQueryPayload), headers=self.headers)
            officialRes = requests.get(url, data=officialDsl, headers=self.headers)
            res = requests.get(url, data=json.loads(json.dumps(dsls[i])), headers=self.headers)
            # convert responses to json
            officialRes = officialRes.json() if officialRes and officialRes.status_code == 200 else None
            res = res.json() if res and res.status_code == 200 else None
            self.assertNotEqual(officialRes, None, 'official dsl query {} failed'.format(i+1))
            self.assertNotEqual(res, None, 'dsl query {} failed'.format(i + 1))

            if 'aggs' in dsls[i]:
                if 'groupby' in dsls[i]:
                    self.check_equal_group(res, officialRes, i)
                else:
                    self.check_equal_analysis(res, officialRes, i)
            else:
                if 'COUNT(*)' in sqls[i]:
                    print ('query {} not yet tested'.format(i + 1))
                else:
                    self.check_equal(res, officialRes, i)

    def check_equal(self, res, officialRes, i):
        self.assertEqual(officialRes['hits']['total'], res['hits']['total'], 'number of hits in query {} not match\n\tget {}, expected {}'.format(i + 1, res['hits']['total'], officialRes['hits']['total']))
        self.assertEqual(len(officialRes['hits']['hits']), len(res['hits']['hits']), 'number of result in query {} not match\n\tget {}, expected {}'.format(i+1, res['hits']['total'], officialRes['hits']['total']))
        # check all the row id matches
        officialIds = []
        ids = []
        for v in officialRes['hits']['hits']:
            officialIds.append(v['_id'])
            # officialIds.append(officialRes['hits']['hits'][j]['_id'])
        for v in res['hits']['hits']:
            ids.append(v['_id'])
        officialIds = sorted(officialIds)
        ids = sorted(ids)
        # print(officialIds, '\n')
        # print(ids, '\n')
        for j in range(len(ids)):
            self.assertEqual(ids[j], officialIds[j], 'document id of query {} not match'.format(i + 1))
        print ('query {} returns {} documents, pass'.format(i + 1, len(ids)))

    def check_equal_group(self, res, officialRes, i):
        # check all group number matches
        officialCounts = []
        counts = []
        for v in officialRes['aggregations']['groupby']['buckets']:
            officialCounts.append(v['doc_count'])
        for v in res['aggregations']['groupby']['buckets']:
            counts.append(v['doc_count'])
        self.assertEqual(len(counts), len(officialCounts), 'number of groups in query {} not match\n\tget {}, expected {}'.format(i+1, len(counts), len(officialCounts)))
        officialCounts = sorted(officialCounts)
        counts = sorted(counts)
        for j in range(len(counts)):
            self.assertEqual(counts[j], officialCounts[j], 'document id of query {} not match'.format(i + 1))
        print ('query {} returns {} groups, pass'.format(i + 1, len(counts)))

    def check_equal_analysis(self, res, officialRes, i):
        print ('query {} not yet tested'.format(i + 1))


if __name__ == '__main__':
    unittest.main()