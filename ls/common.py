import json
import os
import sys

from tornado.httpclient import AsyncHTTPClient
from urllib.parse import quote


class ElasticSearchCli:
    def __init__(self, host, port):
        self._uri = 'http://%s:%d' % (host, port)
        self._cli = AsyncHTTPClient()

    def close(self):
        self._cli.close()

    async def create_table(self, table_name, attrs):
        url = '%s/%s' % (self._uri, table_name,)
        body = {
            "mappings": {
                "properties": {
                    "created": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    }
                }
            }
        }
        for k, v in attrs.items():
            body['mappings']['properties'][k] = v

        res = await self._cli.fetch(url, method='PUT', body=json.dumps(body), raise_error=False,
                                    headers={
                                        'Content-Type': 'application/json'
                                    })
        return res.body

    async def edit_doc(self, table_name, id, attrs):
        url = '%s/%s/_doc/%s' % (self._uri, table_name, str(id))

        res = await self._cli.fetch(url, method='PUT', body=json.dumps(attrs), raise_error=False,
                                    headers={
                                        'Content-Type': 'application/json'
                                    })
        return res.body

    async def add_doc(self, table_name, id, attrs):
        url = '%s/%s/_doc/%s' % (self._uri, table_name, str(id))

        res = await self._cli.fetch(url, method='POST', body=json.dumps(attrs), headers={
            'Content-Type': 'application/json'
        }, raise_error=False)
        return res.body

    async def get_docs(self, table_name, from_, size, q):
        url = '%s/%s/_search?q=%s&from=%d&size=%d' % (self._uri, table_name, quote(q), from_, size)
        res = await self._cli.fetch(url, raise_error=False, allow_nonstandard_methods=True)
        return res.body

    async def get_docs_by_body(self, table_name, body):
        url = '%s/%s/_search' % (self._uri, table_name)

        res = await self._cli.fetch(url, method='GET', raise_error=False,
                                    headers={'Content-Type': 'application/json'},
                                    allow_nonstandard_methods=True, body=json.dumps(body))
        return res.body

    def parse_get_docs_hits(self, body):
        jd = json.loads(body)
        rows = []
        total = 0
        if 'hits' in jd and 'hits' in jd['hits']:
            total = jd['hits']['total']['value']
            for hit in jd['hits']['hits']:
                rows.append(hit['_source'])
        return rows, total

    def parse_get_docs_hits_93(self, body):
        jd = json.loads(body)
        rows = []
        total = 0
        if 'hits' in jd and 'hits' in jd['hits']:
            total = jd['hits']['total']['value']
            for hit in jd['hits']['hits']:
                hit['_source']['text'] = hit['_source']['text'][:93]
                rows.append(hit['_source'])
        return rows, total

    async def delete_table(self, table_name):
        url = '%s/%s' % (self._uri, table_name)

        res = await self._cli.fetch(url, method='DELETE', raise_error=False)
        return res.body


class LocalFileStorage(object):
    def __init__(self, root):
        self._root = root

        if not os.path.exists(root):
            os.makedirs(root)

    def get(self, filename):
        fp = os.path.sep.join([self._root, filename])
        with open(fp, 'r') as f:
            return f.read()

    def post(self, filename, content_obj):
        fp = os.path.sep.join([self._root, filename])

        with open(fp, 'w') as f:
            f.truncate()
            f.write(content_obj)


class TailCallException(BaseException):
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs


def tail_call_optimized(func):
    def _wrapper(*args, **kwargs):
        f = sys._getframe()
        # 当前帧的代码和当前帧的前一个帧的前一个帧的代码相同，此时，有三个帧
        if f.f_back and f.f_back.f_back and f.f_code == f.f_back.f_back.f_code:
            raise TailCallException(args, kwargs)  # 抛出异常
        else:
            while True:
                try:
                    return func(*args, **kwargs)
                except TailCallException as e:
                    # 这里捕获到异常，同时，也得到了函数执行时的参数，args和 kwargs
                    # 抛出异常的函数退出了，那么它的帧也就被回收了
                    args = e.args
                    kwargs = e.kwargs

    return _wrapper