import logging
import os
import pytest, json, pprint


@pytest.mark.asyncio
async def test_create_table(esc):
    body = await esc.create_table('test1', attrs={
        'name': {
            'type': 'text1',
        }
    })
    # jd = json.loads(body)
    # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_add_doc(esc):
    for i in range(2, 55):
        body = await esc.add_doc('test1', i, {
            'name': '啊啊啊啊' + str(i)
        })
        # jd = json.loads(body)
        # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_query(esc):
    body = await esc.get_docs('test1', 0, 10, 'sdaf')
    # jd = json.loads(body)
    # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_edit(esc):
    body = await esc.edit_doc('test1', 1, {
        'name': '修改过的啊'
    })
    # jd = json.loads(body)
    # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_add_test_data(esc):
    fp = os.path.sep.join([os.path.dirname(__file__), '..', 'ls', 'schema.json'])
    jd = json.load(open(fp, 'r'))
    tb = jd['website']['table_name']
    for i in range(100, 200):
        tos = str(i)
        body = await esc.add_doc(tb, i, {
            'url': 'url' + tos,
            'text': 'texttt' + tos,
            'pickle_path': 'pickle_path' + tos
        })
        # jd = json.loads(body)
        # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_query_test_data(esc):
    fp = os.path.sep.join([os.path.dirname(__file__), '..', 'ls', 'schema.json'])
    jd = json.load(open(fp, 'r'))
    tb = jd['website']['table_name']

    body = await esc.get_docs(tb, 0, 5, q='text')
    # pprint.pprint(esc.parse_get_docs_hits(body))


@pytest.mark.asyncio
async def test_add_test_data1():
    from ls.db import get_full_index_search_cli
    esc = get_full_index_search_cli()
    fp = os.path.sep.join([os.path.dirname(__file__), '..', 'ls', 'schema.json'])
    jd = json.load(open(fp, 'r'))
    tb = jd['website']['table_name']
    for i in range(400, 540):
        tos = str(i)
        body = await esc.add_doc(tb, i, {
            'url': '链接' + tos,
            'title': '标题' + tos,
            'text': '描述' + tos,
            'pickle_path': 'pickle_path' + tos
        })
        # jd = json.loads(body)
        # pprint.pprint(jd)


@pytest.mark.asyncio
async def test_query_test_data1():
    from ls.db import get_full_index_search_cli
    esc = get_full_index_search_cli()
    fp = os.path.sep.join([os.path.dirname(__file__), '..', 'ls', 'schema.json'])
    jd = json.load(open(fp, 'r'))
    tb = jd['website']['table_name']

    body = await esc.get_docs(tb, from_=0, q='标题', size=300)
    # pprint.pprint(esc.parse_get_docs_hits(body))


@pytest.mark.asyncio
async def test_query_index():
    from ls.db import get_full_index_search_cli
    cli = get_full_index_search_cli()
    body = await cli.get_docs_by_body('website', body={
        "from": 0,
        "size": 20,
        "sort": {
            "date": {
                "order": "desc"
            }
        },
        "query": {
            "multi_match": {
                "query": "女友",
                "fields": ["title", "text"],
            },
        }
    })
    # logging.info(body)
    # logging.info(cli.parse_get_docs_hits(body))