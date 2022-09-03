import pytest

from ls.spider import DGStaticWebsite, DGdyWebsite, StoreWebsite
from requests_html import HTMLSession
from datadefines import dyn_urls, static_urls


@pytest.mark.timeout(30)
def test_requests_html():
    session = HTMLSession()
    r = session.get('http://localhost:5000')
    r.html.render()
    print(r.html.html)
    print(r.html.find('title', first=True).text)
    session.close()


# @pytest.mark.timeout(30)
def test_static():
    host = 'localhost'
    port = 5672
    uname = 'guest'
    passwd = 'guest'
    task_queue = 'static_task_queue'
    store_queue = 'static_store_queue'
    dg = DGStaticWebsite(host, port, uname, passwd, task_queue, store_queue)
    dg.loop_task()


def test_dyn():
    host = 'localhost'
    port = 5672
    uname = 'guest'
    passwd = 'guest'
    task_queue = 'dyn_task_queue'
    store_queue = 'dyn_store_queue'
    dg = DGdyWebsite(host, port, uname, passwd, task_queue, store_queue)
    dg.loop_task()


def test_publish_url():
    host = 'localhost'
    port = 5672
    uname = 'guest'
    passwd = 'guest'
    task_queue = 'task_queue'
    store_queue = 'store_queue'
    st = DGStaticWebsite(host, port, uname, passwd, 'static_' + task_queue, 'static_' + store_queue)
    dy = DGdyWebsite(host, port, uname, passwd, 'dyn_' + task_queue, 'dyn_' + store_queue)
    for url in static_urls:
        st.send_task(url)
    for url in dyn_urls:
        dy.send_task(url)

    st.close()
    dy.close()


# @pytest.mark.timeout(30)
def test_static_store():
    host = 'localhost'
    port = 5672
    uname = 'guest'
    passwd = 'guest'
    task_queue = 'task_queue'
    store_queue = 'store_queue'
    dg = StoreWebsite(host, port, uname, passwd, 'static_' + store_queue, 'static_' + task_queue)
    dg.loop_task()


# @pytest.mark.timeout(30)
def test_dyn_store():
    host = 'localhost'
    port = 5672
    uname = 'guest'
    passwd = 'guest'
    task_queue = 'task_queue'
    store_queue = 'store_queue'
    dg = StoreWebsite(host, port, uname, passwd, 'dyn_' + store_queue, 'dyn_' + task_queue)
    dg.loop_task()
