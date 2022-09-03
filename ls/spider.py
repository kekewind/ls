"""
用几个最常见的情况，直接举例说明：

1. 允许所有SE收录本站：

robots.txt为空就可以，什么都不要写。

2. 禁止所有SE收录网站的某些目录：

User-agent: *

Disallow: /目录名1/

Disallow: /目录名2/

Disallow: /目录名3/

3. 禁止某个SE收录本站，例如禁止百度：

User-agent: Baiduspider

Disallow: /

4. 禁止所有SE收录本站：

User-agent: *

Disallow: /
"""
import asyncio
import datetime
import hashlib
import json
import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import charset_normalizer
import click
import pika
import requests
import urllib3
from lxml import etree
from requests_html import HTMLSession

from ls import config
from ls.db import get_full_index_search_cli, get_file_storage

urllib3.disable_warnings()


class RedirectTooManyException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class AllNoneException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class WrongUrlException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class GoEndException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class UrlDuplExceotion(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RabbitMQCli(object):
    def __init__(self, host, port, uname, passwd, task_queue, store_queue):
        self._host = host
        self._port = port
        self._uname = uname
        self._passwd = passwd
        self._task_queue = task_queue
        self._store_queue = store_queue
        self._conn = None
        self._ch = None

    def close(self):
        if self._conn: self._conn.close()

    def _connect(self):
        if not self._conn:
            credentials = pika.PlainCredentials(self._uname, self._passwd)
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host=self._host,
                                                                           port=self._port,
                                                                           credentials=credentials))
        return self._conn

    def callback(self, ch, method, properties, body):
        # ch.basic_publish(exchange='', routing_key=store_queue, body=message)
        # ch.basic_ack(delivery_tag=method.delivery_tag)
        raise NotImplementedError()

    def loop_task(self):
        conn = self._connect()
        channel = conn.channel()
        channel.queue_declare(queue=self._task_queue)
        channel.queue_declare(queue=self._store_queue)

        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(queue=self._task_queue, on_message_callback=self.callback)
        channel.start_consuming()

    def send_task(self, body):
        conn = self._connect()
        if not (self._ch and self._ch.is_open):
            self._ch = conn.channel()

        self._ch.basic_publish(exchange='', routing_key=self._task_queue, body=json.dumps(body))


class BaseWebsite(RabbitMQCli):
    def __init__(self, host, port, uname, passwd, task_queue, store_queue):
        super(BaseWebsite, self).__init__(host, port, uname, passwd, task_queue, store_queue)

        self._headers = {
            'User-Agent': 'kuaiso-webspider'
        }
        self._rfp = RobotFileParser()
        self._robots_dupl = set()
        self._url_dupl = set()

    def _parse_robot(self, url):
        if not hasattr(BaseWebsite, '_redirect_num'):
            self._redirect_num = 1

        if self._redirect_num > 2:
            raise RedirectTooManyException()

        if url not in self._robots_dupl:
            r = requests.get(url, headers=self._headers, verify=False, timeout=10, allow_redirects=False)
            r.encoding = r.apparent_encoding if r.apparent_encoding else 'utf8'
            if r.url == url:
                self._rfp.parse(r.text)
                self._robots_dupl.add(url)
            else:
                base_url = self._parse_base_uri(r.url)
                self._redirect_num = self._redirect_num + 1
                self._parse_robot(base_url + '/robots.txt')

            r.close()

    def _parse_base_uri(self, url):
        pr = urlparse(url)
        if pr.scheme and pr.netloc:
            return pr.scheme + '://' + pr.netloc
        else:
            raise WrongUrlException()

    def climb(self, url):
        self._parse_robot(self._parse_base_uri(url) + '/robots.txt')

        if url and self._rfp.can_fetch(self._headers['User-Agent'], url):
            return self.fetch_and_parse(url)
        raise GoEndException('go end url: %s' % url)

    def callback(self, ch, method, properties, body):
        try:
            url = json.loads(body)
            if url in self._url_dupl:
                raise UrlDuplExceotion()

            title, html, text, url, links = self.climb(url)
            logging.info(title + ' ** ' + url)
            ch.basic_publish(exchange='', routing_key=self._store_queue,
                             body=json.dumps([title, html, text, url]))

            for link in links:
                self.send_task(link)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except GoEndException as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except UrlDuplExceotion as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except WrongUrlException as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except RedirectTooManyException as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except requests.exceptions.InvalidURL as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except Exception as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))

    def fetch_and_parse(self, url):
        raise NotImplementedError()


class DGStaticWebsite(BaseWebsite):
    def fetch_and_parse(self, url):
        r = requests.get(url, headers=self._headers, verify=False, timeout=10, allow_redirects=False)
        r.encoding = charset_normalizer.detect(r.content)['encoding']
        dom = etree.HTML(r.text)
        title_texts = '-'.join(dom.xpath('//title/text()'))
        title = title_texts
        html = r.text
        # text = ''.join(dom.xpath('string(.)')).strip().replace('\n', '').replace('\t', '').replace(' ', '')
        text = ''.join(dom.xpath('//meta[@name="description"]/@content'))
        links = set()
        for href in dom.xpath('//a/@href'):
            pr = urlparse(href)
            if not pr.netloc:
                pr = urlparse(url)
                href = pr.scheme + '://' + pr.netloc + href
            links.add(href)

        return title, html, text, url, links


class DGdyWebsite(BaseWebsite):
    def fetch_and_parse(self, url):
        session = HTMLSession()
        r = session.get(url, headers=self._headers, timeout=10, verify=False, allow_redirects=False)
        r.encoding = r.apparent_encoding if r.apparent_encoding else 'utf8'
        r.html.render()
        r.html.encoding = charset_normalizer.detect(r.html.raw_html)['encoding']

        title_dom = r.html.find('title', first=True)
        title = title_dom.text if title_dom else None
        # text = ''.join(r.html.xpath('string(.)')).strip().replace('\n', '').replace('\t', '').replace(' ', '')
        text = ''.join(r.html.xpath('//meta[@name="description"]/@content'))
        links = r.html.absolute_links
        html = r.html.html
        session.close()

        return title, html, text, url, links


class StoreWebsite(RabbitMQCli):
    def _get_full_index(self):
        if not hasattr(StoreWebsite, '_full_index'):
            self._full_index = get_full_index_search_cli()
        return self._full_index

    def _get_event_loop(self):
        if not hasattr(StoreWebsite, '_event_loop'):
            self._event_loop = asyncio.get_event_loop()
        return self._event_loop

    def _get_file_storage(self):
        if not hasattr(StoreWebsite, '_file_storage'):
            self._file_storage = get_file_storage()
        return self._file_storage

    def callback(self, ch, method, properties, body):
        try:
            title, html, text, url = json.loads(body)
            # uid = hashlib.md5(html.encode()).hexdigest()

            uid = None
            if len(text.strip()) > 0:
                uid = hashlib.md5(text.encode()).hexdigest()
            elif len(title.strip()) > 0:
                uid = hashlib.md5(title.encode()).hexdigest()
            elif len(html) > 0:
                uid = hashlib.md5(html.encode()).hexdigest()
            else:
                raise AllNoneException()

            logging.info(title + ' ** ' + url)

            self._get_event_loop().run_until_complete(self._get_full_index().add_doc('website', uid, {
                'url': url,
                'title': title,
                'text': text,
                'html': uid,
                'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }))

            self._get_file_storage().post(uid, html)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except AllNoneException as e:
            logging.error('%s * %s * %s', title, url, str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error('%s * %s * %s', title, url, str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)


def static_task_queue(host, port, uname, passwd, static_task_queue_name, static_store_queue_name):
    dg = DGStaticWebsite(host, port, uname, passwd, static_task_queue_name, static_store_queue_name)
    dg.loop_task()


def dyn_task_queue(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name):
    dg = DGdyWebsite(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name)
    dg.loop_task()


def publish_url(host, port, uname, passwd,
                static_task_queue_name, static_store_queue_name,
                dyn_task_queue_name, dyn_store_queue_name,
                static_url, dyn_url):
    st = DGStaticWebsite(host, port, uname, passwd, static_task_queue_name, static_store_queue_name)
    dy = DGdyWebsite(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name)
    if static_url:
        st.send_task(static_url)
    if dyn_url:
        dy.send_task(dyn_url)

    st.close()
    dy.close()


def static_store_queue(host, port, uname, passwd, static_task_queue_name, static_store_queue_name):
    dg = StoreWebsite(host, port, uname, passwd, static_store_queue_name, static_task_queue_name)
    dg.loop_task()


def dyn_store_queue(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name):
    dg = StoreWebsite(host, port, uname, passwd, dyn_store_queue_name, dyn_task_queue_name)
    dg.loop_task()


@click.command()
@click.option('--command', help='dyn_store_queue, static_store_queue, publish_url, dyn_task_queue, static_task_queue')
@click.option('--host', help='rabbitmq search host', default='localhost')
@click.option('--port', help='rabbitmq search port', default=5672)
@click.option('--uname', help='rabbitmq uname', default='guest')
@click.option('--passwd', help='rabbitmq passwd', default='guest')
@click.option('--static_task_queue_name', help='the task queue name that is the spider for static website',
              default='static_task_queue')
@click.option('--static_store_queue_name', help='the task queue name that is to store static website data',
              default='static_store_queue')
@click.option('--dyn_store_queue_name', help='the task queue name that is to store dyn website data',
              default='dyn_task_queue')
@click.option('--dyn_task_queue_name', help='the task queue name that is the spider for dyn website',
              default='dyn_store_queue')
@click.option('--static_url', help='url you want to use static webspider to crawl')
@click.option('--dyn_url', help='url you want to use dyn webspider to crawl')
@click.option('--LOCAL_FILE_STORAGE_ROOT', help='path of the page source you want to save',
              default=config.LOCAL_FILE_STORAGE_ROOT)
def main(command, host, port, uname, passwd,
         static_task_queue_name, static_store_queue_name,
         dyn_store_queue_name, dyn_task_queue_name,
         static_url, dyn_url, local_file_storage_root):
    logging.basicConfig(level=logging.INFO)
    config.LOCAL_FILE_STORAGE_ROOT = local_file_storage_root

    if command == 'static_task_queue':
        static_task_queue(host, port, uname, passwd, static_task_queue_name, static_store_queue_name)
    elif command == 'static_store_queue':
        static_store_queue(host, port, uname, passwd, static_task_queue_name, static_store_queue_name)
    elif command == 'dyn_store_queue':
        dyn_store_queue(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name)
    elif command == 'dyn_task_queue':
        dyn_task_queue(host, port, uname, passwd, dyn_task_queue_name, dyn_store_queue_name)
    elif command == 'publish_url':
        publish_url(host, port, uname, passwd,
                    static_task_queue_name, static_store_queue_name,
                    dyn_task_queue_name, dyn_store_queue_name,
                    static_url, dyn_url)
    else:
        logging.info('command args maybe invalid.')
    click.echo(command)
