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

import aioredis
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


class HTTPResponseContentZeroException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NoEtreeHTMLDomReqRedirectException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


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


class RedisDuplicateUrlFilter(object):
    def __init__(self, redis_host, redis_port, redis_passwd, redis_dup_key):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_passwd = redis_passwd
        self.redis_dup_key = redis_dup_key

    def __del__(self):
        if hasattr(self, '_redis_conn'):
            if self._redis_conn:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self._redis_conn.close())

    def _get_conn(self):
        if not hasattr(self, '_redis_conn'):
            self._redis_conn = aioredis.from_url('redis://:%s@%s:%d' % (self.redis_passwd, self.redis_host, self.redis_port))
        return self._redis_conn

    async def find(self, url):
        u = hashlib.md5(url.encode()).hexdigest()
        if await self._get_conn().sismember(self.redis_dup_key, u):
            return True
        await self._get_conn().sadd(self.redis_dup_key, u)
        return False


class RabbitMQCli(object):
    def __init__(self, mq_host, mq_port, mq_uname, mq_passwd, mq_task_queue, mq_store_queue):
        self._mq_host = mq_host
        self._mq_port = mq_port
        self._mq_uname = mq_uname
        self._mq_passwd = mq_passwd
        self._mq_task_queue = mq_task_queue
        self._mq_store_queue = mq_store_queue
        self._mq_conn = None
        self._mq_ch = None

    def close(self):
        if self._mq_conn: self._mq_conn.close()

    def _connect(self):
        if not self._mq_conn:
            credentials = pika.PlainCredentials(self._mq_uname, self._mq_passwd)
            self._mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_host,
                                                                           port=self._mq_port,
                                                                           credentials=credentials))
        return self._mq_conn

    def callback(self, ch, method, properties, body):
        # ch.basic_publish(exchange='', routing_key=store_queue, body=message)
        # ch.basic_ack(delivery_tag=method.delivery_tag)
        raise NotImplementedError()

    def loop_task(self):
        conn = self._connect()
        channel = conn.channel()
        channel.queue_declare(queue=self._mq_task_queue)
        channel.queue_declare(queue=self._mq_store_queue)

        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(queue=self._mq_task_queue, on_message_callback=self.callback)
        channel.start_consuming()

    def send_task(self, body):
        conn = self._connect()
        if not (self._mq_ch and self._mq_ch.is_open):
            self._mq_ch = conn.channel()

        self._mq_ch.basic_publish(exchange='', routing_key=self._mq_task_queue, body=json.dumps(body))


class BaseWebsite(RabbitMQCli, RedisDuplicateUrlFilter):
    def __init__(self, mq_host, mq_port, mq_uname, mq_passwd, task_queue, store_queue,
                 redis_host, redis_port, redis_pass, dup_filter_key):
        RabbitMQCli.__init__(self, mq_host, mq_port, mq_uname, mq_passwd, task_queue, store_queue)
        RedisDuplicateUrlFilter.__init__(self, redis_host, redis_port, redis_pass, dup_filter_key)

        self._headers = {
            'User-Agent': 'kuaiso-webspider'
        }
        self._rfp = RobotFileParser()

    def _parse_robot(self, url):
        if not hasattr(BaseWebsite, '_redirect_num'):
            self._redirect_num = 1

        if self._redirect_num > 2:
            raise RedirectTooManyException('redirect too many err')

        r = requests.get(url, headers=self._headers, verify=False, timeout=10, allow_redirects=False)
        r.encoding = r.apparent_encoding if r.apparent_encoding else 'utf8'
        if r.url == url:
            self._rfp.parse(r.text.splitlines())
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
            raise WrongUrlException('wrong url err')

    def climb(self, url):
        self._parse_robot(self._parse_base_uri(url) + '/robots.txt')

        if url and self._rfp.can_fetch(self._headers['User-Agent'], url):
            return self.fetch_and_parse(url)
        raise GoEndException('go end url: %s err' % url)

    def callback(self, ch, method, properties, body):
        try:
            url = json.loads(body)
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.find(url))
            loop.run_until_complete(future)
            if future.result():
                raise UrlDuplExceotion('url dup err')

            title, html, text, url, links = self.climb(url)
            logging.info(title + ' ** ' + url)
            ch.basic_publish(exchange='', routing_key=self._mq_store_queue,
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
        except NoEtreeHTMLDomReqRedirectException as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except HTTPResponseContentZeroException as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
        except Exception as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.error('%s * %s', url, str(e))
            import traceback
            logging.error('%s', traceback.format_exc())

    def fetch_and_parse(self, url):
        raise NotImplementedError()


class DGStaticWebsite(BaseWebsite):
    def fetch_and_parse(self, url):
        session = requests.session()
        session.max_redirects = 1
        try:
            r = session.get(url, headers=self._headers, verify=False, timeout=10)
            # if r.status_code in (301, 302):
            #     raise NoEtreeHTMLDomReqRedirectException('dom is none err, http status 301 or 302')
            if len(r.content) == 0 and r.status_code not in (301, 302):
                raise HTTPResponseContentZeroException('http response content 0 err')

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
        except Exception as e:
            raise e
        finally:
            session.close()


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
                raise AllNoneException('title text html all none err')

            logging.info(title + ' ** ' + url)
            future = asyncio.ensure_future(self._get_full_index().get_doc_by_id('website', uid))
            self._get_event_loop().run_until_complete(future)
            body = future.result()
            doc = self._get_full_index().parse_doc(body)
            if not doc:
                self._get_event_loop().run_until_complete(self._get_full_index().add_doc('website', uid, {
                    'url': url,
                    'title': title,
                    'text': text,
                    'html': uid,
                    "webpage_uid": hashlib.md5(html.encode()).hexdigest(),
                    'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'click_num': 0,
                    'stay_time': 0
                }))
                if config.SAVE_WEB_PAGE:
                    new_webpage_uid = hashlib.md5(html.encode()).hexdigest()
                    self._get_file_storage().post(new_webpage_uid, html)
            else:
                if config.SAVE_WEB_PAGE:
                    webpage_uid = None
                    old_webpage_uid = doc['webpage_uid']
                    new_webpage_uid = hashlib.md5(html.encode()).hexdigest()
                    # 如果网页指纹和上一次不同，则 代表网页内容发生变化
                    if old_webpage_uid != new_webpage_uid:
                        # 删除旧的网页缓存，增加新的
                        self._get_file_storage().del_file(old_webpage_uid)
                        self._get_file_storage().post(new_webpage_uid, html)
                        webpage_uid = new_webpage_uid

                    self._get_event_loop().run_until_complete(self._get_full_index().edit_doc('website', uid, {
                        'url': url,
                        'title': title,
                        'text': text,
                        'html': webpage_uid,
                        "webpage_uid": new_webpage_uid,
                        'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }))

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except AllNoneException as e:
            logging.error('%s * %s * %s', title, url, str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error('%s * %s * %s', title, url, str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)


def static_task_queue(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name,
                      redis_host, redis_port, redis_pass, dup_filter_key):
    dg = DGStaticWebsite(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name,
                         redis_host, redis_port, redis_pass, dup_filter_key)
    dg.loop_task()


def dyn_task_queue(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name, redis_host,
                   redis_port, redis_pass, dup_filter_key):
    dg = DGdyWebsite(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name, redis_host,
                     redis_port, redis_pass, dup_filter_key)
    dg.loop_task()


def publish_url(mq_host, mq_port, mq_uname, mq_passwd,
                static_task_queue_name, static_store_queue_name,
                dyn_task_queue_name, dyn_store_queue_name,
                static_url, dyn_url, redis_host, redis_port, redis_pass, dup_filter_key):
    st = DGStaticWebsite(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name,
                         redis_host, redis_port, redis_pass, dup_filter_key)
    dy = DGdyWebsite(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name, redis_host,
                     redis_port, redis_pass, dup_filter_key)
    if static_url:
        st.send_task(static_url)
    if dyn_url:
        dy.send_task(dyn_url)

    st.close()
    dy.close()


def static_store_queue(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name):
    dg = StoreWebsite(mq_host, mq_port, mq_uname, mq_passwd, static_store_queue_name, static_task_queue_name)
    dg.loop_task()


def dyn_store_queue(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name):
    dg = StoreWebsite(mq_host, mq_port, mq_uname, mq_passwd, dyn_store_queue_name, dyn_task_queue_name)
    dg.loop_task()


@click.command()
@click.option('--command', help='dyn_store_queue, static_store_queue, publish_url, dyn_task_queue, static_task_queue')
@click.option('--mq_host', help='rabbitmq search host', default='localhost')
@click.option('--mq_port', help='rabbitmq search port', default=5672)
@click.option('--mq_uname', help='rabbitmq uname', default='guest')
@click.option('--mq_passwd', help='rabbitmq passwd', default='guest')
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
@click.option('--SAVE_WEB_PAG', help='save web page to the path', default=config.SAVE_WEB_PAGE, type=bool)
@click.option('--redis_host', help='redis host', default='localhost')
@click.option('--redis_port', help='redis port', default=6379)
@click.option('--redis_pass', help='redis passwd', default='123456')
@click.option('--dup_filter_key', help='redis dup_filter_key', default='website_dup')
def main(command, mq_host, mq_port, mq_uname, mq_passwd,
         static_task_queue_name, static_store_queue_name,
         dyn_store_queue_name, dyn_task_queue_name,
         static_url, dyn_url, local_file_storage_root, save_web_pag,
         redis_host, redis_port, redis_pass, dup_filter_key):
    logging.basicConfig(level=logging.INFO)
    config.LOCAL_FILE_STORAGE_ROOT = local_file_storage_root
    config.SAVE_WEB_PAGE = save_web_pag

    if command == 'static_task_queue':
        static_task_queue(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name,
                          redis_host, redis_port, redis_pass, dup_filter_key)
    elif command == 'static_store_queue':
        static_store_queue(mq_host, mq_port, mq_uname, mq_passwd, static_task_queue_name, static_store_queue_name)
    elif command == 'dyn_store_queue':
        dyn_store_queue(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name)
    elif command == 'dyn_task_queue':
        dyn_task_queue(mq_host, mq_port, mq_uname, mq_passwd, dyn_task_queue_name, dyn_store_queue_name, redis_host,
                       redis_port, redis_pass, dup_filter_key)
    elif command == 'publish_url':
        publish_url(mq_host, mq_port, mq_uname, mq_passwd,
                    static_task_queue_name, static_store_queue_name,
                    dyn_task_queue_name, dyn_store_queue_name,
                    static_url, dyn_url, redis_host, redis_port, redis_pass, dup_filter_key)
    else:
        logging.info('command args maybe invalid.')
    click.echo(command)
