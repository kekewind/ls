import asyncio
import os

import tornado.locks
import tornado.web
from tornado.options import define, options, parse_command_line

from ls import config
from ls.common import AESCBC
from ls.db import close_db, get_db, get_full_index_search_cli, close_full_index_search_cli

define("port", default=config.WEB_PORT, help="run on the given port", type=int)
define("ELASTIC_HOST", default=config.ELASTIC_HOST, help="elastic search host", type=str)
define("ELASTIC_PORT", default=config.ELASTIC_PORT, help="elastic search port", type=int)
define("LOCAL_FILE_STORAGE_ROOT", default=config.LOCAL_FILE_STORAGE_ROOT, help="file storage root path", type=str)
define("KEY", default=config.KEY, help="to encrypt the url", type=str)


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.db = get_db()
        self.full_index_search = get_full_index_search_cli()

    def on_finish(self):
        close_db(self.db)
        close_full_index_search_cli(self.full_index_search)


class SearchHandler(BaseHandler):
    async def get(self):
        self.render('index.html', null=False, title=None)

    async def post(self):
        keyword = self.get_argument('keyword', None)
        page = int(self.get_argument('page', 1))
        from_ = 0
        if page > 0:
            from_ = (page - 1) * 20

        body = await self.full_index_search.get_docs_by_body('website', body={
            "size": 20,
            "sort": {
                "date": {
                    "order": "desc"
                },
                "click_num": {
                    "order": "desc"
                },
                "stay_time": {
                    "order": "desc"
                }
            },
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": [
                        "title^2",
                        "text^1"
                    ],
                    "minimum_should_match": "100%"
                }
            },
            "from": from_
        })
        rows, total = self.full_index_search.parse_get_docs_hits(body)
        len_rows = len(rows)
        aescbc = AESCBC(config.KEY)
        if len_rows > 0:
            for row in rows:
                row['url'] = '/go?eu=' + aescbc.encrypt(row['url'])
            self.render('index_results.html', title=keyword,
                        last_page=(page - 1) if page > 1 else None, page=page,
                        next_page=(page + 1) if from_ + 20 < total else None, rows=rows)
        else:
            self.render('index.html', null=True, title=keyword)


class URLGOHandler(BaseHandler):
    def get(self):
        eurl = self.get_argument('eu')
        aescbc = AESCBC(config.KEY)
        url = aescbc.decrypt(eurl.encode())

        self.redirect(url)


def make_app():
    settings = dict(
        debug=True,
        # xsrf_cookies=True,
        static_path=os.path.join(os.path.dirname(__file__), "static"),
        template_path=os.path.join(os.path.dirname(__file__), "templates"),
    )
    return tornado.web.Application([
        (r"/", SearchHandler),
        (r'/go', URLGOHandler),
    ], **settings)


async def start():
    parse_command_line()
    config.LOCAL_FILE_STORAGE_ROOT = options.LOCAL_FILE_STORAGE_ROOT
    config.WEB_PORT = options.port
    config.ELASTIC_PORT = options.ELASTIC_PORT
    config.ELASTIC_HOST = options.ELASTIC_HOST
    config.KEY = options.KEY
    app = make_app()
    app.listen(options.port)
    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()


def main():
    asyncio.run(start())


if __name__ == "__main__":
    main()