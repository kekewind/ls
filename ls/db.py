import os.path
import asyncio

from .common import LocalFileStorage, ElasticSearchCli
from .config import ELASTIC_HOST, ELASTIC_PORT, LOCAL_FILE_STORAGE_ROOT
import click, json


def get_db():
    pass


def close_db(db):
    pass


def get_file_storage(type=None):
    return LocalFileStorage(LOCAL_FILE_STORAGE_ROOT)


def close_file_storage(type=None):
    pass


def get_full_index_search_cli(type=None):
    return ElasticSearchCli(ELASTIC_HOST, ELASTIC_PORT)


def close_full_index_search_cli(full_index_cli):
    full_index_cli.close()


async def delete_full_index(type=None, test=False):
    fp = os.path.sep.join([os.path.dirname(__file__), 'schema.json'])
    jd = json.load(open(fp, 'r'))
    eci = get_full_index_search_cli()
    for _, v in jd.items():
        table_name = v['table_name'] + '_test' if test else v['table_name']
        await eci.delete_table(table_name)


async def init_full_index(type=None, test=False):
    fp = os.path.sep.join([os.path.dirname(__file__), 'schema.json'])
    jd = json.load(open(fp, 'r'))
    eci = get_full_index_search_cli()
    for _, v in jd.items():
        table_name = v['table_name'] + '_test' if test else v['table_name']
        attrs = v['attrs']
        await eci.create_table(table_name, attrs)


async def init_full_index_command(type=None, test=None):
    await delete_full_index(type, test)
    await init_full_index(type, test)


@click.command()
@click.option('--command', help='value: \t* init_full_index, create tables for the full index.\n')
def main(command):
    if command == 'init_full_index':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(init_full_index_command())
        loop.close()
    click.echo(command)


if __name__ == '__main__':
    main()