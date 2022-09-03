import pytest
import asyncio
from ls.db import get_full_index_search_cli, init_full_index_command, delete_full_index


@pytest.fixture()
def esc():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_full_index_command(test=True))

    cli = get_full_index_search_cli()
    yield cli

    loop = asyncio.get_event_loop()
    loop.run_until_complete(delete_full_index(test=True))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(cli.delete_table('test1'))
    loop.close()
    cli.close()