from cyberdrop_dl.supported_sites import get_crawlers_info_as_rich_table


def test_rich_table() -> None:
    table = get_crawlers_info_as_rich_table()
    assert len(table.rows) >= 169
