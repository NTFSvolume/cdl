from cyberdrop_dl import __version__, yaml
from cyberdrop_dl.managers.manager import Manager


def test_cache_file_is_not_saved_outside_ctx(manager: Manager) -> None:
    cache_file = manager.appdata.cache_file
    manager.cache["test"] = 1

    assert manager.cache == {"test": 1}
    assert not cache_file.exists()


async def test_cache_file_is_saved_in_ctx(manager: Manager) -> None:
    cache_file = manager.appdata.cache_file
    async with manager:
        manager.cache["test"] = 1
        assert manager.cache == {"test": 1}
        assert cache_file.is_file()

    assert cache_file.is_file()
    assert yaml.load(cache_file) == {"test": 1, "version": __version__}
