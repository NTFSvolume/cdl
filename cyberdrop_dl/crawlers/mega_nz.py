"""Crawler to download files and folders from mega.nz

This crawler does several CPU intensive operations

It calls checks_complete_by_referer several times even if no request is going to be made, to skip unnecessary compute time
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, NamedTuple

from mega import crypto
from mega.api import MegaAPI
from mega.core import MegaCore
from mega.data_structures import Crypto

from cyberdrop_dl.crawlers.crawler import Crawler, DBPathBuilder, SupportedDomains, SupportedPaths, auto_task_id
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.downloader.mega_nz import MegaDownloader
from cyberdrop_dl.exceptions import LoginError, ScrapeError
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from mega.filesystem import FileSystem

    from cyberdrop_dl.data_structures.url_objects import ScrapeItem

PRIMARY_URL = AbsoluteHttpURL("https://mega.nz")


class FileTuple(NamedTuple):
    id: str
    crypto: Crypto


class MegaNzCrawler(Crawler):
    SUPPORTED_DOMAINS: ClassVar[SupportedDomains] = "mega.io", "mega.nz"
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "File": (
            "/file/<file_id>#<share_key>",
            "/folder/<folder_id>#<share_key>/file/<file_id>",
            "/!#<file_id>!<share_key>",
        ),
        "Folder": (
            "/folder/<folder_id>#<share_key>",
            "/F!#<folder_id>!<share_key>",
        ),
        "Subfolder": "/folder/<folder_id>#<share_key>/folder/<subfolder_id>",
        "**NOTE**": "Downloads can not be resumed. Partial downloads will always be deleted and new downloads will start over",
    }
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = PRIMARY_URL
    SKIP_PRE_CHECK: ClassVar[bool] = True
    DOMAIN: ClassVar[str] = "mega.nz"
    FOLDER_DOMAIN: ClassVar[str] = "MegaNz"
    OLD_DOMAINS = ("mega.co.nz",)
    create_db_path = staticmethod(DBPathBuilder.path_qs_frag)

    core: MegaCore
    downloader: MegaDownloader

    @property
    def user(self) -> str | None:
        return self.manager.auth_config.meganz.email or None

    @property
    def password(self) -> str | None:
        return self.manager.auth_config.meganz.password or None

    def _init_downloader(self) -> MegaDownloader:
        self.core = MegaCore(MegaAPI(self.manager.client_manager._session))
        self.downloader = dl = MegaDownloader(self.manager, self.DOMAIN)  # type: ignore[reportIncompatibleVariableOverride]
        dl.startup()
        return dl

    async def async_startup(self) -> None:
        await self.login(PRIMARY_URL)

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        if not self.logged_in:
            return

        if frag := scrape_item.url.fragment:  # Mega stores access key in fragment. We can't do anything without the key
            # v1 URLs
            if frag.count("!") == 2:
                if frag.startswith("F!"):
                    folder_id, _, shared_key = frag.removeprefix("F!").partition("!")
                    return await self.folder(scrape_item, folder_id, shared_key)
                if frag.startswith("!"):
                    # https://mega.nz/#!Ue5VRSIQ!kC2E4a4JwfWWCWYNJovGFHlbz8F
                    file_id, _, shared_key = frag.removeprefix("!").partition("!")
                    return await self.file(scrape_item, file_id, shared_key)

            # v2 URLs
            match scrape_item.url.parts[1:]:
                # https://mega.nz/folder/oZZxyBrY#oU4jASLPpJVvqGHJIMRcgQ/file/IYZABDGY
                # https://mega.nz/folder/oZZxyBrY#oU4jASLPpJVvqGHJIMRcgQ
                case ["folder", folder_id]:
                    root_id = file_id = None
                    shared_key, *rest = frag.split("/")
                    if rest:
                        match rest:
                            case ["folder", id_]:
                                root_id = id_
                            case ["file", id_]:
                                file_id = id_
                            case _:
                                raise ValueError
                    return await self.folder(scrape_item, folder_id, shared_key, root_id or None, file_id or None)
                # https://mega.nz/file/cH51DYDR#qH7QOfRcM-7N9riZWdSjsRq
                case ["file", file_id]:
                    return await self.file(scrape_item, file_id, frag)

        raise ValueError

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem, file_id: str, shared_key: str) -> None:
        canonical_url = (PRIMARY_URL / "file" / file_id).with_fragment(shared_key)
        if await self.check_complete_from_referer(canonical_url):
            return

        scrape_item.url = canonical_url
        full_key = crypto.b64_to_a32(shared_key)
        file = FileTuple(file_id, Crypto.decompose(full_key))
        await self._process_file(scrape_item, file)

    @error_handling_wrapper
    async def _process_file(self, scrape_item: ScrapeItem, file: FileTuple, *, folder_id: str | None = None) -> None:
        file_data = await self.core.request_file_info(file.id, folder_id, is_public=not folder_id)
        if not file_data.url:
            raise ScrapeError(410, "File not accessible anymore")

        name = self.core.decrypt_attrs(file_data._at, file.crypto.key).name
        self.downloader.register(scrape_item.url, file.crypto, file_data.size)
        file_url = self.parse_url(file_data.url)
        filename, ext = self.get_filename_and_ext(name)
        await self.handle_file(scrape_item.url, scrape_item, filename, ext, debrid_link=file_url)

    _process_file_task = auto_task_id(_process_file)

    @error_handling_wrapper
    async def folder(
        self,
        scrape_item: ScrapeItem,
        folder_id: str,
        shared_key: str,
        root_id: str | None = None,
        single_file_id: str | None = None,
    ) -> None:
        if single_file_id and await self.check_complete_from_referer(scrape_item.url):
            return

        selected_node = root_id or single_file_id
        fs = await self.core.get_public_filesystem(folder_id, shared_key)
        root = next(iter(fs))
        title = self.create_title(root.name, folder_id)
        scrape_item.setup_as_album(title, album_id=folder_id)
        canonical_url = (PRIMARY_URL / "folder" / folder_id).with_fragment(shared_key)
        scrape_item.url = canonical_url
        await self._process_folder_filesystem(scrape_item, fs, selected_node)

    async def _process_folder_filesystem(
        self,
        scrape_item: ScrapeItem,
        filesystem: FileSystem,
        selected_node: str | None,
    ) -> None:
        folder_id, shared_key = scrape_item.url.name, scrape_item.url.fragment

        for file in filesystem.files_from(selected_node):
            path = filesystem.relative_path(file.id)
            file_fragment = f"{shared_key}/file/{file.id}"
            canonical_url = scrape_item.url.with_fragment(file_fragment)
            if await self.check_complete_from_referer(canonical_url):
                continue

            new_scrape_item = scrape_item.create_child(canonical_url, possible_datetime=file.created_at)
            for part in path.parent.parts[1:]:
                new_scrape_item.add_to_parent_title(part)

            file = FileTuple(file.id, file._crypto)
            self.create_task(self._process_file_task(new_scrape_item, file, folder_id=folder_id))
            scrape_item.add_children()

    @error_handling_wrapper
    async def login(self, *_) -> None:
        # This takes a really long time (dozens of seconds)
        # TODO: Add a way to cache this login
        # TODO: Show some logging message / UI about login
        try:
            await self.core.login(self.user, self.password)
            self.logged_in = True
        except Exception as e:
            self.disabled = True
            raise LoginError(f"[MegaNZ] {e}") from e
