import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path

from aiohttp import web

from cyberdrop_dl.utils.utilities import parse_url

router = web.RouteTableDef()


def json_error_wrapper(
    func: Callable[[web.Request], Awaitable[web.Response]],
) -> Callable[[web.Request], Awaitable[web.Response]]:
    async def handler(request: web.Request) -> web.Response:
        try:
            return await func(request)
        except asyncio.CancelledError:
            raise
        except json.JSONDecodeError:
            return web.json_response({"status": "failed", "reason": "Invalid payload"}, status=400)
        except Exception as ex:
            return web.json_response({"status": "failed", "reason": repr(ex)}, status=400)

    return handler


@router.get("/")
async def root(request: web.Request) -> web.Response:
    return web.Response(text="Cyberdrop DL API")


@router.post("/add_urls")
@json_error_wrapper
async def add_urls(request: web.Request) -> web.Response:
    post = await request.json()
    urls: list[str] = post["urls"]
    print(urls)  # noqa: T201
    if not urls:
        raise ValueError("No urls to add")
    for url in urls:
        try:
            parse_url(url)
        except Exception:
            raise ValueError(f"Invalid {url = }") from None

    name = str(datetime.now()).replace(":", "_")
    Path(name).write_text("\n".join(urls))
    return web.json_response({"status": "ok"})


async def run() -> None:
    app = web.Application()
    app.add_routes(router)
    runner = web.AppRunner(app, handle_signals=False)
    await runner.setup()

    try:
        site = web.TCPSite(runner)
        await site.start()
        print(f"Running server on {site.name}")  # noqa: T201

    except (web.GracefulExit, KeyboardInterrupt):
        pass

    finally:
        await runner.cleanup()


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
