"""下载层 —— 并发抓取所有源，全程在内存里完成。

不落临时文件。最大的源（几十万条 CIDR）也只有几 MB 文本，放内存毫无压力，
而省掉临时目录之后，"文件名冲突"、"断点续传把两个源首尾相接"、"清理阶段把自己
的缓存目录删掉"这一整类问题就不存在了。

URL 会先去重，同一个地址被多个规则集引用时只抓一次。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import httpx

from .errors import SrsBoxError

USER_AGENT = "srs-box/0.2 (+https://github.com/Sentsuki/srs-box)"

# 单个源的体积上限，防止误配一个巨大地址把内存吃光
MAX_BYTES = 64 * 1024 * 1024

# 这些状态码重试没有意义，直接判失败
_NO_RETRY = frozenset({400, 401, 403, 404, 405, 410, 451})


@dataclass(frozen=True)
class Fetched:
    url: str
    text: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.text is not None


def _decode(response: httpx.Response) -> str:
    """优先按 UTF-8 解码；失败才退回 httpx 依据响应头的猜测。"""
    try:
        return response.content.decode("utf-8")
    except UnicodeDecodeError:
        return response.text


async def _fetch_one(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    retries: int,
) -> Fetched:
    last = "未知错误"
    async with semaphore:
        for attempt in range(retries + 1):
            try:
                response = await client.get(url)
                if response.status_code in _NO_RETRY:
                    return Fetched(url, error=f"HTTP {response.status_code}")
                response.raise_for_status()
                if len(response.content) > MAX_BYTES:
                    return Fetched(
                        url, error=f"响应过大: {len(response.content) / 1e6:.1f} MB"
                    )
                if not response.content.strip():
                    return Fetched(url, error="响应为空")
                return Fetched(url, text=_decode(response))
            except httpx.HTTPStatusError as exc:
                last = f"HTTP {exc.response.status_code}"
            except httpx.HTTPError as exc:
                last = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                await asyncio.sleep(2**attempt)
    return Fetched(url, error=last)


async def _fetch_all(
    urls: Sequence[str],
    concurrency: int,
    timeout: float,
    retries: int,
    on_done: Callable[[Fetched, int, int], None] | None,
) -> dict[str, Fetched]:
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(
        max_connections=concurrency, max_keepalive_connections=concurrency
    )
    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        follow_redirects=True,
        http2=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        tasks = [
            asyncio.create_task(_fetch_one(client, url, semaphore, retries))
            for url in urls
        ]
        results: dict[str, Fetched] = {}
        for index, task in enumerate(asyncio.as_completed(tasks), 1):
            result = await task
            results[result.url] = result
            if on_done:
                on_done(result, index, len(tasks))
    return results


def fetch_all(
    urls: Iterable[str],
    *,
    concurrency: int = 8,
    timeout: float = 30.0,
    retries: int = 3,
    on_done: Callable[[Fetched, int, int], None] | None = None,
) -> dict[str, Fetched]:
    """并发抓取，返回 ``url -> Fetched``。失败不抛异常，记在 ``Fetched.error`` 里。"""
    unique = list(dict.fromkeys(urls))
    if not unique:
        return {}
    try:
        return asyncio.run(_fetch_all(unique, concurrency, timeout, retries, on_done))
    except RuntimeError as exc:  # pragma: no cover - 嵌套事件循环
        raise SrsBoxError(f"下载阶段无法启动事件循环: {exc}") from exc
