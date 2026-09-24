import asyncio

import httpx
import pytest

from srsbox import fetch
from srsbox.fetch import MAX_BYTES, _read_capped, fetch_all

# 打桩前先抓住真的 sleep，否则替身调用 asyncio.sleep 就是在调用自己
_REAL_SLEEP = asyncio.sleep


async def _no_sleep(seconds):
    await _REAL_SLEEP(0)


async def _tiny_sleep(seconds):
    """退避缩短到可忽略，但仍然真的让出事件循环。"""
    await _REAL_SLEEP(0.01)


class _Stream:
    """只实现 aiter_bytes 的假响应，用来直接检验收流上限。"""

    def __init__(self, chunk: bytes, count: int) -> None:
        self.chunk = chunk
        self.count = count
        self.delivered = 0

    async def aiter_bytes(self, size=None):
        for _ in range(self.count):
            self.delivered += 1
            yield self.chunk


def transport(handler):
    return httpx.MockTransport(handler)


def patch_client(monkeypatch, handler):
    """把 AsyncClient 换成挂了 MockTransport 的同款客户端。"""
    real = httpx.AsyncClient

    def factory(**kwargs):
        kwargs.pop("http2", None)
        kwargs["transport"] = transport(handler)
        return real(**kwargs)

    monkeypatch.setattr(fetch.httpx, "AsyncClient", factory)


class TestFetchAll:
    def test_happy_path(self, monkeypatch):
        patch_client(monkeypatch, lambda req: httpx.Response(200, text="a.com"))
        got = fetch_all(["https://x/a"])
        assert got["https://x/a"].ok
        assert got["https://x/a"].text == "a.com"

    def test_urls_are_deduped(self, monkeypatch):
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, text="ok")

        patch_client(monkeypatch, handler)
        fetch_all(["https://x/a", "https://x/a", "https://x/b"])
        assert sorted(seen) == ["https://x/a", "https://x/b"]

    def test_empty_response_is_an_error(self, monkeypatch):
        patch_client(monkeypatch, lambda req: httpx.Response(200, text="   "))
        assert fetch_all(["https://x/a"])["https://x/a"].error == "响应为空"

    def test_no_urls(self):
        assert fetch_all([]) == {}

    def test_utf8_preferred_over_declared_encoding(self, monkeypatch):
        body = "例子.com".encode("utf-8")
        patch_client(
            monkeypatch,
            lambda req: httpx.Response(
                200, content=body, headers={"content-type": "text/plain; charset=gbk"}
            ),
        )
        assert fetch_all(["https://x/a"])["https://x/a"].text == "例子.com"


class TestSizeCap:
    """上限必须在收流时生效，而不是把整个响应读进内存之后才检查。"""

    def test_oversized_response_is_rejected(self, monkeypatch):
        huge = b"x" * (MAX_BYTES + 1024)
        patch_client(monkeypatch, lambda req: httpx.Response(200, content=huge))
        got = fetch_all(["https://x/big"])["https://x/big"]
        assert not got.ok and "上限" in (got.error or "")

    def test_reading_stops_at_the_cap(self):
        """收够上限就停手，不会把剩下的字节也拉进内存。

        旧实现是 client.get() 读完整个响应体再看 len(content) —— 内存那时已经
        花掉了，"防止把内存吃光"的注释是假的。
        """
        chunk = b"y" * (1 << 20)
        total = (MAX_BYTES // len(chunk)) + 8
        stream = _Stream(chunk, total)
        assert asyncio.run(_read_capped(stream)) is None
        assert stream.delivered < total

    def test_under_the_cap_reads_everything(self):
        stream = _Stream(b"z" * 1024, 4)
        assert asyncio.run(_read_capped(stream)) == b"z" * 4096
        assert stream.delivered == 4


class TestRetries:
    def test_transient_failure_is_retried(self, monkeypatch):
        calls = []

        def handler(request):
            calls.append(1)
            if len(calls) < 3:
                return httpx.Response(503)
            return httpx.Response(200, text="ok")

        patch_client(monkeypatch, handler)
        monkeypatch.setattr(fetch.asyncio, "sleep", _no_sleep)
        got = fetch_all(["https://x/a"], retries=3)["https://x/a"]
        assert got.ok and len(calls) == 3

    @pytest.mark.parametrize("status", [404, 403, 410])
    def test_permanent_failures_are_not_retried(self, monkeypatch, status):
        calls = []

        def handler(request):
            calls.append(1)
            return httpx.Response(status)

        patch_client(monkeypatch, handler)
        got = fetch_all(["https://x/a"], retries=3)["https://x/a"]
        assert got.error == f"HTTP {status}" and len(calls) == 1

    def test_exhausted_retries_report_the_last_error(self, monkeypatch):
        patch_client(monkeypatch, lambda req: httpx.Response(500))
        monkeypatch.setattr(fetch.asyncio, "sleep", _no_sleep)
        got = fetch_all(["https://x/a"], retries=2)["https://x/a"]
        assert got.error == "HTTP 500"

    def test_backoff_does_not_hold_a_concurrency_slot(self, monkeypatch):
        """一个死链不该在退避期间霸着并发位，把健康的源一起拖慢。

        并发设为 1：旧实现把信号量套在整个重试循环外，健康的源必须等死链
        把三轮退避睡完才轮得到。现在信号量只包住真正发请求的那一段，
        健康的源会在死链的退避间隙里先完成。
        """

        def handler(request):
            if request.url.path == "/dead":
                return httpx.Response(500)
            return httpx.Response(200, text="ok")

        patch_client(monkeypatch, handler)
        monkeypatch.setattr(fetch.asyncio, "sleep", _tiny_sleep)

        done: list[str] = []
        got = fetch_all(
            ["https://x/dead", "https://x/ok"],
            concurrency=1,
            retries=3,
            on_done=lambda f, i, n: done.append(f.url),
        )
        assert got["https://x/ok"].ok and not got["https://x/dead"].ok
        assert done[0] == "https://x/ok"
