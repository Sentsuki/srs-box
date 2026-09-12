"""sing-box 二进制的获取与调用。

二进制只落一次盘，放在版本化的缓存目录里而不是工作区根目录，因此重复运行
不需要重新下载 20+ MB，CI 也可以直接缓存这个目录。

本地开发可以用 ``--sing-box`` 或环境变量 ``SING_BOX_BIN`` 指向已有的二进制，
完全跳过下载。
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

import httpx

from .errors import CompileError
from .fetch import USER_AGENT

RELEASE_URL = (
    "https://github.com/SagerNet/sing-box/releases/download/"
    "v{version}/sing-box-{version}-{platform}.{ext}"
)

# 编译超时按输入大小放缩：cn-ip 这种几十万条 CIDR 的规则集在慢机器上
# 远超旧实现写死的 60 秒。
_BASE_TIMEOUT = 60.0
_SECONDS_PER_MB = 30.0


class SingBox:
    def __init__(
        self,
        version: str,
        platform: str,
        *,
        cache_dir: Path = Path(".cache/sing-box"),
        explicit: str | os.PathLike[str] | None = None,
    ) -> None:
        self.version = version
        self.platform = platform
        self.cache_dir = Path(cache_dir) / f"{version}-{platform}"
        self.explicit = explicit or os.environ.get("SING_BOX_BIN") or None
        self._binary: Path | None = None

    # ---------------- 获取 ----------------

    @property
    def _is_windows(self) -> bool:
        return "windows" in self.platform

    @property
    def _binary_name(self) -> str:
        return "sing-box.exe" if self._is_windows else "sing-box"

    def binary(self) -> Path:
        """返回可执行文件路径，必要时下载并解压。结果会缓存。"""
        if self._binary is not None:
            return self._binary

        if self.explicit:
            path = Path(self.explicit)
            if not path.is_file():
                raise CompileError(f"指定的 sing-box 不存在: {path}")
            self._binary = path.resolve()
            self._verify(self._binary)
            return self._binary

        cached = self.cache_dir / self._binary_name
        if cached.is_file():
            self._binary = cached.resolve()
            return self._binary

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        ext = "zip" if self._is_windows else "tar.gz"
        url = RELEASE_URL.format(version=self.version, platform=self.platform, ext=ext)

        with tempfile.TemporaryDirectory(prefix="srsbox-singbox-") as tmp:
            archive = Path(tmp) / f"sing-box.{ext}"
            self._download(url, archive)
            extracted = self._extract(archive, Path(tmp))
            shutil.copy2(extracted, cached)

        if os.name != "nt":
            cached.chmod(cached.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP)
        self._binary = cached.resolve()
        self._verify(self._binary)
        return self._binary

    def _download(self, url: str, target: Path) -> None:
        try:
            with httpx.stream(
                "GET",
                url,
                follow_redirects=True,
                timeout=120.0,
                headers={"User-Agent": USER_AGENT},
            ) as response:
                if response.status_code == 404:
                    raise CompileError(
                        f"sing-box {self.version} 的 {self.platform} 构建不存在: {url}\n"
                        "  请检查 sing_box.version 与 sing_box.platform 是否匹配实际发布"
                    )
                response.raise_for_status()
                with target.open("wb") as handle:
                    for chunk in response.iter_bytes(65536):
                        handle.write(chunk)
        except httpx.HTTPError as exc:
            raise CompileError(f"下载 sing-box 失败: {exc}") from exc

    def _extract(self, archive: Path, into: Path) -> Path:
        target_dir = into / "unpacked"
        try:
            if archive.suffix == ".zip":
                with zipfile.ZipFile(archive) as zf:
                    zf.extractall(target_dir)
            else:
                with tarfile.open(archive, "r:gz") as tf:
                    # filter="data" 阻断压缩包里的路径穿越/特殊文件；
                    # 旧实现直接 extractall，Python 3.14 起默认行为也会改变。
                    tf.extractall(target_dir, filter="data")
        except (tarfile.TarError, zipfile.BadZipFile, OSError) as exc:
            raise CompileError(f"解压 sing-box 失败: {exc}") from exc

        found = next(target_dir.rglob(self._binary_name), None)
        if found is None:
            raise CompileError(f"压缩包里找不到 {self._binary_name}")
        return found

    def _verify(self, binary: Path) -> None:
        try:
            result = subprocess.run(
                [str(binary), "version"],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise CompileError(f"无法执行 sing-box ({binary}): {exc}") from exc
        if result.returncode != 0:
            raise CompileError(
                f"sing-box 无法运行 ({binary}): {result.stderr.strip() or '未知错误'}"
            )

    # ---------------- 编译 ----------------

    def compile(self, source: Path, target: Path) -> int:
        """把 JSON 规则集编译成 .srs，返回产物字节数。"""
        binary = self.binary()
        target.parent.mkdir(parents=True, exist_ok=True)
        size_mb = source.stat().st_size / 1e6
        timeout = _BASE_TIMEOUT + size_mb * _SECONDS_PER_MB

        try:
            result = subprocess.run(
                [
                    str(binary),
                    "rule-set",
                    "compile",
                    str(source),
                    "--output",
                    str(target),
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            raise CompileError(f"编译超时（>{timeout:.0f}s）") from None
        except OSError as exc:
            raise CompileError(f"调用 sing-box 失败: {exc}") from exc

        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip() or "未知错误"
            raise CompileError(f"sing-box 编译失败: {detail}")
        if not target.is_file() or target.stat().st_size == 0:
            raise CompileError("sing-box 返回成功但没有产出有效文件")
        return target.stat().st_size
