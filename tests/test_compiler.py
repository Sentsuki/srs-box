import hashlib
import os
import sys

import pytest

from srsbox.compiler import SingBox
from srsbox.errors import CompileError

# 用当前解释器冒充 sing-box：`python version` 会以非零码退出，
# 所以再包一层脚本，让 `<script> version` 成功返回。
STUB = """import sys
sys.exit(0 if sys.argv[1:2] == ["version"] else 1)
"""


@pytest.fixture
def stub_binary(tmp_path):
    """一个能跑、能对 `version` 返回 0 的假二进制。"""
    script = tmp_path / "stub.py"
    script.write_text(STUB, encoding="utf-8")
    launcher = tmp_path / "sing-box"
    if os.name == "nt":
        launcher = tmp_path / "sing-box.bat"
        launcher.write_text(f'@echo off\r\n"{sys.executable}" "{script}" %*\r\n')
    else:
        launcher.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
        launcher.chmod(0o755)
    return launcher


def digest_of(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestExplicitBinary:
    def test_missing_binary_is_reported(self, tmp_path):
        box = SingBox("1.0", "linux-amd64", explicit=tmp_path / "nope")
        with pytest.raises(CompileError, match="不存在"):
            box.binary()

    def test_unrunnable_binary_is_reported(self, tmp_path):
        broken = tmp_path / "sing-box"
        broken.write_bytes(b"not an executable at all")
        box = SingBox("1.0", "linux-amd64", explicit=broken)
        with pytest.raises(CompileError):
            box.binary()

    def test_env_var_is_honoured(self, tmp_path, monkeypatch, stub_binary):
        monkeypatch.setenv("SING_BOX_BIN", str(stub_binary))
        assert SingBox("1.0", "linux-amd64").binary() == stub_binary.resolve()


class TestDigestPinning:
    """SagerNet 不发布官方校验和，所以这里提供的是可选的自钉。"""

    def test_no_pin_means_no_check(self, stub_binary):
        box = SingBox("1.0", "linux-amd64", explicit=stub_binary)
        assert box.binary() == stub_binary.resolve()

    def test_matching_digest_passes(self, stub_binary):
        box = SingBox(
            "1.0", "linux-amd64", explicit=stub_binary, sha256=digest_of(stub_binary)
        )
        assert box.binary() == stub_binary.resolve()

    def test_digest_comparison_is_case_insensitive(self, stub_binary):
        box = SingBox(
            "1.0",
            "linux-amd64",
            explicit=stub_binary,
            sha256=digest_of(stub_binary).upper(),
        )
        assert box.binary() == stub_binary.resolve()

    def test_mismatch_refuses_to_run_it(self, stub_binary):
        box = SingBox("1.0", "linux-amd64", explicit=stub_binary, sha256="0" * 64)
        with pytest.raises(CompileError, match="校验和不匹配") as excinfo:
            box.binary()
        # 错误信息要给出实际值，否则用户无从更新配置
        assert digest_of(stub_binary) in str(excinfo.value)


class TestCache:
    def test_cached_binary_is_verified_not_trusted_blindly(self, tmp_path):
        """旧实现命中缓存就直接返回，坏掉的缓存会一直用下去。"""
        cache = tmp_path / "cache"
        target = cache / "1.0-linux-amd64"
        target.mkdir(parents=True)
        (target / "sing-box").write_bytes(b"truncated garbage")

        box = SingBox("1.0", "linux-amd64", cache_dir=cache)
        with pytest.raises(CompileError):
            box.binary()

    def test_cached_binary_digest_is_checked(self, tmp_path, stub_binary):
        cache = tmp_path / "cache"
        target = cache / "1.0-linux-amd64"
        target.mkdir(parents=True)
        name = "sing-box.bat" if os.name == "nt" else "sing-box"
        cached = target / name
        cached.write_bytes(stub_binary.read_bytes())
        if os.name != "nt":
            cached.chmod(0o755)

        platform = "windows-amd64" if os.name == "nt" else "linux-amd64"
        box = SingBox("1.0", platform, cache_dir=cache, sha256="0" * 64)
        with pytest.raises(CompileError, match="校验和不匹配"):
            box.binary()

    def test_install_is_atomic(self, tmp_path):
        """中途失败不该在缓存里留下半个二进制。"""
        box = SingBox("1.0", "linux-amd64", cache_dir=tmp_path / "cache")
        source = tmp_path / "src"
        source.write_bytes(b"payload")
        target = tmp_path / "cache" / "sing-box"

        box._install(source, target)
        assert target.read_bytes() == b"payload"
        # 临时文件不该留下
        assert not list(target.parent.glob("*.part"))

    def test_install_replaces_an_existing_file(self, tmp_path):
        box = SingBox("1.0", "linux-amd64", cache_dir=tmp_path / "cache")
        target = tmp_path / "cache" / "sing-box"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"old")
        source = tmp_path / "src"
        source.write_bytes(b"new")

        box._install(source, target)
        assert target.read_bytes() == b"new"
