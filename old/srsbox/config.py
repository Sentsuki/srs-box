"""配置加载与校验。

``rulesets`` 是唯一的命名空间，条目有两种形态，靠有没有 ``items`` 区分：

* 单个规则集 —— 字符串、数组，或含 ``sources`` 的对象；
* 一组规则集 —— 含 ``items`` 的对象，键名自动作为名字前缀。

展开之后做一次全局重名检测。旧配置把规则集分散在 ``ip_only`` / ``rulesets`` /
``convert`` 三段里，三者共用同一套输出文件名却互不知情，重名会静默覆盖。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .errors import ConfigError
from .parse import Format

SCHEMA = 1

# sing-box 只认 1-4（1.13.14 实测 version=5 报 "unknown rule-set version"）。
# 放行到 255 的话，写错的版本号要等到编译阶段才报，错误还很难懂。
# 新版 sing-box 增加版本时，改这一个数即可。
MAX_RULESET_VERSION = 4

# 规则集名字会直接变成输出文件名，必须限制成安全字符
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# 旧实现把这条硬编码在 ProcessorService 里，会静默丢弃规则且无任何提示。
# 这里仍然硬编码（只有一条，不值得开配置项），但丢弃条数会出现在摘要中。
DROP_VALUES_CONTAINING: tuple[str, ...] = ("ruleset.skk.moe",)


@dataclass(frozen=True)
class Ruleset:
    name: str
    sources: tuple[str, ...]
    format: Format = Format.SINGBOX
    aggregate: bool = False


@dataclass(frozen=True)
class Config:
    ruleset_version: int
    sing_box_version: str
    sing_box_platform: str
    json_dir: Path
    srs_dir: Path
    concurrency: int
    timeout: float
    retries: int
    rulesets: tuple[Ruleset, ...] = field(default_factory=tuple)
    sing_box_sha256: str | None = None


def _need(mapping: dict[str, Any], key: str, kind: type, where: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"{where} 缺少必需字段 {key!r}")
    value = mapping[key]
    if not isinstance(value, kind) or isinstance(value, bool) != (kind is bool):
        raise ConfigError(
            f"{where}.{key} 应为 {kind.__name__}，实际是 {type(value).__name__}"
        )
    return value


def _number(
    mapping: dict[str, Any],
    key: str,
    default: float,
    *,
    where: str,
    integer: bool = False,
) -> float:
    """取一个数值配置项。

    旧实现直接 ``int(fetch.get("concurrency", 8))``，配置里写个 ``"lots"``
    就会抛裸 ValueError 打出 traceback —— 违反"所有可预期失败都是 SrsBoxError"
    的约定。字符串 ``"8"`` 也一并拒绝：配置是 JSON，数字就该写成数字。
    """
    value = mapping.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(
            f"{where}.{key} 应为数字，实际是 {type(value).__name__}: {value!r}"
        )
    if integer and not float(value).is_integer():
        raise ConfigError(f"{where}.{key} 应为整数，实际是 {value!r}")
    return value


def _check_url(url: str, where: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ConfigError(
            f"{where} 的 URL 协议必须是 http/https，实际是 {parsed.scheme or '空'}: {url!r}"
        )
    if not parsed.netloc:
        raise ConfigError(f"{where} 的 URL 缺少主机名: {url!r}")
    return url


def _check_name(name: str, where: str) -> str:
    if not _NAME_RE.match(name):
        raise ConfigError(
            f"{where}: 规则集名 {name!r} 非法。名字会直接作为输出文件名，"
            "只允许字母、数字、点、下划线和连字符，且不能以点或连字符开头"
        )
    return name


def _as_paths(value: Any, where: str) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and value and all(isinstance(v, str) for v in value):
        return list(value)
    raise ConfigError(f"{where} 应为字符串或非空字符串数组")


def _parse_format(raw: Any, where: str) -> Format:
    if raw is None:
        return Format.SINGBOX
    try:
        return Format(str(raw))
    except ValueError:
        allowed = ", ".join(f.value for f in Format)
        raise ConfigError(
            f"{where}.format 取值非法: {raw!r}，可选: {allowed}"
        ) from None


def _expand_entry(key: str, spec: Any, out: dict[str, Ruleset]) -> None:
    where = f"rulesets.{key}"

    if isinstance(spec, (str, list)):
        spec = {"sources": _as_paths(spec, where)}
    if not isinstance(spec, dict):
        raise ConfigError(
            f"{where} 应为字符串、数组或对象，实际是 {type(spec).__name__}"
        )

    base = spec.get("base", "")
    if not isinstance(base, str):
        raise ConfigError(f"{where}.base 应为字符串")
    fmt = _parse_format(spec.get("format"), where)
    aggregate = bool(spec.get("aggregate", False))

    if "items" in spec:
        items = spec["items"]
        if not isinstance(items, dict) or not items:
            raise ConfigError(f"{where}.items 应为非空对象")
        if "sources" in spec:
            raise ConfigError(f"{where} 不能同时有 items 和 sources")
        prefix = spec.get("prefix", f"{key}-")
        if not isinstance(prefix, str):
            raise ConfigError(f"{where}.prefix 应为字符串")
        for item_key, paths in items.items():
            name = _check_name(prefix + item_key, f"{where}.items.{item_key}")
            if name in out:
                raise ConfigError(
                    f"规则集重名: {name!r}（展开自 {where}.items.{item_key}）"
                )
            urls = tuple(
                _check_url(base + p, f"{where}.items.{item_key}")
                for p in _as_paths(paths, f"{where}.items.{item_key}")
            )
            out[name] = Ruleset(name, urls, fmt, aggregate)
        return

    if "sources" not in spec:
        raise ConfigError(f"{where} 既没有 sources 也没有 items")
    name = _check_name(key, "rulesets")
    if name in out:
        raise ConfigError(f"规则集重名: {name!r}")
    urls = tuple(
        _check_url(base + s, where)
        for s in _as_paths(spec["sources"], f"{where}.sources")
    )
    out[name] = Ruleset(name, urls, fmt, aggregate)


def load(path: str | Path) -> Config:
    """读取并校验配置文件。任何问题都抛 :class:`ConfigError`，消息可直接展示。"""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"配置文件不存在: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"配置文件不是合法 JSON（行 {exc.lineno} 列 {exc.colno}）: {exc.msg}"
        ) from exc
    if not isinstance(raw, dict):
        raise ConfigError("配置文件顶层必须是对象")

    schema = raw.get("schema")
    if schema != SCHEMA:
        raise ConfigError(
            f"配置 schema 版本不匹配：期望 {SCHEMA}，实际 {schema!r}。"
            "旧版三段式配置（ip_only / rulesets / convert）需要迁移到新格式"
            if schema is None
            else f"配置 schema 版本不匹配：期望 {SCHEMA}，实际 {schema!r}"
        )

    ruleset_version = _need(raw, "ruleset_version", int, "配置")
    if not 1 <= ruleset_version <= MAX_RULESET_VERSION:
        raise ConfigError(
            f"ruleset_version 必须在 1-{MAX_RULESET_VERSION} 之间，"
            f"实际 {ruleset_version}。这是 sing-box 认识的全部 rule-set 版本；"
            "若新版 sing-box 增加了版本，请同步更新 srsbox.config.MAX_RULESET_VERSION"
        )

    sing_box = _need(raw, "sing_box", dict, "配置")
    version = _need(sing_box, "version", str, "sing_box")
    platform = _need(sing_box, "platform", str, "sing_box")
    sha256 = sing_box.get("sha256")
    if sha256 is not None and (
        not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", sha256)
    ):
        raise ConfigError(
            f"sing_box.sha256 应为 64 位十六进制字符串，实际是 {sha256!r}"
        )

    output = raw.get("output", {})
    if not isinstance(output, dict):
        raise ConfigError("output 应为对象")
    json_dir = Path(output.get("json_dir", "output/json"))
    srs_dir = Path(output.get("srs_dir", "output/srs"))

    fetch = raw.get("fetch", {})
    if not isinstance(fetch, dict):
        raise ConfigError("fetch 应为对象")
    concurrency = int(_number(fetch, "concurrency", 8, where="fetch", integer=True))
    timeout = float(_number(fetch, "timeout", 30, where="fetch"))
    retries = int(_number(fetch, "retries", 3, where="fetch", integer=True))
    if concurrency < 1:
        raise ConfigError("fetch.concurrency 必须 >= 1")
    if timeout <= 0:
        raise ConfigError("fetch.timeout 必须 > 0")
    if retries < 0:
        raise ConfigError("fetch.retries 不能为负")

    rulesets_raw = _need(raw, "rulesets", dict, "配置")
    if not rulesets_raw:
        raise ConfigError("rulesets 不能为空")
    expanded: dict[str, Ruleset] = {}
    for key, spec in rulesets_raw.items():
        _expand_entry(key, spec, expanded)

    return Config(
        ruleset_version=ruleset_version,
        sing_box_version=version,
        sing_box_platform=platform,
        json_dir=json_dir,
        srs_dir=srs_dir,
        concurrency=concurrency,
        timeout=timeout,
        retries=retries,
        rulesets=tuple(expanded.values()),
        sing_box_sha256=sha256.lower() if sha256 else None,
    )
