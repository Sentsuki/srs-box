"""规范化数据模型 —— 整个项目唯一的规则容器。

设计契约：

* 规则值有两条入口，只能经 :meth:`RuleSet.to_json` 离开。
* 主入口 :meth:`RuleSet.add` 负责字段白名单和取值归一/校验，因此并入 ``plain``
  的值不可能是 sing-box 不认识的字段名、错误类型（例如字符串端口）或非法 CIDR。
* 旁路 :meth:`RuleSet.add_verbatim` 原样透传整条规则，**不做任何校验** ——
  这是它存在的意义（上游的未知字段不该被我们判死刑），也是它的风险：
  自己构造 verbatim 规则的调用方必须自行调用 :func:`normalize`，
  否则就绕开了上面那条保证。
* ``to_json`` 是唯一的序列化出口，转义交给 ``json`` 模块完成，
  绝不对序列化结果做字符串手术。
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from ipaddress import collapse_addresses, ip_network
from itertools import chain
from typing import Any, Iterable

from .errors import InvalidValue

# 输出顺序固定，保证同样的输入产出逐字节相同的文件（便于 diff 和幂等提交）
FIELD_ORDER: tuple[str, ...] = (
    "query_type",
    "network",
    "domain",
    "domain_suffix",
    "domain_keyword",
    "domain_regex",
    "source_ip_cidr",
    "ip_cidr",
    "source_port",
    "port",
    "process_name",
    "process_path",
    "package_name",
)
FIELDS: frozenset[str] = frozenset(FIELD_ORDER)

_INT_FIELDS = frozenset({"port", "source_port"})
_CIDR_FIELDS = frozenset({"ip_cidr", "source_ip_cidr"})
_HOST_FIELDS = frozenset({"domain", "domain_suffix"})
# 大小写敏感，不能 lower()。
#
# 域名可以安全地小写（DNS 本就大小写不敏感），但进程名和路径在 Linux/macOS 上
# 是大小写敏感的：把 ``Telegram`` 压成 ``telegram`` 会让规则永不命中。
_VERBATIM_FIELDS = frozenset(
    {"domain_regex", "process_name", "process_path", "package_name"}
)

# 不含 ``*``：带通配符的值在 sing-box 的 domain/domain_suffix 里是字面量，
# 永不命中。通配符域名由解析层转成 domain_regex，到不了这里。
_LABEL = r"(?!-)[A-Za-z0-9_-]{1,63}(?<!-)"
_DOMAIN_RE = re.compile(rf"^(?=.{{1,253}}$){_LABEL}(?:\.{_LABEL})*$")

MAX_INVALID_SAMPLES = 5


def _to_ascii(host: str) -> str:
    """把非 ASCII 域名逐 label 转 punycode；转不了的 label 原样保留。"""
    if host.isascii():
        return host
    out = []
    for label in host.split("."):
        if label.isascii():
            out.append(label)
        else:
            try:
                out.append(label.encode("idna").decode("ascii"))
            except UnicodeError:
                out.append(label)
    return ".".join(out)


def normalize_host(raw: str) -> str:
    """归一域名：小写、去首尾空白、去末尾根点、punycode。保留前导点。"""
    host = raw.strip().lower().rstrip(".")
    lead = ""
    if host.startswith("."):
        lead, host = ".", host.lstrip(".")
    if not host:
        raise InvalidValue(f"空域名: {raw!r}")
    host = _to_ascii(host)
    if not _DOMAIN_RE.match(host):
        raise InvalidValue(f"非法域名: {raw!r}")
    return lead + host


def normalize_cidr(raw: str) -> str:
    """归一 IP/CIDR。裸 IP 补全掩码；主机位非零的网段按网络地址归整。"""
    text = raw.strip()
    try:
        return str(ip_network(text, strict=False))
    except ValueError as exc:
        raise InvalidValue(f"非法 IP/CIDR: {raw!r} ({exc})") from exc


def normalize_port(raw: Any) -> int:
    try:
        port = int(str(raw).strip())
    except ValueError as exc:
        raise InvalidValue(f"非法端口: {raw!r}") from exc
    if not 0 <= port <= 65535:
        raise InvalidValue(f"端口越界: {raw!r}")
    return port


def normalize(field_name: str, raw: Any) -> str | int:
    """按字段把原始值归一成可直接写进 JSON 的值。"""
    if field_name not in FIELDS:
        raise InvalidValue(f"不支持的字段: {field_name}")
    if field_name in _INT_FIELDS:
        return normalize_port(raw)
    if field_name in _CIDR_FIELDS:
        return normalize_cidr(str(raw))
    if field_name in _HOST_FIELDS:
        return normalize_host(str(raw))
    if field_name in _VERBATIM_FIELDS:
        value = str(raw).strip()
    else:
        value = str(raw).strip().lower()
    if not value:
        raise InvalidValue(f"{field_name} 的值为空")
    return value


@dataclass
class Diagnostics:
    """解析过程中积累的"非致命异常"，最终出现在摘要里。

    旧实现把这些打成 INFO 日志，而配置里 level=SUCCESS 会把它们全部过滤掉，
    等于永远看不见。这里改成结构化累计，由摘要直接输出。
    """

    skipped: Counter[str] = field(default_factory=Counter)
    unknown: Counter[str] = field(default_factory=Counter)
    invalid: Counter[str] = field(default_factory=Counter)
    invalid_samples: list[str] = field(default_factory=list)
    dropped: int = 0

    def skip(self, rule_type: str) -> None:
        self.skipped[rule_type.upper()] += 1

    def unknown_type(self, rule_type: str) -> None:
        self.unknown[rule_type.upper()] += 1

    def bad_value(self, reason: str, sample: str) -> None:
        self.invalid[reason] += 1
        if len(self.invalid_samples) < MAX_INVALID_SAMPLES:
            self.invalid_samples.append(sample)

    @property
    def invalid_total(self) -> int:
        return sum(self.invalid.values())

    def merge(self, other: "Diagnostics") -> None:
        self.skipped.update(other.skipped)
        self.unknown.update(other.unknown)
        self.invalid.update(other.invalid)
        self.dropped += other.dropped
        room = MAX_INVALID_SAMPLES - len(self.invalid_samples)
        if room > 0:
            self.invalid_samples.extend(other.invalid_samples[:room])


class RuleSet:
    """一个具名规则集的规范化中间表示。"""

    def __init__(self, name: str, version: int) -> None:
        self.name = name
        self.version = version
        self.plain: dict[str, set[str | int]] = {}
        # 无法并入 plain 的规则（逻辑规则、带 invert 的规则、含未知字段的规则）
        # 原样透传，绝不丢弃，也绝不与其他规则的值混在一起去重。
        self.verbatim: list[dict[str, Any]] = []
        self.diag = Diagnostics()

    # ---------------- 写入 ----------------

    def add(self, field_name: str, raw: Any) -> None:
        """加入一条规则值。非法值抛 InvalidValue，由调用方决定记录还是中止。"""
        value = normalize(field_name, raw)
        self.plain.setdefault(field_name, set()).add(value)

    def add_lenient(self, field_name: str, raw: Any) -> bool:
        """加入一条规则值；非法则记入 diagnostics 并返回 False。"""
        try:
            self.add(field_name, raw)
            return True
        except InvalidValue as exc:
            self.diag.bad_value(field_name, str(exc))
            return False

    def add_verbatim(self, rule: dict[str, Any]) -> None:
        """原样透传一条无法合并的规则。

        不做校验：上游 sing-box JSON 里的未知字段正是靠这条路活下来的。
        代价是调用方若**自己构造**规则（而非透传上游内容），必须先用
        :func:`normalize` 归一每个叶子值 —— 见 :func:`srsbox.parse._feed_logical`。
        """
        self.verbatim.append(rule)

    def mergeable(self, rule: dict[str, Any]) -> bool:
        """判断一条 sing-box 规则能否并入 plain。

        带 ``invert`` 的规则语义是整条取反，把它的值和别的规则混在一起去重会
        改变语义；含未知字段的规则我们无法校验，同样只能整条透传。
        """
        return bool(rule) and "invert" not in rule and set(rule) <= FIELDS

    # ---------------- 变换 ----------------

    def drop_values_containing(self, needles: Iterable[str]) -> int:
        """删除含指定子串的规则值（大小写不敏感），返回删除条数。"""
        lowered = [n.lower() for n in needles]
        if not lowered:
            return 0
        removed = 0
        for field_name, values in self.plain.items():
            if field_name in _INT_FIELDS:
                continue
            kept = {v for v in values if not any(n in str(v).lower() for n in lowered)}
            removed += len(values) - len(kept)
            self.plain[field_name] = kept
        self.diag.dropped += removed
        return removed

    def aggregate_cidr(self) -> int:
        """合并相邻/包含的网段，返回减少的条数。v4 与 v6 分开处理。"""
        total_removed = 0
        for field_name in ("ip_cidr", "source_ip_cidr"):
            values = self.plain.get(field_name)
            if not values:
                continue
            v4, v6 = [], []
            for value in values:
                text = str(value)
                (v6 if ":" in text else v4).append(ip_network(text))
            merged = {
                str(net)
                for net in chain(collapse_addresses(v4), collapse_addresses(v6))
            }
            total_removed += len(values) - len(merged)
            self.plain[field_name] = set(merged)
        return total_removed

    # ---------------- 读出 ----------------

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.plain.values()) + len(self.verbatim)

    def is_empty(self) -> bool:
        return self.total == 0

    def counts(self) -> dict[str, int]:
        out = {f: len(self.plain[f]) for f in FIELD_ORDER if self.plain.get(f)}
        if self.verbatim:
            out["verbatim"] = len(self.verbatim)
        return out

    def to_json(self) -> dict[str, Any]:
        """唯一的序列化出口。排序保证幂等，转义交给 json 模块。"""
        rules: list[dict[str, Any]] = []
        for field_name in FIELD_ORDER:
            values = self.plain.get(field_name)
            if values:
                rules.append({field_name: sorted(values)})
        rules.extend(self.verbatim)
        return {"version": self.version, "rules": rules}
