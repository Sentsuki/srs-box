"""解析层 —— 按配置声明的容器格式把源内容喂进 :class:`RuleSet`。

不做格式嗅探。默认 ``singbox``，其余格式必须在配置里声明；声明错了会在这里立刻
报错并给出改法，而不是猜一个然后产出垃圾。

四个容器解析器最终都汇流到同一套"条目"逻辑：

* **typed** —— ``DOMAIN-SUFFIX,example.com,PROXY``。策略列（第三段）直接丢弃。
  Clash / Surge / Quantumult X 的差异几乎都在这一列，丢掉之后它们就是同一种
  东西，所以本项目里不存在"方言"这个概念，词汇差异由 :mod:`srsbox.vocab`
  一张表吸收。
* **bare** —— 裸值。种类逐条判断（IP 还是域名），不需要先把整个文件归类，
  因此同一文件里混写 IP 和域名也能正确处理。
"""

from __future__ import annotations

import json
import re
from enum import Enum
from ipaddress import ip_network
from typing import Any, Iterable

from .errors import InvalidValue, ParseError
from .models import RuleSet, normalize
from .vocab import LOGICAL, SKIP, TYPES


class Format(str, Enum):
    SINGBOX = "singbox"
    TEXT = "text"
    YAML = "yaml"
    CIDR = "cidr"
    DOMAINSET = "domainset"

    def __str__(self) -> str:  # pragma: no cover - 仅用于错误信息
        return self.value


# 严格格式的字段白名单：出现白名单外的内容直接失败，不静默接受。
# cn-ip 这类从第三方仓库拉的 IP 列表最容易悄悄腐坏（上游返回 HTTP 200 的 HTML
# 错误页，下载层发现不了），严格模式让它当场暴露而不是混进规则集。
_STRICT_ALLOW: dict[Format, frozenset[str]] = {
    Format.CIDR: frozenset({"ip_cidr"}),
    # domain_regex 在列表里：通配符域名（``*.example.com``）是域名列表的常客，
    # 转换后落在 domain_regex 上。不放行的话，一个通配符就会让整个规则集失败。
    Format.DOMAINSET: frozenset({"domain", "domain_suffix", "domain_regex"}),
}

_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_COMMENT_RE = re.compile(r"\s+(?:#|//).*$")
_IPISH_RE = re.compile(r"^[0-9a-fA-F:.]+(?:/\d{1,3})?$")

_HINT = (
    '  若这是 Clash/Surge 规则列表，请为该规则集声明 "format": "text"；'
    '若是 YAML，声明 "format": "yaml"'
)


def _snippet(text: str, limit: int = 80) -> str:
    flat = " ".join(text.split())
    return flat[:limit] + ("…" if len(flat) > limit else "")


def _clean_line(line: str) -> str:
    """整行级清洗：去整行注释、去 YAML 序列短横线、去成对引号。

    剥掉 ``- `` 前缀这一步让 TEXT 路也能正确吃下一个 YAML 裸序列，于是"格式声明
    错了"在扁平列表这种绝大多数情形下只是降级而不是崩溃。
    """
    line = line.strip()
    if not line or line[0] in "#;" or line.startswith("//"):
        return ""
    if line.startswith("- "):
        line = line[2:].strip()
    elif line == "-":
        return ""
    if len(line) >= 2 and line[0] == line[-1] and line[0] in "\"'":
        line = line[1:-1].strip()
    return line


def _strip_inline_comment(value: str) -> str:
    """只剥"空白 + # 或 //"形式的行尾注释。

    不剥无空白前缀的 ``#``，避免把 domain_regex 里的 ``#`` 当注释砍掉。
    """
    return _COMMENT_RE.sub("", value).strip()


def _looks_like_ip(value: str) -> bool:
    """先用字符集快速排除，再用真解析确认。

    只看字符集会把纯十六进制字面的域名误判成 IP —— ``bad.cc``、``cafe.fee``、
    ``dead.beef`` 全部由 ``0-9a-f.`` 组成。误判的后果不只是丢一条规则：
    在 domainset 严格模式下 ``_commit`` 会因字段不在白名单而抛错，整个规则集陪葬。

    字符集正则是所有合法 IP 的超集，所以拿它做快速否定不会漏判；真解析只在
    少数"长得像 IP"的值上跑，几十万行的域名列表不会因此变慢。
    """
    if not _IPISH_RE.match(value):
        return False
    try:
        ip_network(value, strict=False)
    except ValueError:
        return False
    return True


def _regex_value(rest: str) -> str:
    """从 ``rest`` 里取出正则值，在第一个**顶层**逗号处截断。

    正则里的逗号几乎总在 ``{m,n}``、``[a,b]`` 或 ``(a|b)`` 内部，而策略列的逗号
    在最外层。无脑 ``split(",")`` 会把 ``^a{1,3}\\.com$`` 砍成 ``^a{1`` ——
    砍完仍是合法正则、仍能编译，只是匹配的东西变了，是最难发现的一类错误。
    """
    depth = 0
    escaped = False
    in_class = False
    for index, char in enumerate(rest):
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif in_class:
            if char == "]":
                in_class = False
        elif char == "[":
            in_class = True
        elif char in "({":
            depth += 1
        elif char in ")}":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            return rest[:index].strip()
    return rest.strip()


def _wildcard_to_regex(host: str) -> str | None:
    """``*.example.com`` -> ``^[^.]+\\.example\\.com$``；无法处理时返回 None。

    Clash 的 ``*`` 严格匹配**一个** label：``a.example.com`` 命中，
    ``a.b.example.com`` 和 ``example.com`` 都不命中。sing-box 的
    domain / domain_suffix 都没有等价写法，只能落到 domain_regex ——
    用 ``domain_suffix`` 近似会把多级子域也吃进来，那是放宽匹配面。

    只认整段 label 是 ``*`` 的写法；``ad*.example.com`` 这种返回 None，
    交给上层当普通域名处理（进而被记成非法值），而不是猜。
    """
    labels = host.split(".")
    if not any(label == "*" for label in labels):
        return None
    if any("*" in label and label != "*" for label in labels):
        return None
    parts = ["[^.]+" if label == "*" else re.escape(label) for label in labels]
    return "^" + r"\.".join(parts) + "$"


# ----------------------------------------------------------------------
# 条目层
# ----------------------------------------------------------------------


def _commit(
    field: str,
    value: Any,
    rs: RuleSet,
    allow: frozenset[str] | None,
    *,
    origin: str,
) -> None:
    if allow is not None and field not in allow:
        raise ParseError(
            f"严格格式不允许 {field} 规则（来自 {origin!r}）；"
            '若该源确实混有此类规则，请改用 "format": "text"'
        )
    if not rs.add_lenient(field, value) and allow is not None:
        raise ParseError(
            f"严格格式下遇到非法值: {origin!r}；"
            '请检查上游内容是否已变化，或改用 "format": "text"'
        )


def _feed_bare(value: str, rs: RuleSet, allow: frozenset[str] | None) -> None:
    """裸值：按值自身的形态决定字段，不依赖文件级归类。"""
    if not value:
        return
    origin = value
    if _looks_like_ip(value):
        field = "ip_cidr"
    elif value.startswith("+."):
        field, value = "domain_suffix", value[2:]
    elif "*" in value and (pattern := _wildcard_to_regex(value)) is not None:
        field, value = "domain_regex", pattern
    elif value.startswith("."):
        field, value = "domain_suffix", value[1:]
    else:
        field = "domain"
    _commit(field, value, rs, allow, origin=origin)


def _feed_logical(
    head: str,
    rest: str,
    rs: RuleSet,
    allow: frozenset[str] | None,
    *,
    origin: str,
) -> None:
    """``AND,((DOMAIN,a),(DOMAIN-SUFFIX,b))`` -> sing-box logical rule。

    逻辑规则走 :meth:`RuleSet.add_verbatim`，绕开了 ``add`` 那条归一化管道，
    因此这里必须自己调用 :func:`~srsbox.models.normalize`：否则 ``DST-PORT,443``
    会产出字符串端口，sing-box 直接拒绝编译**整个**规则集。

    任一子项无法表达时整条丢弃。逻辑规则少一个子项就是另一条规则 —— AND 少一项
    等于放宽匹配面，把它输出去比丢掉更危险 —— 所以绝不产出残缺的逻辑规则。
    """
    mode, invert = LOGICAL[head]
    children: list[dict[str, Any]] = []

    def give_up(reason: str) -> None:
        rs.diag.bad_value("logical", f"{reason}，整条逻辑规则丢弃: {origin!r}")

    for part in re.findall(r"\(([^()]*)\)", rest):
        sub_head, sep, sub_rest = part.partition(",")
        sub_type = sub_head.strip().upper()
        field = TYPES.get(sub_type)
        if not sep or field is None:
            give_up(f"子项类型 {sub_type or part!r} 无法表达")
            return
        if allow is not None and field not in allow:
            raise ParseError(
                f"严格格式不允许逻辑规则里的 {field} 子项（来自 {origin!r}）；"
                '若该源确实混有此类规则，请改用 "format": "text"'
            )
        if field == "domain_regex":
            value = _regex_value(sub_rest)
        else:
            value = _strip_inline_comment(sub_rest.split(",")[0].strip())
        try:
            children.append({field: [normalize(field, value)]})
        except InvalidValue as exc:
            give_up(f"子项取值非法（{exc}）")
            return

    if not children:
        give_up("没有可用子项")
        return
    rule: dict[str, Any] = {"type": "logical", "mode": mode, "rules": children}
    if invert:
        rule["invert"] = True
    rs.add_verbatim(rule)


def _feed_entry(raw: str, rs: RuleSet, allow: frozenset[str] | None) -> None:
    """一个条目 -> 若干规则值。分支互斥，没有 fallthrough 到"猜"。"""
    head, sep, rest = raw.partition(",")
    head = head.strip()
    upper = head.upper()

    if sep:
        if upper in LOGICAL:
            _feed_logical(upper, rest, rs, allow, origin=raw)
            return
        field = TYPES.get(upper)
        if field:
            if field == "domain_regex":
                value = _regex_value(rest)
            else:
                value = _strip_inline_comment(rest.split(",", 1)[0].strip())
            _commit(field, value, rs, allow, origin=raw)
            return
        if upper in SKIP:
            rs.diag.skip(upper)
            return
        if _TYPE_RE.match(head):
            rs.diag.unknown_type(head)
            return

    _feed_bare(_strip_inline_comment(raw), rs, allow)


# ----------------------------------------------------------------------
# 容器层
# ----------------------------------------------------------------------


def _parse_lines(text: str, rs: RuleSet, allow: frozenset[str] | None) -> None:
    for line in text.splitlines():
        entry = _clean_line(line)
        if entry:
            _feed_entry(entry, rs, allow)


def _parse_singbox(text: str, rs: RuleSet) -> None:
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ParseError(
            f"期望 sing-box JSON，解析失败（{exc.msg}，行 {exc.lineno}）；"
            f"实际收到 {_snippet(text)!r}\n" + _HINT
        ) from exc

    if isinstance(doc, dict):
        rules = doc.get("rules")
        if not isinstance(rules, list):
            raise ParseError(f"sing-box JSON 缺少 rules 数组，顶层键: {list(doc)[:6]}")
    elif isinstance(doc, list):
        rules = doc
    else:
        raise ParseError(
            f"sing-box JSON 顶层应为对象或数组，实际是 {type(doc).__name__}"
        )

    for rule in rules:
        if not isinstance(rule, dict):
            rs.diag.bad_value("rule", f"规则不是对象: {rule!r}")
            continue
        # 逻辑规则、带 invert 的规则、含未知字段的规则一律整条透传：
        # 把它们的值拆开与别的规则混在一起去重会改变语义。
        if rule.get("type") == "logical" or not rs.mergeable(rule):
            rs.add_verbatim(rule)
            continue
        for field, values in rule.items():
            for value in values if isinstance(values, list) else [values]:
                rs.add_lenient(field, value)


def _pick_sequence(doc: dict[str, Any]) -> list[Any] | None:
    """从 YAML 顶层对象里找出规则序列。

    不只认 ``payload``：``rules`` 等键名同样常见，找不到已知键时退而求其次，
    取"恰好唯一的那个列表值"。仍然找不到就报错 —— 旧实现在这里是静默返回空列表，
    整个规则集会变成 0 条而没有任何提示。
    """
    for key in ("payload", "rules", "domain", "domains", "host", "hosts", "ip"):
        if isinstance(doc.get(key), list):
            return doc[key]
    seqs = [v for v in doc.values() if isinstance(v, list)]
    return seqs[0] if len(seqs) == 1 else None


def _parse_yaml(text: str, rs: RuleSet, allow: frozenset[str] | None) -> None:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - 依赖已在 pyproject 声明
        raise ParseError("解析 YAML 需要 PyYAML：pip install pyyaml") from exc

    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ParseError(f"YAML 解析失败: {exc}") from exc

    if isinstance(doc, dict):
        items = _pick_sequence(doc)
        if items is None:
            raise ParseError(f"YAML 顶层是对象但找不到规则序列，键: {list(doc)[:6]}")
    elif isinstance(doc, list):
        items = doc
    else:
        raise ParseError(
            f"YAML 顶层应为序列或含序列的对象，实际是 {type(doc).__name__}；"
            f"实际收到 {_snippet(text)!r}\n" + _HINT
        )

    for item in items:
        if isinstance(item, str):
            entry = _clean_line(item)
            if entry:
                _feed_entry(entry, rs, allow)
        elif isinstance(item, dict):
            # {"DOMAIN-SUFFIX": ["a", "b"]} 这种映射写法
            for key, value in item.items():
                for one in value if isinstance(value, list) else [value]:
                    _feed_entry(f"{key},{one}", rs, allow)
        else:
            rs.diag.bad_value("yaml", f"无法识别的条目: {item!r}")


def parse(text: str, fmt: Format, rs: RuleSet) -> None:
    """把一个源的内容按声明的格式并入 ``rs``。"""
    if fmt is Format.SINGBOX:
        _parse_singbox(text, rs)
    elif fmt is Format.YAML:
        _parse_yaml(text, rs, _STRICT_ALLOW.get(fmt))
    else:
        _parse_lines(text, rs, _STRICT_ALLOW.get(fmt))


def parse_all(chunks: Iterable[str], fmt: Format, rs: RuleSet) -> None:
    """依次把多个源的内容并入同一个规则集。"""
    for text in chunks:
        parse(text, fmt, rs)
