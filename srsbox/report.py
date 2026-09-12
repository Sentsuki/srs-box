"""执行结果与摘要输出。

摘要直接写 stdout，不经过 logging。旧实现把整个摘要打成 INFO 日志，而配置里
``level: SUCCESS`` 会把它整体过滤掉 —— CI 日志里永远看不到哪个规则集失败、
为什么失败。日志级别只该影响诊断信息，不该影响结果。
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

from .models import Diagnostics


@dataclass
class Result:
    """单个规则集的最终状态。"""

    name: str
    ok: bool = False
    rules: int = 0
    error: str | None = None
    diag: Diagnostics = field(default_factory=Diagnostics)
    json_path: Path | None = None
    srs_path: Path | None = None
    srs_size: int = 0
    aggregated: int = 0
    failed_sources: list[str] = field(default_factory=list)


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"  # pragma: no cover


def _notes(result: Result) -> list[str]:
    notes: list[str] = []
    diag = result.diag
    if diag.skipped:
        notes.append(
            "跳过 " + ", ".join(f"{k}×{v}" for k, v in diag.skipped.most_common(4))
        )
    if diag.unknown:
        notes.append(
            "未知类型 " + ", ".join(f"{k}×{v}" for k, v in diag.unknown.most_common(4))
        )
    if diag.invalid_total:
        notes.append(f"非法值 {diag.invalid_total}")
    if diag.dropped:
        notes.append(f"按过滤规则丢弃 {diag.dropped}")
    if result.aggregated:
        notes.append(f"CIDR 聚合 -{result.aggregated}")
    if result.failed_sources:
        notes.append(f"{len(result.failed_sources)} 个源不可用")
    return notes


def summarize(results: list[Result], *, compiled: bool, stream=None) -> None:
    out = stream or sys.stdout
    write = lambda line="": print(line, file=out)  # noqa: E731

    ok = [r for r in results if r.ok]
    bad = [r for r in results if not r.ok]
    width = max((len(r.name) for r in results), default=10)

    write()
    write("规则集")
    write("─" * 60)
    for result in sorted(results, key=lambda r: r.name):
        mark = "✓" if result.ok else "✗"
        size = f"{human_size(result.srs_size):>9}" if result.srs_size else " " * 9
        line = f"  {mark} {result.name:<{width}}  {result.rules:>7,} 条 {size}"
        notes = _notes(result)
        if notes:
            line += "   " + "; ".join(notes)
        write(line)
        if result.error:
            for chunk in result.error.splitlines():
                write(f"      {chunk}")
        for url in result.failed_sources:
            write(f"      源不可用: {url}")

    total_rules = sum(r.rules for r in ok)
    total_size = sum(r.srs_size for r in ok)
    write("─" * 60)
    verb = "编译" if compiled else "生成"
    write(
        f"  {verb} {len(ok)}/{len(results)} 个规则集，"
        f"共 {total_rules:,} 条规则"
        + (f"，{human_size(total_size)}" if total_size else "")
    )

    samples = [s for r in results for s in r.diag.invalid_samples][:5]
    if samples:
        write()
        write("非法值样例:")
        for sample in samples:
            write(f"  · {sample}")

    if bad:
        write()
        write(f"失败 {len(bad)} 个: " + ", ".join(r.name for r in bad))
    write()
