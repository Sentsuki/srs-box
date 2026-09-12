"""命令行入口与流水线编排。

编排上只有一条硬规则：**每个规则集是独立单元**。任何一个规则集失败都不会影响
其他规则集，退出码只看总产出。旧实现是分阶段整体判定，某一类源全挂就直接
``return False``，连已经下载好的几十个规则集都不再编译，摘要也不会打印。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import __version__
from . import config as config_mod
from .compiler import SingBox
from .config import DROP_VALUES_CONTAINING, Config, Ruleset
from .errors import SrsBoxError
from .fetch import Fetched, fetch_all
from .models import RuleSet
from .parse import parse
from .report import Result, run_report, summarize, write_run_report

log = logging.getLogger("srsbox")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="srsbox", description="把各家规则源编译成 sing-box rule-set"
    )
    parser.add_argument("-c", "--config", default="config.json", help="配置文件路径")
    parser.add_argument(
        "--only",
        action="append",
        metavar="NAME",
        help="只处理指定规则集，可重复",
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true", help="只生成 JSON，不编译 .srs"
    )
    parser.add_argument(
        "--strict", action="store_true", help="任一规则集失败即以非零码退出"
    )
    parser.add_argument("--sing-box", metavar="PATH", help="使用指定的 sing-box 二进制")
    parser.add_argument(
        "--report-json",
        metavar="PATH",
        help="把本次运行的机器可读报告写到该路径（供发布流程判断保留/清理）",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="更详细的日志"
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="只输出警告和错误")
    parser.add_argument("--version", action="version", version=f"srs-box {__version__}")
    return parser


def force_utf8() -> None:
    """把标准流切到 UTF-8。

    Windows 控制台默认是 cp1252/GBK，输出中文会直接抛 UnicodeEncodeError 把
    进程打死 —— 连 ``--help`` 都跑不起来。
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(  # type: ignore[union-attr]
                encoding="utf-8", errors="replace"
            )
        except (AttributeError, OSError, ValueError):  # pragma: no cover
            pass


def setup_logging(verbose: int, quiet: bool) -> None:
    level = logging.WARNING if quiet else (logging.DEBUG if verbose else logging.INFO)
    logging.basicConfig(
        level=level, format="%(message)s", stream=sys.stderr, force=True
    )
    # httpx 会把每个请求打成 INFO，几十个源刷屏。只有 -vv 才放出来。
    logging.getLogger("httpx").setLevel(
        logging.DEBUG if verbose >= 2 else logging.WARNING
    )
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def select(cfg: Config, only: list[str] | None) -> tuple[Ruleset, ...]:
    if not only:
        return cfg.rulesets
    wanted = set(only)
    chosen = tuple(r for r in cfg.rulesets if r.name in wanted)
    missing = wanted - {r.name for r in chosen}
    if missing:
        raise SrsBoxError(f"配置里没有这些规则集: {', '.join(sorted(missing))}")
    return chosen


def build(
    spec: Ruleset, fetched: dict[str, Fetched], version: int
) -> tuple[Result, RuleSet | None]:
    """解析、合并、归一一个规则集。返回 (结果, 规则集)，失败时后者为 None。"""
    result = Result(name=spec.name)
    rs = RuleSet(spec.name, version)

    texts: list[str] = []
    for url in spec.sources:
        got = fetched.get(url)
        if got is None or not got.ok:
            reason = got.error if got else "未下载"
            result.failed_sources.append(f"{url} ({reason})")
        else:
            texts.append(got.text or "")

    if not texts:
        result.error = "所有源都不可用"
        result.diag = rs.diag
        return result, None

    try:
        for text in texts:
            parse(text, spec.format, rs)
    except SrsBoxError as exc:
        result.error = str(exc)
        result.diag = rs.diag
        return result, None

    rs.drop_values_containing(DROP_VALUES_CONTAINING)
    if spec.aggregate:
        result.aggregated = rs.aggregate_cidr()

    result.diag = rs.diag
    result.rules = rs.total
    if rs.is_empty():
        result.error = "解析后没有任何规则"
        return result, None

    result.ok = True
    return result, rs


def write_json(result: Result, rs: RuleSet, json_dir: Path) -> None:
    json_dir.mkdir(parents=True, exist_ok=True)
    path = json_dir / f"{result.name}.json"
    payload = json.dumps(rs.to_json(), ensure_ascii=False, indent=2)
    path.write_text(payload + "\n", encoding="utf-8")
    result.json_path = path


def run(args: argparse.Namespace) -> int:
    cfg = config_mod.load(args.config)
    specs = select(cfg, args.only)
    log.info("配置 %s：%d 个规则集", args.config, len(specs))

    urls = list(dict.fromkeys(u for s in specs for u in s.sources))
    log.info("抓取 %d 个源（并发 %d）", len(urls), cfg.concurrency)

    def on_done(got: Fetched, index: int, total: int) -> None:
        if got.ok:
            log.debug("[%d/%d] %s", index, total, got.url)
        else:
            log.warning("[%d/%d] 失败 %s — %s", index, total, got.url, got.error)

    fetched = fetch_all(
        urls,
        concurrency=cfg.concurrency,
        timeout=cfg.timeout,
        retries=cfg.retries,
        on_done=on_done,
    )

    results: list[Result] = []
    for spec in specs:
        try:
            result, rs = build(spec, fetched, cfg.ruleset_version)
            if rs is not None:
                write_json(result, rs, cfg.json_dir)
        except SrsBoxError as exc:
            result = Result(name=spec.name, error=str(exc))
        except OSError as exc:
            result = Result(name=spec.name, error=f"写入失败: {exc}")
        results.append(result)
        log.debug("%s: %s", spec.name, "ok" if result.ok else result.error)

    compiled = False
    if not args.dry_run and any(r.ok for r in results):
        sing_box = SingBox(
            cfg.sing_box_version,
            cfg.sing_box_platform,
            explicit=args.sing_box,
            sha256=cfg.sing_box_sha256,
        )
        try:
            log.info("使用 sing-box %s", sing_box.binary())
            compiled = True
        except SrsBoxError as exc:
            # 二进制拿不到是全局性失败，但已生成的 JSON 仍然有效
            log.error("%s", exc)
            for result in results:
                if result.ok:
                    result.ok = False
                    result.error = str(exc)

        if compiled:
            for result in results:
                if not result.ok or result.json_path is None:
                    continue
                target = cfg.srs_dir / f"{result.name}.srs"
                try:
                    result.srs_size = sing_box.compile(result.json_path, target)
                    result.srs_path = target
                except SrsBoxError as exc:
                    result.ok = False
                    result.error = str(exc)

    summarize(results, compiled=compiled)

    if args.report_json:
        write_run_report(
            Path(args.report_json),
            run_report(
                results,
                configured=[r.name for r in cfg.rulesets],
                selected={s.name for s in specs},
            ),
        )

    succeeded = sum(1 for r in results if r.ok)
    if succeeded == 0:
        return 1
    if args.strict and succeeded != len(results):
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    force_utf8()
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose, args.quiet)
    try:
        return run(args)
    except SrsBoxError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("已中断", file=sys.stderr)
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
