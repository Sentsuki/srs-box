import json

from srsbox import cli
from srsbox.config import Ruleset
from srsbox.fetch import Fetched
from srsbox.parse import Format
from srsbox.report import Result, run_report, summarize


def ok(url, text):
    return Fetched(url, text=text)


SINGBOX = json.dumps({"version": 2, "rules": [{"domain": ["a.com"]}]})


class TestBuild:
    def test_happy_path(self):
        spec = Ruleset("x", ("u1",))
        result, rs = cli.build(spec, {"u1": ok("u1", SINGBOX)}, 4)
        assert result.ok and result.rules == 1
        assert rs is not None and rs.version == 4

    def test_partial_source_failure_still_produces_output(self):
        # 一个源挂了不该让整个规则集作废
        spec = Ruleset("x", ("u1", "u2"))
        fetched = {"u1": ok("u1", SINGBOX), "u2": Fetched("u2", error="HTTP 502")}
        result, rs = cli.build(spec, fetched, 4)
        assert result.ok and rs is not None
        assert result.failed_sources == ["u2 (HTTP 502)"]

    def test_all_sources_failed(self):
        spec = Ruleset("x", ("u1",))
        result, rs = cli.build(spec, {"u1": Fetched("u1", error="超时")}, 4)
        assert not result.ok and rs is None
        assert result.error == "所有源都不可用"

    def test_parse_error_is_contained_in_the_result(self):
        spec = Ruleset("x", ("u1",), Format.SINGBOX)
        result, rs = cli.build(spec, {"u1": ok("u1", "DOMAIN,a.com")}, 4)
        assert not result.ok and rs is None
        assert "期望 sing-box JSON" in (result.error or "")

    def test_empty_after_parsing_is_a_failure(self):
        spec = Ruleset("x", ("u1",), Format.TEXT)
        result, rs = cli.build(spec, {"u1": ok("u1", "# 只有注释\n")}, 4)
        assert not result.ok and result.error == "解析后没有任何规则"

    def test_hardcoded_filter_is_applied_and_counted(self):
        spec = Ruleset("x", ("u1",), Format.TEXT)
        text = "DOMAIN,keep.com\nDOMAIN,x.ruleset.skk.moe\n"
        result, rs = cli.build(spec, {"u1": ok("u1", text)}, 4)
        assert result.rules == 1
        assert result.diag.dropped == 1  # 丢弃可见，不是暗箱

    def test_aggregate_only_when_requested(self):
        text = "1.1.1.0/25\n1.1.1.128/25\n"
        plain = Ruleset("x", ("u1",), Format.CIDR)
        agg = Ruleset("y", ("u1",), Format.CIDR, aggregate=True)
        assert cli.build(plain, {"u1": ok("u1", text)}, 4)[0].rules == 2
        assert cli.build(agg, {"u1": ok("u1", text)}, 4)[0].rules == 1


class TestPipeline:
    """一个规则集失败绝不影响其他规则集 —— 旧实现在这里会整体中止。"""

    def _config(self, tmp_path):
        return {
            "schema": 1,
            "ruleset_version": 4,
            "sing_box": {"version": "1.13.14", "platform": "linux-amd64"},
            "output": {
                "json_dir": str(tmp_path / "json"),
                "srs_dir": str(tmp_path / "srs"),
            },
            "rulesets": {
                "good": ["https://x/good.json"],
                "broken": ["https://x/broken.json"],
                "gone": ["https://x/gone.json"],
            },
        }

    def _patch_fetch(self, monkeypatch):
        responses = {
            "https://x/good.json": ok("https://x/good.json", SINGBOX),
            "https://x/broken.json": ok("https://x/broken.json", "not json at all"),
            "https://x/gone.json": Fetched("https://x/gone.json", error="HTTP 404"),
        }
        monkeypatch.setattr(cli, "fetch_all", lambda urls, **kw: responses)

    def test_survivors_are_still_written(self, tmp_path, monkeypatch, capsys):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(self._config(tmp_path)), encoding="utf-8")
        self._patch_fetch(monkeypatch)

        code = cli.main(["-c", str(path), "--dry-run", "-q"])

        assert code == 0  # 有产出就算成功
        assert (tmp_path / "json" / "good.json").is_file()
        assert not (tmp_path / "json" / "broken.json").exists()
        out = capsys.readouterr().out
        assert "✓ good" in out and "✗ broken" in out and "✗ gone" in out
        assert "生成 1/3" in out

    def test_strict_flag_fails_the_run(self, tmp_path, monkeypatch):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(self._config(tmp_path)), encoding="utf-8")
        self._patch_fetch(monkeypatch)
        assert cli.main(["-c", str(path), "--dry-run", "-q", "--strict"]) == 1

    def test_only_selects_a_subset(self, tmp_path, monkeypatch):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(self._config(tmp_path)), encoding="utf-8")
        self._patch_fetch(monkeypatch)
        assert cli.main(["-c", str(path), "--dry-run", "-q", "--only", "good"]) == 0
        assert not (tmp_path / "json" / "broken.json").exists()

    def test_unknown_only_name_is_rejected(self, tmp_path, monkeypatch, capsys):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(self._config(tmp_path)), encoding="utf-8")
        self._patch_fetch(monkeypatch)
        assert cli.main(["-c", str(path), "--dry-run", "-q", "--only", "nope"]) == 2
        assert "nope" in capsys.readouterr().err


class TestSummary:
    def test_summary_goes_to_stdout_regardless_of_log_level(self, capsys):
        # 旧实现的摘要走 logger.info，配置里 level=SUCCESS 会把它整体吃掉
        cli.setup_logging(verbose=0, quiet=True)
        summarize([Result(name="a", ok=True, rules=3)], compiled=False)
        assert "✓ a" in capsys.readouterr().out

    def test_failure_reason_is_shown(self, capsys):
        summarize([Result(name="a", error="炸了")], compiled=False)
        out = capsys.readouterr().out
        assert "✗ a" in out and "炸了" in out


class TestRunReport:
    """发布流程靠这份报告区分「源挂了」和「配置里删了」。"""

    def _report(self, results, configured, selected):
        return run_report(results, configured=configured, selected=selected)

    def test_status_per_ruleset(self):
        report = self._report(
            [Result(name="a", ok=True, rules=7), Result(name="b", error="炸了")],
            ["a", "b"],
            {"a", "b"},
        )
        assert report["counts"] == {
            "ok": 1,
            "failed": 1,
            "skipped": 0,
            "configured": 2,
        }
        by_name = {e["name"]: e for e in report["rulesets"]}
        assert by_name["a"]["status"] == "ok" and by_name["a"]["rules"] == 7
        assert by_name["b"]["status"] == "failed" and by_name["b"]["error"] == "炸了"

    def test_unselected_rulesets_are_skipped_not_failed(self):
        # --only 跑一个规则集时，其余的必须是 skipped：
        # 发布方要像对待失败项一样保留它们，否则一次 --only 就删光其余全部
        report = self._report([Result(name="a", ok=True)], ["a", "b", "c"], {"a"})
        statuses = {e["name"]: e["status"] for e in report["rulesets"]}
        assert statuses == {"a": "ok", "b": "skipped", "c": "skipped"}
        assert report["counts"]["failed"] == 0

    def test_configured_set_drives_the_report_not_the_results(self):
        # 配置里删掉的规则集不会出现在报告里 —— 发布方据此判定孤儿
        report = self._report([Result(name="gone", ok=True)], ["kept"], {"kept"})
        assert [e["name"] for e in report["rulesets"]] == ["kept"]

    def test_failed_sources_are_carried_through(self):
        result = Result(name="a", ok=True, rules=1, failed_sources=["u2 (HTTP 502)"])
        report = self._report([result], ["a"], {"a"})
        assert report["rulesets"][0]["failed_sources"] == ["u2 (HTTP 502)"]

    def test_written_to_disk_on_request(self, tmp_path, monkeypatch, capsys):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(TestPipeline()._config(tmp_path)), encoding="utf-8")
        TestPipeline()._patch_fetch(monkeypatch)
        target = tmp_path / "nested" / "run-report.json"

        code = cli.main(
            ["-c", str(path), "--dry-run", "-q", "--report-json", str(target)]
        )

        assert code == 0
        payload = json.loads(target.read_text(encoding="utf-8"))
        assert payload["schema"] == 1
        assert payload["counts"] == {
            "ok": 1,
            "failed": 2,
            "skipped": 0,
            "configured": 3,
        }
        capsys.readouterr()
