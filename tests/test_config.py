import json

import pytest

from srsbox.config import load
from srsbox.errors import ConfigError
from srsbox.parse import Format

BASE = {
    "schema": 1,
    "ruleset_version": 4,
    "sing_box": {"version": "1.13.14", "platform": "linux-amd64"},
}


def write(tmp_path, **overrides):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({**BASE, **overrides}), encoding="utf-8")
    return path


def names(cfg):
    return sorted(r.name for r in cfg.rulesets)


class TestShorthands:
    def test_array_shorthand(self, tmp_path):
        cfg = load(write(tmp_path, rulesets={"a": ["https://x/a.json"]}))
        (ruleset,) = cfg.rulesets
        assert ruleset.sources == ("https://x/a.json",)
        assert ruleset.format is Format.SINGBOX  # 默认就是 sing-box，无需声明

    def test_string_shorthand(self, tmp_path):
        cfg = load(write(tmp_path, rulesets={"a": "https://x/a.json"}))
        assert cfg.rulesets[0].sources == ("https://x/a.json",)

    def test_base_is_prefixed_to_sources(self, tmp_path):
        cfg = load(
            write(
                tmp_path,
                rulesets={"a": {"base": "https://x/d/", "sources": ["1.txt", "2.txt"]}},
            )
        )
        assert cfg.rulesets[0].sources == ("https://x/d/1.txt", "https://x/d/2.txt")


class TestGroups:
    def test_key_becomes_name_prefix(self, tmp_path):
        cfg = load(
            write(
                tmp_path,
                rulesets={
                    "skk": {
                        "base": "https://s/",
                        "items": {"ai": "non_ip/ai.json", "lan": "non_ip/lan.json"},
                    }
                },
            )
        )
        assert names(cfg) == ["skk-ai", "skk-lan"]
        assert cfg.rulesets[0].sources == ("https://s/non_ip/ai.json",)

    def test_multi_source_item(self, tmp_path):
        cfg = load(
            write(
                tmp_path,
                rulesets={
                    "skk": {"base": "https://s/", "items": {"x": ["a.json", "b.json"]}}
                },
            )
        )
        assert cfg.rulesets[0].sources == ("https://s/a.json", "https://s/b.json")

    def test_prefix_can_be_overridden(self, tmp_path):
        cfg = load(
            write(
                tmp_path,
                rulesets={
                    "g": {
                        "base": "https://s/",
                        "prefix": "",
                        "items": {"flat": "a.json"},
                    }
                },
            )
        )
        assert names(cfg) == ["flat"]

    def test_group_format_applies_to_every_item(self, tmp_path):
        cfg = load(
            write(
                tmp_path,
                rulesets={
                    "g": {
                        "base": "https://s/",
                        "format": "cidr",
                        "items": {"a": "a.txt", "b": "b.txt"},
                    }
                },
            )
        )
        assert {r.format for r in cfg.rulesets} == {Format.CIDR}

    def test_items_and_sources_are_mutually_exclusive(self, tmp_path):
        with pytest.raises(ConfigError, match="不能同时"):
            load(
                write(
                    tmp_path,
                    rulesets={
                        "g": {"items": {"a": "https://s/a"}, "sources": ["https://s/b"]}
                    },
                )
            )


class TestValidation:
    def test_duplicate_name_across_group_and_single_is_rejected(self, tmp_path):
        # 旧配置的三个分区共用输出文件名却互不知情，重名会静默覆盖
        with pytest.raises(ConfigError, match="重名"):
            load(
                write(
                    tmp_path,
                    rulesets={
                        "g": {"base": "https://s/", "items": {"a": "a.json"}},
                        "g-a": ["https://other/x.json"],
                    },
                )
            )

    @pytest.mark.parametrize("bad", ["../escape", "with/slash", ".hidden", "-lead"])
    def test_unsafe_names_rejected(self, tmp_path, bad):
        # 名字会直接变成输出文件名
        with pytest.raises(ConfigError, match="非法"):
            load(write(tmp_path, rulesets={bad: ["https://x/a.json"]}))

    @pytest.mark.parametrize("bad", ["file:///etc/passwd", "ftp://x/a", "/local/path"])
    def test_non_http_urls_rejected(self, tmp_path, bad):
        with pytest.raises(ConfigError, match="http"):
            load(write(tmp_path, rulesets={"a": [bad]}))

    def test_unknown_format_lists_the_valid_ones(self, tmp_path):
        with pytest.raises(ConfigError, match="可选"):
            load(
                write(
                    tmp_path,
                    rulesets={"a": {"format": "clash", "sources": ["https://x/a"]}},
                )
            )

    def test_schema_mismatch_mentions_migration(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text(
            json.dumps({"ip_only": {}, "rulesets": {}, "version": 4}), encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="迁移"):
            load(path)

    def test_missing_sing_box(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text(
            json.dumps(
                {"schema": 1, "ruleset_version": 4, "rulesets": {"a": ["https://x/a"]}}
            ),
            encoding="utf-8",
        )
        with pytest.raises(ConfigError, match="sing_box"):
            load(path)

    def test_empty_rulesets(self, tmp_path):
        with pytest.raises(ConfigError, match="不能为空"):
            load(write(tmp_path, rulesets={}))

    def test_bad_json_reports_position(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text('{"schema": 1,,}', encoding="utf-8")
        with pytest.raises(ConfigError, match="行 1"):
            load(path)

    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigError, match="不存在"):
            load(tmp_path / "nope.json")


class TestRealConfig:
    def test_shipped_config_loads(self):
        cfg = load("config.json")
        assert len(cfg.rulesets) == 52
        assert len({r.name for r in cfg.rulesets}) == 52
        assert sum(len(r.sources) for r in cfg.rulesets) == 61
        # 只有 5 个需要声明 format，其余走默认
        declared = [r.name for r in cfg.rulesets if r.format is not Format.SINGBOX]
        assert sorted(declared) == [
            "block-dns",
            "cn-ip",
            "normal-ai",
            "riot",
            "telegram-ip",
        ]
