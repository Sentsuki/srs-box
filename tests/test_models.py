import json

import pytest

from srsbox.errors import InvalidValue
from srsbox.models import RuleSet, normalize


def rs() -> RuleSet:
    return RuleSet("t", 4)


class TestNormalize:
    def test_port_becomes_int(self):
        assert normalize("port", "443") == 443

    @pytest.mark.parametrize("bad", ["http", "-1", "65536", ""])
    def test_bad_port_rejected(self, bad):
        with pytest.raises(InvalidValue):
            normalize("port", bad)

    def test_bare_ip_gets_mask(self):
        assert normalize("ip_cidr", "1.2.3.4") == "1.2.3.4/32"
        assert normalize("ip_cidr", "2001:db8::1") == "2001:db8::1/128"

    def test_host_bits_are_normalized(self):
        assert normalize("ip_cidr", "1.2.3.4/24") == "1.2.3.0/24"

    @pytest.mark.parametrize("bad", ["<html>", "1.2.3.4/99", "999.1.1.1", ""])
    def test_bad_cidr_rejected(self, bad):
        with pytest.raises(InvalidValue):
            normalize("ip_cidr", bad)

    def test_domain_lowercased_and_root_dot_stripped(self):
        assert normalize("domain", "Example.COM.") == "example.com"

    def test_leading_dot_preserved_on_suffix(self):
        assert normalize("domain_suffix", ".Example.com") == ".example.com"

    def test_idn_converted_to_punycode(self):
        assert normalize("domain", "中文.example.com") == "xn--fiq228c.example.com"

    @pytest.mark.parametrize("bad", ["", "  ", "a..b", "-bad.com", "x" * 300])
    def test_bad_domain_rejected(self, bad):
        with pytest.raises(InvalidValue):
            normalize("domain", bad)

    def test_regex_case_is_preserved(self):
        # 正则大小写敏感，不能跟域名一样 lower()
        assert normalize("domain_regex", "^AdS?\\.") == "^AdS?\\."

    @pytest.mark.parametrize(
        "field,value",
        [
            ("process_name", "Telegram"),
            ("process_name", "WeChat.exe"),
            ("process_path", "/Applications/Surge.app/Contents/MacOS/Surge"),
            ("package_name", "com.Example.App"),
        ],
    )
    def test_process_and_package_case_is_preserved(self, field, value):
        # Linux/macOS 上进程名与路径大小写敏感，压成小写等于让规则永不命中
        assert normalize(field, value) == value

    @pytest.mark.parametrize("field", ["domain", "domain_suffix"])
    def test_wildcards_rejected_in_host_fields(self, field):
        # sing-box 的 domain/domain_suffix 不认通配符，写进去就是永不命中的死规则；
        # 通配符域名由解析层转成 domain_regex，到不了这里
        with pytest.raises(InvalidValue):
            normalize(field, "*.example.com")

    def test_unknown_field_rejected(self):
        with pytest.raises(InvalidValue):
            normalize("geoip", "CN")


class TestRuleSet:
    def test_dedup_and_sort(self):
        r = rs()
        for d in ("b.com", "a.com", "b.com"):
            r.add("domain", d)
        assert r.to_json()["rules"] == [{"domain": ["a.com", "b.com"]}]

    def test_output_field_order_is_stable(self):
        r = rs()
        r.add("port", 443)
        r.add("domain", "a.com")
        r.add("ip_cidr", "1.1.1.0/24")
        fields = [next(iter(rule)) for rule in r.to_json()["rules"]]
        assert fields == ["domain", "ip_cidr", "port"]

    def test_regex_survives_json_roundtrip(self):
        # 旧实现对序列化结果做 replace("\\\\", "\\")，任何含反斜杠的正则
        # 都会让产物变成非法 JSON。
        r = rs()
        r.add("domain_regex", r"^ad\..*\.com$")
        text = json.dumps(r.to_json(), ensure_ascii=False)
        assert json.loads(text)["rules"][0]["domain_regex"] == [r"^ad\..*\.com$"]

    def test_add_lenient_records_instead_of_raising(self):
        r = rs()
        assert r.add_lenient("ip_cidr", "<html>") is False
        assert r.diag.invalid_total == 1
        assert r.diag.invalid_samples

    def test_verbatim_rules_pass_through_untouched(self):
        r = rs()
        rule = {"type": "logical", "mode": "or", "rules": [{"domain": ["a.com"]}]}
        r.add_verbatim(rule)
        assert r.to_json()["rules"] == [rule]

    def test_invert_rule_is_not_merged(self):
        # 带 invert 的规则语义是整条取反，拆开与别的值混在一起会改变语义
        r = rs()
        assert r.mergeable({"domain": ["a.com"]}) is True
        assert r.mergeable({"domain": ["a.com"], "invert": True}) is False
        assert r.mergeable({"domain": ["a.com"], "wifi_ssid": ["x"]}) is False

    def test_drop_values_containing(self):
        r = rs()
        r.add("domain", "keep.com")
        r.add("domain_suffix", "tracker.skk.moe")
        assert r.drop_values_containing(["SKK.moe"]) == 1
        assert r.to_json()["rules"] == [{"domain": ["keep.com"]}]
        assert r.diag.dropped == 1

    def test_drop_skips_int_fields(self):
        r = rs()
        r.add("port", 443)
        assert r.drop_values_containing(["44"]) == 0

    def test_aggregate_merges_adjacent_networks(self):
        r = rs()
        for net in ("10.0.0.0/25", "10.0.0.128/25", "192.168.1.0/24"):
            r.add("ip_cidr", net)
        assert r.aggregate_cidr() == 1
        assert sorted(r.plain["ip_cidr"]) == ["10.0.0.0/24", "192.168.1.0/24"]

    def test_aggregate_keeps_v4_and_v6_separate(self):
        r = rs()
        r.add("ip_cidr", "10.0.0.0/8")
        r.add("ip_cidr", "2001:db8::/32")
        assert r.aggregate_cidr() == 0
        assert len(r.plain["ip_cidr"]) == 2

    def test_output_is_byte_identical_across_runs(self):
        def build():
            r = rs()
            for d in ("z.com", "a.com", "m.com"):
                r.add("domain", d)
            return json.dumps(r.to_json(), ensure_ascii=False, indent=2)

        assert build() == build()
