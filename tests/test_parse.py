import json

import pytest

from srsbox.errors import ParseError
from srsbox.models import RuleSet
from srsbox.parse import Format, parse


def run(text: str, fmt: Format = Format.TEXT) -> RuleSet:
    rs = RuleSet("t", 4)
    parse(text, fmt, rs)
    return rs


class TestTypedEntries:
    def test_basic_clash_line(self):
        rs = run("DOMAIN-SUFFIX,example.com")
        assert rs.to_json()["rules"] == [{"domain_suffix": ["example.com"]}]

    def test_policy_column_is_dropped(self):
        # Clash/Surge/QX 的差异几乎都在第三列，丢掉之后三者就是同一种东西
        rs = run("DOMAIN,a.com,PROXY\nIP-CIDR,1.2.3.0/24,DIRECT,no-resolve")
        assert rs.counts() == {"domain": 1, "ip_cidr": 1}

    def test_type_matching_is_case_insensitive(self):
        rs = run("host-suffix,a.com\nHOST-SUFFIX,b.com\nDomain-Suffix,c.com")
        assert rs.counts() == {"domain_suffix": 3}

    def test_ports_become_integers(self):
        rs = run("DST-PORT,443\nSRC-PORT,8080")
        payload = json.dumps(rs.to_json())
        assert '"port": [443]' in payload
        assert '"source_port": [8080]' in payload

    def test_geoip_is_skipped_not_mistranslated(self):
        # 旧实现把 GEOIP 映射成根本不存在的 geoip 字段，sing-box 会拒绝编译
        rs = run("GEOIP,CN\nDOMAIN,a.com")
        assert rs.counts() == {"domain": 1}
        assert rs.diag.skipped["GEOIP"] == 1

    def test_url_regex_is_skipped_not_mapped_to_domain_regex(self):
        rs = run("URL-REGEX,^https?://ads\\.")
        assert rs.total == 0
        assert rs.diag.skipped["URL-REGEX"] == 1

    def test_unknown_type_is_counted_not_silently_dropped(self):
        rs = run("TOTALLY-MADE-UP,value")
        assert rs.total == 0
        assert rs.diag.unknown["TOTALLY-MADE-UP"] == 1

    def test_inline_comment_stripped_but_not_inside_regex(self):
        rs = run("DOMAIN,a.com  # 说明")
        assert rs.plain["domain"] == {"a.com"}
        rs = run(r"DOMAIN-REGEX,^a#b\.com$")
        assert rs.plain["domain_regex"] == {r"^a#b\.com$"}


class TestBareEntries:
    def test_kind_decided_per_entry_not_per_file(self):
        # 同一文件里混写 IP 和域名也能正确处理
        rs = run("1.2.3.0/24\nexample.com\n.cdn.example.com\n2001:db8::/32")
        assert rs.counts() == {"domain": 1, "domain_suffix": 1, "ip_cidr": 2}

    def test_bare_ip_gets_mask(self):
        assert run("8.8.8.8").plain["ip_cidr"] == {"8.8.8.8/32"}

    def test_clash_plus_prefix_means_suffix(self):
        rs = run("+.example.com")
        assert rs.plain["domain_suffix"] == {"example.com"}

    def test_comments_and_blanks_ignored(self):
        rs = run("# c\n\n; c2\n// c3\na.com\n")
        assert rs.counts() == {"domain": 1}

    def test_garbage_recorded_as_invalid(self):
        rs = run("<html><body>404</body></html>")
        assert rs.total == 0
        assert rs.diag.invalid_total == 1


class TestLogical:
    def test_and_rule(self):
        rs = run("AND,((DOMAIN,a.com),(DST-PORT,443))")
        assert rs.to_json()["rules"] == [
            {
                "type": "logical",
                "mode": "and",
                "rules": [{"domain": ["a.com"]}, {"port": ["443"]}],
            }
        ]

    def test_not_becomes_invert(self):
        rs = run("NOT,((DOMAIN,a.com))")
        rule = rs.to_json()["rules"][0]
        assert rule["mode"] == "and" and rule["invert"] is True


class TestSingbox:
    def test_merges_and_dedups_across_sources(self):
        rs = RuleSet("t", 4)
        a = json.dumps({"version": 2, "rules": [{"domain": ["a.com", "b.com"]}]})
        b = json.dumps({"version": 3, "rules": [{"domain": ["b.com", "c.com"]}]})
        parse(a, Format.SINGBOX, rs)
        parse(b, Format.SINGBOX, rs)
        assert rs.to_json() == {
            "version": 4,  # 用配置的版本覆盖源的版本
            "rules": [{"domain": ["a.com", "b.com", "c.com"]}],
        }

    def test_logical_rule_passes_through(self):
        rule = {"type": "logical", "mode": "or", "rules": [{"domain": ["a.com"]}]}
        rs = run(json.dumps({"rules": [rule]}), Format.SINGBOX)
        assert rs.to_json()["rules"] == [rule]

    def test_unknown_field_passes_through_instead_of_being_dropped(self):
        rule = {"domain": ["a.com"], "wifi_ssid": ["home"]}
        rs = run(json.dumps({"rules": [rule]}), Format.SINGBOX)
        assert rs.to_json()["rules"] == [rule]

    def test_wrong_format_error_is_actionable(self):
        with pytest.raises(ParseError) as excinfo:
            run("DOMAIN-SUFFIX,example.com", Format.SINGBOX)
        message = str(excinfo.value)
        assert "期望 sing-box JSON" in message
        assert '"format": "text"' in message

    def test_missing_rules_array(self):
        with pytest.raises(ParseError, match="缺少 rules"):
            run('{"version": 3}', Format.SINGBOX)


class TestYaml:
    def test_payload_mapping(self):
        rs = run("payload:\n  - DOMAIN,a.com\n  - IP-CIDR,1.2.3.0/24\n", Format.YAML)
        assert rs.counts() == {"domain": 1, "ip_cidr": 1}

    def test_other_key_names_work(self):
        # payload 不是 YAML 的普遍特征，rules 等键名同样常见
        rs = run("rules:\n  - DOMAIN,a.com\n", Format.YAML)
        assert rs.counts() == {"domain": 1}

    def test_sole_list_value_is_used(self):
        rs = run("whatever:\n  - DOMAIN,a.com\n", Format.YAML)
        assert rs.counts() == {"domain": 1}

    def test_bare_sequence(self):
        rs = run("- DOMAIN,a.com\n- DOMAIN,b.com\n", Format.YAML)
        assert rs.counts() == {"domain": 2}

    def test_mapping_entries(self):
        rs = run('payload:\n  - {"DOMAIN-SUFFIX": ["a.com", "b.com"]}\n', Format.YAML)
        assert rs.counts() == {"domain_suffix": 2}

    def test_no_sequence_found_raises_instead_of_silent_empty(self):
        # 旧实现在这里静默返回 []，整个规则集变成 0 条且无任何提示
        with pytest.raises(ParseError, match="找不到规则序列"):
            run("a: 1\nb: 2\n", Format.YAML)

    def test_yaml_sequence_still_parses_when_declared_as_text(self):
        # 格式声明错了，在扁平列表这种绝大多数情形下只降级不崩溃
        rs = run("- DOMAIN,a.com\n- DOMAIN,b.com\n", Format.TEXT)
        assert rs.counts() == {"domain": 2}


class TestStrictFormats:
    def test_cidr_accepts_ip_list(self):
        rs = run("1.2.3.0/24\n# c\n2001:db8::/32\n", Format.CIDR)
        assert rs.counts() == {"ip_cidr": 2}

    def test_cidr_rejects_html_error_page(self):
        # 上游返回 HTTP 200 的 HTML 错误页时，下载层发现不了，严格模式当场暴露
        with pytest.raises(ParseError):
            run("<html><body>Not Found</body></html>", Format.CIDR)

    def test_cidr_rejects_domains(self):
        with pytest.raises(ParseError, match="严格格式不允许"):
            run("1.2.3.0/24\nexample.com\n", Format.CIDR)

    def test_domainset_accepts_domains_and_suffixes(self):
        rs = run("example.com\n.cdn.example.com\n+.other.com\n", Format.DOMAINSET)
        assert rs.counts() == {"domain": 1, "domain_suffix": 2}

    def test_domainset_rejects_ips(self):
        with pytest.raises(ParseError, match="严格格式不允许"):
            run("1.2.3.4\n", Format.DOMAINSET)
