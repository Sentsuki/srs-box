"""规则类型词汇表 —— 纯数据，不含任何逻辑。

设计要点：查表前一律 ``.upper()``，所以 ``DOMAIN-SUFFIX`` / ``domain-suffix`` /
``Host-Suffix`` 自动命中同一条，不必为每种大小写写一行。

Clash / Surge / Quantumult X 的规则行在丢掉策略列之后是同一种东西，差异只体现在
这张表的词汇上，因此不存在"方言"这个概念 —— 支持新方言 = 往表里加行。
"""

# 规则类型 -> sing-box headless rule 字段
TYPES: dict[str, str] = {
    "DOMAIN": "domain",
    "HOST": "domain",
    "DOMAIN-SUFFIX": "domain_suffix",
    "HOST-SUFFIX": "domain_suffix",
    "DOMAIN-KEYWORD": "domain_keyword",
    "HOST-KEYWORD": "domain_keyword",
    "DOMAIN-REGEX": "domain_regex",
    "HOST-REGEX": "domain_regex",
    "IP-CIDR": "ip_cidr",
    "IP-CIDR6": "ip_cidr",
    "IP6-CIDR": "ip_cidr",
    "SRC-IP-CIDR": "source_ip_cidr",
    "SOURCE-IP-CIDR": "source_ip_cidr",
    "DST-PORT": "port",
    "PORT": "port",
    "SRC-PORT": "source_port",
    "SOURCE-PORT": "source_port",
    "PROCESS-NAME": "process_name",
    "PROCESS-PATH": "process_path",
    "PACKAGE-NAME": "package_name",
    "NETWORK": "network",
}

# 认识，但 sing-box headless rule 表达不了 —— 跳过并计数，绝不静默丢弃。
#
# 注意 GEOIP 和 URL-REGEX：旧实现把它们分别映射成了根本不存在的 ``geoip`` 字段
# 和语义不符的 ``domain_regex``，会产出 sing-box 拒绝或永不命中的规则。
SKIP: frozenset[str] = frozenset(
    {
        "GEOIP",
        "GEOSITE",
        "IP-ASN",
        "SRC-IP-ASN",
        "IP-SUFFIX",
        "SRC-IP-SUFFIX",
        "URL-REGEX",
        "USER-AGENT",
        "HEADER",
        "SCRIPT",
        "RULE-SET",
        "SUB-RULE",
        "MATCH",
        "FINAL",
        "DSCP",
        "UID",
        "IN-TYPE",
        "IN-USER",
        "IN-NAME",
        "IN-PORT",
        "PROCESS-PATH-REGEX",
        "PROCESS-NAME-REGEX",
        "AND-SET",
    }
)

# 逻辑规则关键字 -> (sing-box mode, 是否取反)
#
# sing-box 的 logical rule 只有 and / or 两种 mode，NOT 通过 invert 表达。
LOGICAL: dict[str, tuple[str, bool]] = {
    "AND": ("and", False),
    "OR": ("or", False),
    "NOT": ("and", True),
}
