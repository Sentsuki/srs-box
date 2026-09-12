# 配置说明

配置文件默认位于项目根目录 `config.json`，可通过 `-c` 指定路径。

```json
{
  "schema": 1,
  "ruleset_version": 4,
  "sing_box": { "version": "1.13.14", "platform": "linux-amd64" },
  "output": { "json_dir": "output/json", "srs_dir": "output/srs" },
  "fetch": { "concurrency": 8, "timeout": 30, "retries": 3 },
  "rulesets": { }
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `schema` | 是 | 配置文件格式版本，固定为 `1` |
| `ruleset_version` | 是 | 目标 sing-box rule-set 版本 |
| `sing_box.version` | 是 | 编译所用的 sing-box 版本（对应 GitHub Releases） |
| `sing_box.platform` | 是 | 平台架构，如 `linux-amd64`、`windows-amd64`、`darwin-arm64` |
| `sing_box.sha256` | 否 | 固定二进制的 SHA-256，详见下文 |
| `output.*` | 否 | 输出目录，默认 `output/json` 与 `output/srs` |
| `fetch.*` | 否 | 网络配置，默认并发 8、超时 30 秒、重试 3 次 |
| `rulesets` | 是 | 规则集定义 |

## sing_box.sha256

可选。填入 sing-box 二进制的 SHA-256（64 位十六进制），之后每次下载完成、
以及每次命中本地缓存时都会比对，不匹配就拒绝执行。

SagerNet 的 release 不发布校验和文件，因此没有可自动比对的官方摘要 —— 这里是
**自钉**：先正常跑一次拿到摘要，确认无误后写进配置，此后缓存被替换或下载被改写
都会被挡下。不填则跳过校验，行为与从前一致。

```bash
# 取得当前使用的二进制摘要
sha256sum .cache/sing-box/<版本>-<平台>/sing-box
```

```json
"sing_box": {
  "version": "1.14.0",
  "platform": "linux-amd64",
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

## rulesets

键名即输出文件名。同名下的多个源会自动合并去重。

### 单个规则集

简写（仅需指定 URL 或 URL 列表）：

```json
"block-ads": ["https://example.com/ads.json"],
"skk-reject": [
  "https://ruleset.skk.moe/sing-box/domainset/reject.json",
  "https://ruleset.skk.moe/sing-box/non_ip/reject.json"
]
```

完整对象配置：

```json
"cn-ip": {
  "format": "cidr",
  "aggregate": true,
  "base": "https://example.com/ip-lists/",
  "sources": ["chinanet46.txt", "cmcc46.txt"]
}
```

| 键 | 说明 |
| --- | --- |
| `sources` | URL 或 URL 数组（若指定 `base` 则可写相对路径），必填 |
| `base` | 公共 URL 前缀 |
| `format` | 规则格式，默认 `singbox`，详见下文 |
| `aggregate` | 是否合并相邻/包含的 CIDR（仅对 IP 生效），默认 `false` |

### 规则集分组

通过 `items` 将共享前缀或公共配置的规则集归组。组名自动作为输出文件名前缀：

```json
"skk": {
  "base": "https://ruleset.skk.moe/sing-box/",
  "items": {
    "ai": "non_ip/ai.json",
    "stream-jp": "non_ip/stream_jp.json",
    "reject": ["domainset/reject.json", "non_ip/reject.json"]
  }
}
```

展开为 `skk-ai`、`skk-stream-jp`、`skk-reject`。如需自定义前缀或无需前缀，可指定 `"prefix": ""`。

## format

指定源文件的容器格式，默认 `singbox`。

| 取值 | 格式 | 适用场景 |
| --- | --- | --- |
| `singbox` | JSON | sing-box rule-set（默认） |
| `text` | 纯文本行 | Clash / Surge / Quantumult X 的 `.list` 规则或纯文本行 |
| `yaml` | YAML | 含 `payload` / `rules` 列表的 YAML 配置 |
| `cidr` | 纯文本行 | 严格模式：仅允许 IP/CIDR，混入其他内容即报错 |
| `domainset` | 纯文本行 | 严格模式：仅允许域名，混入其他内容即报错 |

- `text` 与 `yaml` 支持自动识别：带策略类型的规则（如 `DOMAIN-SUFFIX,example.com,PROXY`）、裸 IP/CIDR（如 `1.2.3.0/24`）以及裸域名（如 `+.example.com`）。
- 不支持的规则类型（如 `GEOIP`、`IP-ASN`、`USER-AGENT` 等）会自动跳过并计入执行摘要。

## 命令行

```bash
srsbox [-c CONFIG] [--only NAME] [-n] [--strict] [--sing-box PATH] [-v|-q]
```

| 参数 | 说明 |
| --- | --- |
| `-c, --config` | 配置文件路径，默认 `config.json` |
| `--only NAME` | 仅处理指定规则集（支持多次指定） |
| `-n, --dry-run` | 仅生成 JSON，跳过 sing-box 下载与编译 |
| `--strict` | 任一规则集失败即退出码非零 |
| `--sing-box PATH` | 使用本地二进制路径，跳过自动下载（也可使用环境变量 `SING_BOX_BIN`） |
| `--report-json PATH` | 写出机器可读的运行报告，供发布流程判断保留/清理 |
| `-v` / `-vv` | 详细日志（`-vv` 显示 HTTP 抓取详情） |
| `-q` | 静默模式，仅输出警告和错误 |

**退出码**：`0` 至少一个规则集成功；`1` 全部失败或 `--strict` 下存在失败；`2` 配置/参数错误；`130` 用户中断。

## 常见问题

- **报错期望 sing-box JSON**：源文件非 JSON 格式，按内容格式显式声明 `"format": "text"` 或 `"yaml"`。
- **避免重复下载 sing-box**：二进制会缓存在 `.cache/sing-box/<版本>-<平台>/`。CI 中可缓存该目录，或通过 `--sing-box` / `SING_BOX_BIN` 指定预装路径。
- **校验和不匹配**：缓存里的二进制与 `sing_box.sha256` 不符。若是你有意升级了版本，更新配置里的摘要；否则删除缓存目录重新下载。
- **单个规则集失败**：默认各规则集互不影响，其余规则集正常生成与编译。
