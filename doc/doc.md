# 配置说明

配置文件默认是项目根目录的 `config.json`，用 `-c` 可以指定别的路径。

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
| `schema` | 是 | 配置文件自身的格式版本，当前为 `1` |
| `ruleset_version` | 是 | 写进产物 JSON 的 sing-box rule-set 版本 |
| `sing_box.version` | 是 | 用哪个版本的 sing-box 编译，需存在于 GitHub Releases |
| `sing_box.platform` | 是 | 如 `linux-amd64`、`windows-amd64`、`darwin-arm64` |
| `output.*` | 否 | 默认 `output/json` 和 `output/srs` |
| `fetch.*` | 否 | 默认并发 8、超时 30 秒、重试 3 次 |
| `rulesets` | 是 | 见下 |

> 旧版的三段式配置（`ip_only` / `rulesets` / `convert`）和 `logging` 段已经取消。
> 日志详细程度改用命令行的 `-v` / `-q`；执行摘要始终输出，不受日志级别影响。

## rulesets

`rulesets` 是唯一的命名空间，键名直接作为输出文件名。条目有两种形态，靠有没有
`items` 区分。

### 单个规则集

最简写法是一个 URL 数组，同名下的多个源会被合并去重：

```json
"block-ads": ["https://example.com/ads.json"],
"skk-reject": [
  "https://ruleset.skk.moe/sing-box/domainset/reject.json",
  "https://ruleset.skk.moe/sing-box/non_ip/reject.json"
]
```

需要额外选项时写成对象：

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
| `sources` | URL 或 URL 数组（`base` 存在时是相对路径），必填 |
| `base` | 拼在每个 source 前面的公共前缀 |
| `format` | 见下，默认 `singbox` |
| `aggregate` | 合并相邻/包含的 CIDR，默认 `false` |

### 一组规则集

共享同一个站点的多个规则集可以折成一组。键名自动作为名字前缀，`base` 作为 URL
前缀，`format` / `aggregate` 作为组内默认值：

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

展开成 `skk-ai`、`skk-stream-jp`、`skk-reject` 三个规则集。`items` 的值是字符串或
字符串数组。想要别的前缀（包括不要前缀）就写 `"prefix": ""`。

展开之后会做一次全局重名检测，撞名直接报错——名字就是输出文件名，静默覆盖比报错
危险得多。

## format

**默认是 `singbox`，绝大多数源不需要写这一项。** 本项目不做格式嗅探：声明错了会当场
报错并提示怎么改，而不是猜一个然后产出垃圾。

| 取值 | 容器 | 适用 |
| --- | --- | --- |
| `singbox` | JSON | sing-box rule-set，**默认** |
| `text` | 逐行 | Clash / Surge / Quantumult X 的 `.list`，也吃裸值 |
| `yaml` | YAML | `payload:` 之类含规则序列的 YAML |
| `cidr` | 逐行 | 纯 IP/CIDR 列表，**严格**：出现别的内容直接报错 |
| `domainset` | 逐行 | 纯域名列表，**严格**：出现别的内容直接报错 |

注意 `format` 描述的是**容器格式**，不是"哪家软件的方言"。Clash、Surge、
Quantumult X 的规则行在丢掉策略列之后是同一种东西，差异只在规则类型词汇上，由
`srsbox/vocab.py` 一张表统一吸收，所以都用 `text`。

`text` 和 `yaml` 里每个条目的处理方式是逐条判断的：

- `DOMAIN-SUFFIX,example.com,PROXY` —— 有类型，按类型映射，策略列丢弃；
- `1.2.3.0/24` —— 裸值，按值本身的形态判定为 IP；
- `.cdn.example.com` / `+.example.com` —— 裸值，判定为域名后缀。

所以同一个文件里混写 IP 和域名也没问题，不需要事先给整个文件归类。

### 严格格式的用处

`cidr` 和 `domainset` 会拒绝白名单之外的内容。从第三方仓库拉的 IP 列表最容易悄悄
腐坏——上游返回一个 HTTP 200 的 HTML 错误页，下载层完全发现不了，宽容模式会把这
堆 HTML 记成一批非法值然后继续，严格模式会让这个规则集当场失败。给关键的 IP 源
用严格格式更安全。

### 不支持的规则类型

`GEOIP`、`GEOSITE`、`IP-ASN`、`URL-REGEX`、`USER-AGENT` 等在 sing-box 的 headless
rule 里没有对应表达，会被跳过并按类型计入摘要：

```
  ✓ normal-ai        589 条   跳过 IP-ASN×2, USER-AGENT×1
```

不认识的类型（拼错、新方言）单独计入"未知类型"，同样会出现在摘要里。两者都不会
被静默丢弃。

## 命令行

```
srsbox [-c CONFIG] [--only NAME] [-n] [--strict] [--sing-box PATH] [-v|-q]
```

| 参数 | 说明 |
| --- | --- |
| `-c, --config` | 配置文件路径，默认 `config.json` |
| `--only NAME` | 只处理指定规则集，可重复 |
| `-n, --dry-run` | 只生成 JSON，不下载 sing-box、不编译 |
| `--strict` | 任一规则集失败即以非零码退出 |
| `--sing-box PATH` | 用指定的二进制，跳过下载（也可用环境变量 `SING_BOX_BIN`） |
| `-v` / `-vv` | 更详细的日志（`-vv` 放出 HTTP 请求日志） |
| `-q` | 只输出警告和错误（摘要仍然输出） |

退出码：`0` 至少有一个规则集成功；`1` 全部失败，或 `--strict` 下有失败；
`2` 配置或参数有误；`130` 被中断。

## 读懂摘要

```
规则集
────────────────────────────────────────────────────────────
  ✓ cn-ip                       4,932 条    39.2 KB   CIDR 聚合 -5628
  ✓ normal-ai                     589 条     6.1 KB   跳过 IP-ASN×2, USER-AGENT×1
  ✓ skk-speedtest               3,380 条    34.6 KB   按过滤规则丢弃 3
  ✗ some-set                        0 条              源不可用: https://… (HTTP 404)
────────────────────────────────────────────────────────────
  编译 51/52 个规则集，共 232,629 条规则，1.9 MB
```

摘要写 stdout，`-q` 也照常输出——它是结果，不是日志。

## 常见问题

**报错"期望 sing-box JSON"**：该源不是 JSON。按提示给它加 `"format": "text"`
或 `"yaml"`。注意判断依据是内容而不是扩展名，有些叫 `.list` 的文件内容其实是 JSON。

**报错"YAML 顶层是对象但找不到规则序列"**：该 YAML 的规则不在 `payload`、`rules`
等已知键下。检查一下文件结构。

**报错"sing-box x.y.z 的 xxx 构建不存在"**：`sing_box.version` 或 `platform` 与
实际发布对不上，去 GitHub Releases 核对。

**某个规则集失败会影响其他的吗**：不会。每个规则集独立处理，其余照常生成和编译。

**怎么避免每次都下载 sing-box**：二进制会缓存在 `.cache/sing-box/<版本>-<平台>/`，
本地重复运行不会重下。CI 里用 `actions/cache` 缓存这个目录。
