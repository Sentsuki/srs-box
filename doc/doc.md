# 配置说明

配置文件默认是项目根目录的 `config.json`，可用 `-c` 指定路径。

```json
{
  "ruleset_version": 4,
  "output": {
    "srs":  { "dir": "output/srs",  "branch": "srs_release" },
    "json": { "dir": "output/json", "branch": "json_release" }
  },
  "fetch":   { "concurrency": 16, "timeout": "30s", "retries": 3 },
  "geosite": { "normalize": "lenient" },
  "rulesets": { }
}
```

没有 `schema` 字段 —— 防手滑靠**拒绝未知键**：忘了删的 `sing_box`、打错的
`rulesets_version`，都会当场报错并列出认识的键。这比版本号更准，而且对拼写错误
同样有效。

## 顶层字段

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `ruleset_version` | 是 | 目标 rule-set 版本。上限跟着链接进来的 sing-box 走，不用手工同步 |
| `output` | 是 | 产物：种类 → 写到哪、发到哪 |
| `fetch` | 否 | HTTP 输入源设置，默认并发 16、超时 30s、重试 3 次 |
| `geosite` | 否 | geosite 输入源设置 |
| `rulesets` | 是 | 规则集定义 |

### output

键名就是扩展名，所以不需要 `ext`；`dir` 只出现一次。

* **省略 `branch`** = 只本地生成，不发布
* **整条省略** = 根本不产出这种产物

### geosite

| 键 | 说明 |
| --- | --- |
| `repo` | dlc.dat 的上游，默认 `v2fly/domain-list-community` |
| `file` | 本地 dlc.dat，非空时不走网络 |
| `normalize` | `lenient`（默认，非法值丢弃并记账）或 `strict`（非法值即失败） |
| `bulk` | 批量展开，见下 |

dlc.dat 每次都会比对同一 release 里的 `dlc.dat.sha256sum`。不走 GitHub API，
所以不需要 token、不受限流。

## rulesets

键名就是输出文件名。**只有一种条目形状**：名字 → 一组输入 + 几个选项。
URL 写全，没有 `base` / `items` / `prefix`。

```json
"rulesets": {
  "telegram": {
    "sources": ["https://ruleset.skk.moe/sing-box/non_ip/telegram.json"],
    "geosite": ["telegram"]
  }
}
```

### 四种输入，可以混写

| 键 | 内容 |
| --- | --- |
| `sources` | URL |
| `files` | 本地路径（限工作目录内） |
| `geosite` | geosite code 或 glob |
| `inline` | 直接写在配置里的规则行 |

裸字符串或裸数组等于 `sources`，这是唯一的简写：

```json
"block-ads": "https://example.com/ads.json",
"skk-reject": ["https://example.com/a.json", "https://example.com/b.json"]
```

四个键都接受单个字符串或数组。

### 选项

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `exclude` | 无 | 一个对象，键是上面四种输入；从本集合做差 |
| `format` | 自动 | 只在两种情况写，见下 |
| `aggregate` | `false` | 合并相邻/包含的 CIDR |
| `collapse` | `true` | 等价写法收敛 |

### exclude

`exclude` 接受和输入完全一样的四个键，含义也一样：

```json
"reject": {
  "sources": ["https://ruleset.skk.moe/sing-box/non_ip/reject.json"],
  "exclude": { "files": ["data/allowlist.txt"], "inline": ["DOMAIN-SUFFIX,ok.example"] }
}
```

geosite 的属性差集就是这么写的 —— 要 google 但不要它的广告域名：

```json
"google": { "geosite": ["google"], "exclude": { "geosite": ["google@ads"] } }
```

**`exclude` 的输入全部取不到时，该规则集会失败而不是产出。** 否则本该排除的
东西会留在产物里，而产物看起来完全正常。

## format

**一般不用写。** 内容自己会说明它是什么：

```
魔数是 SRS ?             → 按编译好的 rule-set 读
去 BOM/空白后首字节是 { 或 [ → 按 JSON 读
其余一切                  → 按行读
```

按行读本来就是格式无关的：先看首段是不是已知类型词（`DOMAIN-SUFFIX,` 之类），
不是就按值自身的形态定字段。所以 Clash `.list`、Surge `.list`、Quantumult X、
扁平 YAML 序列、裸域名表、裸 CIDR 表全都走这一条路。

只有两种情况需要写 `format`：

| 取值 | 为什么必须写 |
| --- | --- |
| `adguard` | 语法与裸域名列表有重叠，判不出来；而且 `@@` 例外规则会编译成带 `invert` 的嵌套逻辑规则，猜错就是**语义反转** |
| `cidr` / `domainset` | 它们不是格式，是**断言** —— 容器就是按行读，区别只在"混进别的东西就当场失败"。防的正是上游返回 HTTP 200 的 HTML 错误页这种悄悄腐坏 |

## geosite code 与 glob

一个 code 就是 v2fly `domain-list-community` 里一份清单的名字（`cn`、`netflix`、
`geolocation-!cn`）。**属性变体** `code@attr` 也是可寻址的 code：

```json
"geosite": ["google"]        // 全部 google 域名
"geosite": ["google@ads"]    // 其中带 @ads 标记的那些
```

带属性的域名**同时留在父 code 里** —— 属性是标记不是移出，所以"要 google 但不要
广告"需要做差。

glob 是 `path.Match` 语义，匹配的是 **code 名**不是域名：

```json
"geosite": ["category-*-cn", "*@cn"]
```

* `!` 是**字面字符**不是取反（`geolocation-!cn` 要原样写全），glob 没有取反操作符
* 一个 code 都没匹配上会报错 —— 上游改名时静默产出零条规则是最难发现的失败形态

### bulk

唯一的"一条配置生成多个规则集"的口子。名字是 `prefix + code`：

```json
"geosite": {
  "bulk": { "prefix": "geosite-", "include": ["category-games*"], "exclude": ["*@*"] }
}
```

* `include` 必填，想要全量就显式写 `["*"]`（默认全量意味着一次手滑就推上千个文件）
* `exclude` 在批量时基本必写，否则 `category-games*` 会把 `category-games@cn`
  这类属性变体也各自生成一个规则集
* 与 `rulesets` 里的名字撞名会跳过并**标记名单不完整**，发布时据此跳过孤儿清理

## 命令行

```bash
srs-box build   [-c CONFIG] [--only NAME]... [-n] [--strict] [--report PATH] [--github-summary] [-q]
srs-box publish [-c CONFIG] [--report PATH] [--remote URL] [-n] [-q]
```

`build` 的退出码：一个规则集都没产出才返回 1（真·基础设施坏了）。单个源失败
不是失败 —— 逐规则集隔离的全部意义就在这里。加 `--strict` 则任一失败即非零。

`publish` 从 `GITHUB_TOKEN` 取推送凭据，远端默认由 `GITHUB_REPOSITORY` 推断。
它按运行报告分三态处理：本次产出的覆盖，配置里有但失败或被 `--only` 跳过的
**保留上一次发布的文件**，不在配置里的当孤儿删掉。规则集名单本身不可信时
（`bulk` 的通配符没能展开）跳过孤儿清理。
