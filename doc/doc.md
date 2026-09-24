# 配置说明

配置文件默认位于项目根目录的 `config.json`，可通过 `-c` / `--config` 参数指定路径。

## 配置示例

```json
{
  "ruleset_version": 4,
  "output": {
    "srs":  { "dir": "output/srs",  "branch": "srs_release" },
    "json": { "dir": "output/json", "branch": "json_release" }
  },
  "fetch": {
    "concurrency": 16,
    "timeout": "30s",
    "retries": 3
  },
  "geosite": {
    "repo": "v2fly/domain-list-community",
    "normalize": "lenient"
  },
  "rulesets": {
    "direct": {
      "sources": [
        "https://example.com/rules.json"
      ],
      "geosite": ["cn"]
    }
  }
}
```

## 顶层字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `ruleset_version` | integer | 是 | - | 目标 rule-set 规则集版本（1 ~ sing-box 支持的最高版本） |
| `output` | object | 是 | - | 输出产物配置，至少需要包含 `srs` 或 `json` 之一 |
| `fetch` | object | 否 | - | HTTP 下载与并发控制参数 |
| `geosite` | object | 否 | - | Geosite 数据源与预处理配置 |
| `rulesets` | object | 是 | - | 规则集定义集合 |

---

## output

定义不同格式产物的输出目录与发布分支。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `srs` | object | 二进制 `.srs` 规则集配置 |
| `json` | object | JSON 格式规则集配置 |

每个产物对象包含以下字段：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `dir` | string | 是 | - | 本地输出目录 |
| `branch` | string | 否 | 无 | Git 发布分支。若省略则仅在本地生成，不执行发布 |

---

## fetch

HTTP 规则源下载配置：

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `concurrency` | integer | `16` | 最大并发下载数（需 >= 1） |
| `timeout` | string / number | `"30s"` | 单个请求超时时间（支持 `"30s"`、`"1m"` 等格式，或秒数数字） |
| `retries` | integer | `3` | 请求失败重试次数（需 >= 0） |

---

## geosite

Geosite 数据源及相关配置：

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `repo` | string | `"v2fly/domain-list-community"` | 上游 GitHub 仓库，用于自动下载最新 `dlc.dat` |
| `file` | string | 无 | 本地 `dlc.dat` 文件路径，限工作目录内的相对路径。若指定则使用本地文件，不请求网络 |
| `normalize` | string | `"lenient"` | 域名归一化模式：`"lenient"`（忽略非法值）或 `"strict"`（非法值立即报错） |
| `bulk` | object | 无 | 批量展开生成规则集配置 |

### geosite.bulk

根据 geosite code 批量生成规则集，生成的规则集名称为 `${prefix}${code}`：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `prefix` | string | 否 | 生成规则集名称的前缀（如 `"geosite-"`） |
| `include` | string / string[] | 是 | 包含的 geosite code 模式（支持 glob，全量写 `["*"]`） |
| `exclude` | string / string[] | 否 | 排除的 geosite code 模式（支持 glob，如 `["*@*"]` 排除属性变体） |

---

## rulesets

规则集配置字典。键名即规则集名称（对应输出文件名，支持字母、数字、点、下划线、减号、`!`、`@`），值为规则集配置对象。

### 输入源

一个规则集支持同时配置以下多种输入源（均支持单个字符串或字符串数组）：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `sources` | string / string[] | HTTP/HTTPS 远程规则文件 URL |
| `files` | string / string[] | 本地规则文件路径（相对工作目录） |
| `geosite` | string / string[] | Geosite code 名称或 glob 模式（如 `"cn"`、`"google@ads"`、`"category-*-cn"`） |
| `inline` | string / string[] | 内联规则文本（如 `"DOMAIN-SUFFIX,example.com"`） |

> **简写形式**：若规则集的值直接为字符串或字符串数组，等价于仅配置 `sources`。例如：
> ```json
> "block-ads": "https://example.com/ads.json"
> ```

### 规则集选项

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `exclude` | object | 无 | 排除规则，支持与上述相同的输入源字段（`sources`、`files`、`geosite`、`inline`） |
| `format` | string | 自动识别 | 规则格式（默认为空，自动判定） |
| `aggregate` | boolean | `false` | 是否合并相邻或包含的 CIDR / IP 网段 |
| `collapse` | boolean | `true` | 是否收敛等价规则（如冗余的子域名去重） |

#### exclude 说明

`exclude` 用于从当前规则集中排除特定规则。例如保留 `google` 但排除其广告域名：

```json
"google": {
  "geosite": ["google"],
  "exclude": {
    "geosite": ["google@ads"]
  }
}
```

*若 `exclude` 中定义的所有源均获取失败，该规则集构建将判定为失败，以防排除失效导致规则泄露。*

##### 排除按语义生效，而非按字面

排除的判据是「`exclude` 匹配的域名集合是否包含这条规则匹配的集合」，而不是字符串相等。
因此主规则集与 `exclude` 来自不同上游、写法不同也能正确排除 —— 例如主集合里的
`+.example.com`（落为无点 `domain_suffix`）能被 geosite 的 `example.com`（落为
`domain` + 带点 `domain_suffix` 两条）排掉。

共有三种结果，均会出现在构建摘要中：

| 摘要字段 | 含义 |
| --- | --- |
| `差集 -N` | 规则被 `exclude` 完全覆盖，整条删除 |
| `排除改窄 N` | 规则被部分排除，改写为更窄的等价规则后保留（无点 `domain_suffix` 去掉 apex 即带点形式；CIDR 裁掉一段仍是一组 CIDR） |
| `排除不可表达 N` | 规则与 `exclude` 部分重叠，但 headless rule 无法表达该差集，规则原样保留 |

`排除不可表达` 是唯一一种排除不生效的情形。例如产物中有 `domain_suffix: a.com`、
而 `exclude` 中为 `domain: x.a.com`，差集应为「`a.com` 及其子域但不含 `x.a.com`」，
拆分字段的规则写不出这种形式。处理方式是改写配置：缩小主规则集的输入范围，或在
`exclude` 中直接排除 `a.com`。

两项已知限制：

- `domain_regex` 与透传规则（逻辑规则、带 `invert` 的规则）只按完全相同匹配 ——
  判断两个正则谁包含谁是不可判定的，从逻辑规则中「减掉」另一条亦无定义。
- 产物中的 `domain_keyword` 不计入 `排除不可表达`。关键词的匹配面是开放的，
  几乎任何 `exclude` 都与之部分重叠，计数只会产生噪声。

`ip_cidr` / `source_ip_cidr` 不存在表达边界：一组网段减一组网段仍是一组网段，
`10.0.0.0/8` 排除 `10.1.0.0/16` 会被精确裁分。此处不会顺带合并其余网段 ——
合并只由 `aggregate` 控制。

### 产物形状与可合并性

sing-box 在引用方规则同时写了域名条件与 `rule_set` 时，例如

```json
{ "domain_suffix": ["example.org"], "rule_set": ["geosite-google"] }
```

会把规则集并进引用方的匹配组，使两者成为 **OR**（任一命中即命中）。但这项合并
**仅在规则集恰好只有一条 rule 时生效**。规则集有两条或更多时，上述写法退化为
「`domain_suffix` 命中 **且** 规则集命中」，规则集那一半实际失效 —— 而产物本身
合法、条数正常，看不出异常。

因此 srs-box 把目标地址组的字段（`domain`、`domain_suffix`、`domain_keyword`、
`domain_regex`、`ip_cidr`）全部产出在同一条 rule 里，这些字段组内为 OR，合并前后
匹配结果相同。

以下两种情况会使产物超过一条 rule，规则集将不可合并：

| 成因 | 说明 |
| --- | --- |
| 透传规则 | 逻辑规则（`AND,(...)`）、带 `invert` 的规则、含未知字段的规则，以及 `format: "adguard"` 的全部产物 |
| rule 内 AND 项字段 | `network`、`port`、`source_port`、`process_name`、`process_path`、`package_name`、`source_ip_cidr` —— 合进同一条会变成「必须同时命中」，属于改语义，故各自成条 |

两者都不算配置错误，但构建摘要会标出 `N 个对象` 以便察觉。只写 `rule_set`、
不在同一条规则里混写域名条件时，不受此限制。

#### format 说明

大部分常见格式无需显式指定，程序会自动根据内容识别（sing-box JSON、SRS 二进制、Clash / Surge / QX / 纯文本行列表等）。

仅在以下场景需要显式指定：

| 取值 | 说明 |
| --- | --- |
| `adguard` | AdGuard 规则语法（支持 `||example.com^`、`@@` 例外规则等） |
| `cidr` | 断言格式：严格限制内容仅允许 CIDR/IP，混入其他内容则报错 |
| `domainset` | 断言格式：严格限制内容仅允许域名，混入其他内容则报错 |

---

## 命令行工具

### 构建 (build)

```bash
srs-box build [选项]
```

| 选项 | 简写 | 说明 |
| --- | --- | --- |
| `--config PATH` | `-c` | 配置文件路径（默认 `config.json`） |
| `--only NAME` | | 仅构建指定的规则集（可多次指定） |
| `--dry-run` | `-n` | 仅执行解析与处理，不写入磁盘 |
| `--strict` | | 严格模式：任一规则集失败则整体以非零退出码退出 |
| `--report PATH` | | 输出构建运行报告 JSON 路径 |
| `--github-summary` | | 输出 GitHub Actions 步骤摘要与注解 |
| `--quiet` | `-q` | 静默模式，仅输出摘要，不输出过程进度 |

### 发布 (publish)

```bash
srs-box publish [选项]
```

| 选项 | 简写 | 说明 |
| --- | --- | --- |
| `--config PATH` | `-c` | 配置文件路径（默认 `config.json`） |
| `--report PATH` | | 指定构建报告文件路径（默认 `run-report.json`） |
| `--remote URL` | | Git 远端仓库地址（默认根据 `GITHUB_REPOSITORY` 推断） |
| `--dry-run` | `-n` | 仅演练发布流程，不推送到远端分支 |
| `--quiet` | `-q` | 静默模式，不输出详细进度 |

> **发布认证**：`publish` 从环境变量 `GITHUB_TOKEN` 读取推送凭据。

### 发布分支只有一个提交

每次发布都造一个**无父提交**强推上去，发布分支上永远只有一个 commit。

产物分支是「当前快照」，它的历史没有任何使用者：按现在的规模，每天两个提交、
几十个二进制文件，一年七百多个提交，而旧版本的每个 blob 都永远可达 —— 远端只增
不减。断掉父链之后旧对象立刻不可达，由 GitHub 的 gc 回收。

几点影响：

- **内容没变就完全不推**，分支 SHA 纹丝不动 —— 定时任务不会为同一棵树换一个提交。
- 用 `--force-with-lease`：clone 之后别人往这个分支推过东西，本次推送会被拒绝而
  不是把它盖掉。
- 按分支名、raw URL、jsdelivr `@branch` 引用的不受影响；**按 commit SHA 钉住发布
  分支会失效**。
- 出问题时没法从 git 历史翻回上一次的产物 —— 重跑一次 `build` 即可重建。
- 远端仓库体积不会当场变小：旧对象不可达之后要等 GitHub 侧的 gc。

### 退出码

| 码 | 含义 |
| --- | --- |
| `0` | 正常结束（单个规则集失败不算 —— 逐规则集隔离，失败项在摘要里标 ✗） |
| `1` | 一个规则集都没产出（基础设施坏了），或 `--strict` 下有规则集失败，或发布失败 |
| `2` | 用法或配置有误，以及写报告/摘要失败 |
| `130` | 被 Ctrl-C / SIGTERM 中断。**中断不会退 0** —— 否则 CI 里一次超时会被当成一次正常运行，而运行报告和本地产物清理都会按"部分规则集失败"去执行 |
