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

`exclude` 用于从当前规则集中排除特定规则，支持与输入源相同的字段（`sources`、`files`、`geosite`、`inline`）。排除按域名及 IP 的包含关系（语义）生效。

```json
"google": {
  "geosite": ["google"],
  "exclude": {
    "geosite": ["google@ads"]
  }
}
```

> **注意**：若 `exclude` 中定义的所有源均获取失败，该规则集构建将判定为失败，以防排除失效导致规则泄露。

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

> **说明**：
> - `publish` 从环境变量 `GITHUB_TOKEN` 读取推送凭据。
> - 发布以单提交快照形式推送到目标分支；若内容未发生变化则跳过推送。

### 退出码

| 码 | 含义 |
| --- | --- |
| `0` | 成功完成 |
| `1` | 构建或发布失败（如所有规则集构建失败、`--strict` 模式下有规则集失败等） |
| `2` | 命令行参数或配置文件格式错误 |
| `130` | 进程被中断（SIGINT / SIGTERM） |
