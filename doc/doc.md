# 使用说明

## 配置规则集

编辑项目根目录的 `config.json`。

```json
{
  "ip_only": {
    "telegram-ip": [
      "https://core.telegram.org/resources/cidr.txt"
    ]
  },
  "rulesets": {
    "example-json": [
      "https://example.com/rule-set.json"
    ]
  },
  "convert": {
    "example-clash": [
      "https://example.com/rules.list"
    ]
  },
  "sing_box": {
    "version": "1.13.3",
    "platform": "linux-amd64"
  },
  "output": {
    "json_dir": "output/json",
    "srs_dir": "output/srs"
  },
  "logging": {
    "level": "SUCCESS",
    "enable_color": true,
    "show_progress": true
  },
  "version": 4
}
```

规则集名称会直接作为输出文件名，请使用不会与其他规则集重复的名称。

### `ip_only`

用于纯 IP/CIDR 列表。每个键是一套规则集，值为一个或多个文本文件 URL。程序会合并、去重并排序各文件中非空且不以 `#` 开头的行，生成 `ip_cidr` 规则。

```json
"ip_only": {
  "cn-ip": [
    "https://example.com/china-ipv4.txt",
    "https://example.com/china-ipv6.txt"
  ]
}
```

### `rulesets`

用于已经是 sing-box JSON 格式的规则集。每个 URL 都会按 JSON 下载；同一名称下的多个文件会按规则字段合并并去重。不要在这里填写 Clash `.list`、YAML 或普通域名列表。

```json
"rulesets": {
  "apple": [
    "https://example.com/apple.json"
  ]
}
```

### `convert`

用于 Clash `.list` 或 YAML 的 `payload` 规则。支持 `DOMAIN`、`DOMAIN-SUFFIX`、`DOMAIN-KEYWORD`、`IP-CIDR`、`IP-CIDR6`、`GEOIP`、端口和正则等常见规则类型；不支持的类型会跳过。

```json
"convert": {
  "ads": [
    "https://example.com/ads.list",
    "https://example.com/extra.yaml"
  ]
}
```

### 输出和日志（可选）

- `output.json_dir`：中间 JSON 规则集目录，默认 `output/json`。
- `output.srs_dir`：最终 `.srs` 文件目录，默认 `output/srs`。
- `logging.level`：可设为 `DEBUG`、`INFO`、`SUCCESS`、`WARNING` 或 `ERROR`；默认 `INFO`。
- `logging.enable_color`：是否使用彩色日志。
- `logging.show_progress`：是否显示下载进度。

## 生成规则集

在项目根目录执行：

```bash
python main.py
```

程序依次下载规则源、生成 JSON、下载 sing-box、编译 `.srs`，完成后会显示生成文件和失败原因。只要至少有一个规则集成功编译，命令会以成功状态结束。

## 获取生成文件

默认输出如下：

```text
output/
├── json/
│   ├── cn-ip.json
│   ├── apple.json
│   └── ads.json
└── srs/
    ├── cn-ip.srs
    ├── apple.srs
    └── ads.srs
```

## 更新规则

无需删除旧输出。更新 `config.json` 中的 URL 或保持原配置，重新执行：

```bash
python main.py
```

同名文件会在生成时覆盖。临时下载文件、缓存和本次下载的 sing-box 程序会在流程结束时自动清理；`output/` 中的规则文件会保留。

## 常见问题

- **JSON 规则集下载失败**：确认 `rulesets` 中的链接实际返回 JSON，而不是 `.list`、YAML 或网页。
- **YAML 规则未被识别**：安装 PyYAML：`python -m pip install pyyaml`。
- **sing-box 下载或启动失败**：确认网络可访问 GitHub Releases，并检查 `sing_box.version` 是否存在、`sing_box.platform` 是否与运行环境匹配。
- **没有任何文件生成**：至少配置一项 `ip_only`、`rulesets` 或 `convert`，并确认其中至少一个来源可访问。
