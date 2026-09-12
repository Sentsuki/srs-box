# srs-box

把各家规则源（sing-box JSON、Clash/Surge 规则列表、YAML provider、纯 IP/域名列表）
抓取、合并、去重，编译成 sing-box 的 `.srs` 规则集。

```bash
pip install -e .
srsbox                    # 生成 output/json 和 output/srs
srsbox --dry-run          # 只生成 JSON，跳过编译
srsbox --only cn-ip -v    # 只处理一个规则集，看详细日志
```

产物每天由 GitHub Actions 更新并推送到两个分支：

| 分支 | 内容 |
| --- | --- |
| `srs_release` | 编译好的 `.srs` |
| `json_release` | 中间的 JSON 规则集 |

配置说明见 [doc/doc.md](doc/doc.md)。

## 设计

一条流水线，没有按来源类型分叉：

```
config → 并发抓取 → 按声明的格式解析 → RuleSet → 合并去重 → JSON → sing-box 编译 → .srs
```

几个约束撑起了整体的正确性：

- **`RuleSet` 是唯一的规则容器**，值只能经 `add()` 进、经 `to_json()` 出。字段白名单
  和类型归一都在 `add()` 里，所以产出 sing-box 不认识的字段、字符串端口、非法 CIDR
  在结构上就不可能。序列化只用 `json.dump`，不对结果做字符串加工。
- **格式靠声明不靠嗅探**，默认 `singbox`。声明错了当场报错并给出改法，而不是猜一个
  然后产出垃圾。52 个规则集里只有 5 个需要写 `format`。
- **每个规则集是独立单元**，任何一个失败都不影响其他，退出码只看总产出
  （要求全绿就加 `--strict`）。
- **全程不落临时文件**，源内容在内存里流转，只有 sing-box 二进制会缓存到 `.cache/`。

## 开发

```bash
pip install -e ".[dev]"
pytest
black srsbox tests && isort srsbox tests && flake8 srsbox tests
```

本地想跳过 20 MB 的 sing-box 下载，用现成的二进制：

```bash
SING_BOX_BIN=/usr/bin/sing-box srsbox
srsbox --sing-box ./sing-box
```

## License

[MIT](LICENSE)
