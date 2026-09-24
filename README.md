# srs-box

把各家规则源编译成 sing-box rule-set。

**目标：任意来源的规则混进一个规则集，去重后得到你要的那一份。**

输入可以是 HTTP 列表、本地文件、geosite code、直接写在配置里的规则行，甚至是
别人已经编译好的 `.srs`，几种在同一个规则集里混写：

```json
"telegram": {
  "sources": ["https://ruleset.skk.moe/sing-box/non_ip/telegram.json"],
  "geosite": ["telegram"],
  "inline":  ["DOMAIN-SUFFIX,t.me"]
}
```

sing-box 是**链接进来的库**，不是下载下来的二进制 —— 没有子进程、没有校验和
自钉、没有编译超时，`.srs` 直接写出，而且写出去的能读回来验证。

## 用法

```bash
go build -o srs-box ./cmd/srs-box

./srs-box build --report run-report.json
./srs-box publish --report run-report.json
```

配置说明见 [doc/doc.md](doc/doc.md)，完整示例见
[doc/config.example.json](doc/config.example.json)。

## 设计要点

* **每个规则集是独立单元。**任何一个失败都不牵连其他，退出码只看总产出。
* **格式不用声明。**内容自己会说明它是什么；只有 AdGuard 和两种断言
  （`cidr` / `domainset`）需要显式写。
* **等价写法会收敛。**同一件事在不同上游有不同写法（Clash 的无点后缀、
  domainset 的带点后缀、geosite 拆成的两条），不收敛的话集合去重一条都抓不住。
  四条变换都不改变匹配结果，有测试盯着。
* **失败时保留上一次发布的文件。**发布分三态：本次产出的覆盖，配置里有但失败的
  保留，不在配置里的当孤儿清掉。

## License

[MIT](LICENSE)
