package parse

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// lines 是行提取器，也是一切判定的兜底。
//
// 它本来就是格式无关的：条目层先看首段是不是已知类型词，不是就按值自身的形态
// 定字段。配合 cleanLine 剥掉 "- "、引号、注释之后，Clash .list、Surge .list、
// Quantumult X、扁平 YAML 序列、裸域名表、裸 CIDR 表 —— 全部走这一条路，
// 一个格式声明都不需要。
func (p *parser) lines(data []byte) error {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// 默认 64KB 的行上限对规则列表偶尔不够（有源把整份列表塞在一行里）。
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for scanner.Scan() {
		raw := scanner.Text()
		if err := guardYAMLMapping(raw); err != nil {
			return err
		}
		entry, ok := cleanLine(raw)
		if !ok {
			continue
		}
		if err := p.entry(entry); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return parseErr("", "读取内容失败: "+err.Error(), "")
	}
	return nil
}

// cleanLine 做整行级清洗：去整行注释、去 YAML 序列短横线、去成对引号。
//
// 剥掉 "- " 前缀这一步让行提取器能正确吃下一个 YAML 裸序列 —— 这正是
// "不需要 YAML 分支"的底气所在。
func cleanLine(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '#' || line[0] == ';' || strings.HasPrefix(line, "//") {
		return "", false
	}
	if strings.HasPrefix(line, "- ") {
		line = strings.TrimSpace(line[2:])
	} else if line == "-" {
		return "", false
	}
	if len(line) >= 2 && line[0] == line[len(line)-1] && (line[0] == '"' || line[0] == '\'') {
		line = strings.TrimSpace(line[1 : len(line)-1])
	}
	if line == "" {
		return "", false
	}
	// payload: 这类容器键在行提取器眼里是个条目，不认出来就会被当域名判非法，
	// 每个 YAML 源都白白多一条噪声诊断。
	if key, ok := strings.CutSuffix(line, ":"); ok {
		if _, known := containerKeys[strings.ToLower(strings.TrimSpace(key))]; known {
			return "", false
		}
	}
	return line, true
}

// guardYAMLMapping 拦下 YAML 的嵌套映射写法。
//
// 这是取消 YAML 分支之后唯一会**静默出错**的形态：
//
//	payload:
//	  DOMAIN-SUFFIX:
//	    - a.com
//
// 行提取器会把 a.com 当成裸值判成 domain，而它本该是 domain_suffix ——
// 匹配面悄悄变窄，产物合法、条数正常，没有任何迹象。
//
// 合法的规则行永远写成 TYPE,value 而不是 TYPE: value，所以"已知类型名 + 冒号"
// 这个特征只可能来自 YAML 映射。当场报错，比解析出一批错规则强得多。
func guardYAMLMapping(raw string) error {
	line := strings.TrimSpace(raw)
	if strings.HasPrefix(line, "- ") {
		line = strings.TrimSpace(line[2:])
	}
	m := yamlMappingRe.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	key := m[1]
	upper := strings.ToUpper(key)
	_, isType := types[upper]
	if !isType {
		if _, isLogical := logicalMode[upper]; isLogical {
			isType = true
		}
	}
	if !isType {
		if _, isField := ruleset.Lookup(strings.ToLower(key)); isField {
			isType = true
		}
	}
	if !isType {
		return nil
	}
	return parseErr(raw,
		fmt.Sprintf("检测到 YAML 嵌套映射写法（%q 后面跟冒号）", key),
		"按行解析会把它下面的值判成 domain 而不是 "+key+"，匹配面会悄悄变窄。"+
			"这种源需要先转成扁平列表或 sing-box JSON。")
}
