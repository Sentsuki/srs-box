package geosite

import (
	"errors"
	"fmt"
)

// dlc.dat 的 protobuf 解码。
//
// 手写而不是 import v2ray-core：上游 sing-geosite 为这一个 message 背了整个
// v2ray-core v5 加 google.golang.org/protobuf 运行时。我们需要的 schema 只有
// 四个 message、七个字段，两种 wire type：
//
//	GeoSiteList { repeated GeoSite entry = 1; }
//	GeoSite     { string country_code = 1; repeated Domain domain = 2; }
//	Domain      { Type type = 1; string value = 2; repeated Attribute attribute = 3; }
//	Attribute   { string key = 1; bool bool_value = 2; int64 int_value = 3; }
//
// 未知字段一律按 wire type 跳过，所以上游加字段不会让我们解不动。

// domainType 是 Domain.Type 枚举。
type domainType int32

const (
	typePlain      domainType = 0 // 子串匹配
	typeRegex      domainType = 1
	typeRootDomain domainType = 2 // 域名及其子域
	typeFull       domainType = 3 // 精确匹配
)

// rawDomain 是解码出来的一条原始域名记录。
type rawDomain struct {
	Type       domainType
	Value      string
	Attributes []string
}

// rawSite 是一个 code 及其全部域名。
type rawSite struct {
	Code    string
	Domains []rawDomain
}

var errTruncated = errors.New("数据在中途结束")

// decodeDLC 把 dlc.dat 解成若干 site。
func decodeDLC(data []byte) ([]rawSite, error) {
	var sites []rawSite
	r := reader{buf: data}
	for !r.done() {
		field, wire, err := r.tag()
		if err != nil {
			return nil, fmt.Errorf("读 GeoSiteList 失败: %w", err)
		}
		if field != 1 || wire != wireBytes {
			if err := r.skip(wire); err != nil {
				return nil, fmt.Errorf("读 GeoSiteList 失败: %w", err)
			}
			continue
		}
		chunk, err := r.bytes()
		if err != nil {
			return nil, fmt.Errorf("读 GeoSiteList.entry 失败: %w", err)
		}
		site, err := decodeSite(chunk)
		if err != nil {
			// 带上下标和 code 名：dlc.dat 是两百多万字节的二进制，
			// 一句"某个字段读不出来"对排错没有用，得说清是哪一条。
			where := fmt.Sprintf("GeoSiteList.entry[%d]", len(sites))
			if site.Code != "" {
				where += " (" + site.Code + ")"
			}
			return nil, fmt.Errorf("读 %s 失败: %w", where, err)
		}
		sites = append(sites, site)
	}
	if len(sites) == 0 {
		return nil, errors.New("没有解出任何 code —— 这多半不是一份 dlc.dat")
	}
	return sites, nil
}

func decodeSite(data []byte) (rawSite, error) {
	var site rawSite
	r := reader{buf: data}
	for !r.done() {
		field, wire, err := r.tag()
		if err != nil {
			return site, fmt.Errorf("读 GeoSite 失败: %w", err)
		}
		switch {
		case field == 1 && wire == wireBytes: // country_code
			raw, err := r.bytes()
			if err != nil {
				return site, fmt.Errorf("读 GeoSite.country_code 失败: %w", err)
			}
			site.Code = string(raw)
		case field == 2 && wire == wireBytes: // domain
			chunk, err := r.bytes()
			if err != nil {
				return site, fmt.Errorf("读 GeoSite.domain 失败: %w", err)
			}
			domain, err := decodeDomain(chunk)
			if err != nil {
				return site, fmt.Errorf("读 GeoSite.domain[%d] 失败: %w", len(site.Domains), err)
			}
			site.Domains = append(site.Domains, domain)
		default:
			if err := r.skip(wire); err != nil {
				return site, fmt.Errorf("读 GeoSite 失败: %w", err)
			}
		}
	}
	return site, nil
}

func decodeDomain(data []byte) (rawDomain, error) {
	var d rawDomain
	r := reader{buf: data}
	for !r.done() {
		field, wire, err := r.tag()
		if err != nil {
			return d, fmt.Errorf("读 Domain 失败: %w", err)
		}
		switch {
		case field == 1 && wire == wireVarint: // type
			v, err := r.varint()
			if err != nil {
				return d, fmt.Errorf("读 Domain.type 失败: %w", err)
			}
			d.Type = domainType(v)
		case field == 2 && wire == wireBytes: // value
			raw, err := r.bytes()
			if err != nil {
				return d, fmt.Errorf("读 Domain.value 失败: %w", err)
			}
			d.Value = string(raw)
		case field == 3 && wire == wireBytes: // attribute
			chunk, err := r.bytes()
			if err != nil {
				return d, fmt.Errorf("读 Domain.attribute 失败: %w", err)
			}
			key, err := decodeAttributeKey(chunk)
			if err != nil {
				return d, fmt.Errorf("读 Domain.attribute[%d] 失败: %w", len(d.Attributes), err)
			}
			if key != "" {
				d.Attributes = append(d.Attributes, key)
			}
		default:
			if err := r.skip(wire); err != nil {
				return d, fmt.Errorf("读 Domain 失败: %w", err)
			}
		}
	}
	return d, nil
}

// decodeAttributeKey 只取属性名。
//
// 属性的值（bool_value / int_value）在 dlc.dat 里恒为 true，v2fly 的数据格式
// 只用得到"有没有这个标签"。取值一律忽略，省得引入一个没人用的维度。
func decodeAttributeKey(data []byte) (string, error) {
	r := reader{buf: data}
	var key string
	for !r.done() {
		field, wire, err := r.tag()
		if err != nil {
			return "", fmt.Errorf("读 Attribute 失败: %w", err)
		}
		if field == 1 && wire == wireBytes {
			raw, err := r.bytes()
			if err != nil {
				return "", fmt.Errorf("读 Attribute.key 失败: %w", err)
			}
			key = string(raw)
			continue
		}
		if err := r.skip(wire); err != nil {
			return "", fmt.Errorf("读 Attribute 失败: %w", err)
		}
	}
	return key, nil
}

// ---------------- wire format ----------------

const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
	wireFixed32 = 5
)

type reader struct {
	buf []byte
	pos int
}

func (r *reader) done() bool { return r.pos >= len(r.buf) }

func (r *reader) tag() (field int, wire int, err error) {
	v, err := r.varint()
	if err != nil {
		return 0, 0, err
	}
	field = int(v >> 3)
	wire = int(v & 7)
	if field <= 0 {
		return 0, 0, fmt.Errorf("字段号非法: %d", field)
	}
	return field, wire, nil
}

func (r *reader) varint() (uint64, error) {
	var value uint64
	for shift := uint(0); ; shift += 7 {
		if r.pos >= len(r.buf) {
			return 0, errTruncated
		}
		if shift >= 64 {
			return 0, errors.New("varint 过长")
		}
		b := r.buf[r.pos]
		r.pos++
		value |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return value, nil
		}
	}
}

func (r *reader) bytes() ([]byte, error) {
	length, err := r.varint()
	if err != nil {
		return nil, err
	}
	// 先比对剩余长度再切片：不这么做的话，一个被截断或伪造的长度前缀会直接
	// panic，而输入是从网上下载的。
	if length > uint64(len(r.buf)-r.pos) {
		return nil, errTruncated
	}
	start := r.pos
	r.pos += int(length)
	return r.buf[start:r.pos], nil
}

// skip 跳过一个未知字段。上游给 message 加字段时不该让我们解不动。
func (r *reader) skip(wire int) error {
	switch wire {
	case wireVarint:
		_, err := r.varint()
		return err
	case wireFixed64:
		return r.advance(8)
	case wireBytes:
		_, err := r.bytes()
		return err
	case wireFixed32:
		return r.advance(4)
	default:
		return fmt.Errorf("不认识的 wire type %d", wire)
	}
}

func (r *reader) advance(n int) error {
	if n > len(r.buf)-r.pos {
		return errTruncated
	}
	r.pos += n
	return nil
}
