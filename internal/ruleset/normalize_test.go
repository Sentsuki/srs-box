package ruleset

import "testing"

func TestNormalizeHost(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "Example.COM", want: "example.com"},
		{in: "  example.com  ", want: "example.com"},
		{in: "example.com.", want: "example.com"},               // 末尾根点去掉
		{in: ".example.com", want: ".example.com"},              // 前导点保留：语义不同
		{in: "..example.com", want: ".example.com"},             // 多个前导点压成一个
		{in: "+.example.com", wantErr: true},                    // + 由解析层处理，到不了这里
		{in: "xn--fiqs8s", want: "xn--fiqs8s"},                  // 已是 punycode
		{in: "中国", want: "xn--fiqs8s"},                          // 转 punycode
		{in: "测试.example.com", want: "xn--0zwm56d.example.com"}, // 逐 label
		{in: "a_b.example.com", want: "a_b.example.com"},        // 下划线合法
		{in: "-bad.example.com", wantErr: true},                 // label 不能以连字符开头
		{in: "bad-.example.com", wantErr: true},                 // 也不能结尾
		{in: "*.example.com", wantErr: true},                    // 通配符是字面量，永不命中
		{in: "", wantErr: true},
		{in: ".", wantErr: true},
		{in: "a..b", wantErr: true}, // 空 label
	}
	for _, c := range cases {
		got, err := NormalizeHost(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("NormalizeHost(%q) = %q, 期望报错", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizeHost(%q) 意外报错: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("NormalizeHost(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeHostTooLong(t *testing.T) {
	label := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 64
	if _, err := NormalizeHost(label + ".com"); err == nil {
		t.Error("64 字节的 label 应当被拒绝")
	}
	long := ""
	for i := 0; i < 26; i++ {
		long += "abcdefghij."
	}
	if _, err := NormalizeHost(long + "com"); err == nil {
		t.Error("超过 253 字节的域名应当被拒绝")
	}
}

func TestNormalizeCIDR(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "1.2.3.0/24", want: "1.2.3.0/24"},
		{in: "1.2.3.4/24", want: "1.2.3.0/24"}, // 主机位归整，否则去重失效
		{in: "1.2.3.4", want: "1.2.3.4/32"},    // 裸 IP 补满掩码
		{in: "  1.2.3.4/32  ", want: "1.2.3.4/32"},
		{in: "2001:db8::/32", want: "2001:db8::/32"},
		{in: "2001:DB8::1", want: "2001:db8::1/128"},
		{in: "2001:db8::1/32", want: "2001:db8::/32"},
		{in: "not-an-ip", wantErr: true},
		{in: "1.2.3.4/33", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, c := range cases {
		got, err := NormalizeCIDR(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("NormalizeCIDR(%q) = %q, 期望报错", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizeCIDR(%q) 意外报错: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("NormalizeCIDR(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizePort(t *testing.T) {
	for _, in := range []string{"0", "443", "65535", " 80 "} {
		if _, err := NormalizePort(in); err != nil {
			t.Errorf("NormalizePort(%q) 意外报错: %v", in, err)
		}
	}
	for _, in := range []string{"-1", "65536", "80.5", "http", ""} {
		if got, err := NormalizePort(in); err == nil {
			t.Errorf("NormalizePort(%q) = %d, 期望报错", in, got)
		}
	}
}

// 大小写敏感的字段不能被小写化：Linux/macOS 上进程名区分大小写，
// 把 Telegram 压成 telegram 会让规则永不命中。
func TestCaseSensitiveFields(t *testing.T) {
	s := New("t")
	for _, f := range []Field{FieldProcessName, FieldProcessPath, FieldPackageName, FieldDomainRegex} {
		if err := s.Add(f, "Telegram"); err != nil {
			t.Fatalf("Add(%s) 报错: %v", f, err)
		}
		if got := s.Values(f); len(got) != 1 || got[0] != "Telegram" {
			t.Errorf("%s 被改写成了 %v，应当原样保留", f, got)
		}
	}
	// 域名相反：DNS 本就大小写不敏感，小写化是安全且必要的去重前提。
	s2 := New("t")
	s2.Add(FieldDomain, "Example.COM")
	s2.Add(FieldDomain, "example.com")
	if got := s2.Values(FieldDomain); len(got) != 1 {
		t.Errorf("大小写不同的同一域名应当去重成一条，实际 %v", got)
	}
}
