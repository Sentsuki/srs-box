import os
import json
import requests

def load_config():
    """加载配置文件"""
    try:
        with open("config.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

def download_file(url, output_file):
    """下载文件并保存到指定路径"""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"成功下载: {url} -> {output_file}")
        return True
    except Exception as e:
        print(f"下载失败: {url}, 错误: {e}")
        return False

def is_json_ruleset(url):
    """检查URL是否为JSON规则集（后缀为.json或.list）"""
    url_lower = url.lower()
    return url_lower.endswith('.json') or url_lower.endswith('.list')

def download_json_ruleset(url):
    """下载并解析JSON规则集"""
    try:
        print(f"下载JSON规则集: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # 解析JSON
        json_data = response.json()
        return json_data
        
    except Exception as e:
        print(f"下载JSON规则集失败: {url}, 错误: {e}")
        return None

def merge_json_rulesets(json_rulesets, config_version):
    """智能合并多个JSON规则集，将相同类型的规则合并在一起"""
    # 用于存储合并后的规则，按规则类型分组
    rule_groups = {}
    
    for json_data in json_rulesets:
        rules = []
        if 'rules' in json_data and isinstance(json_data['rules'], list):
            rules = json_data['rules']
        else:
            # 如果JSON结构不标准，尝试直接作为规则处理
            rules = [json_data]
        
        # 处理每个规则
        for rule in rules:
            if not isinstance(rule, dict):
                continue
                
            # 遍历规则中的每个字段
            for rule_type, rule_values in rule.items():
                if not isinstance(rule_values, list):
                    continue
                    
                # 如果这个规则类型还没有，创建新的列表
                if rule_type not in rule_groups:
                    rule_groups[rule_type] = []
                
                # 合并规则值，去重
                for value in rule_values:
                    if value not in rule_groups[rule_type]:
                        rule_groups[rule_type].append(value)
    
    # 将分组的规则转换为最终格式
    merged_rules = []
    for rule_type, rule_values in rule_groups.items():
        if rule_values:  # 只添加非空的规则
            merged_rules.append({rule_type: rule_values})
    
    # 创建合并后的规则集
    merged_ruleset = {
        "version": config_version,
        "rules": merged_rules
    }
    
    return merged_ruleset

def process_ruleset(ruleset_name, urls, config_version):
    """处理单个规则集"""
    print(f"\n处理规则集: {ruleset_name}")
    print(f"数据源数量: {len(urls)}")
    
    # 检查是否有JSON规则集
    json_urls = [url for url in urls if is_json_ruleset(url)]
    txt_urls = [url for url in urls if not is_json_ruleset(url)]
    
    # 处理JSON规则集
    if json_urls:
        print(f"发现 {len(json_urls)} 个JSON规则集，开始下载和合并")
        json_rulesets = []
        
        # 下载所有JSON规则集
        for url in json_urls:
            json_data = download_json_ruleset(url)
            if json_data:
                json_rulesets.append(json_data)
        
        if json_rulesets:
            # 合并JSON规则集
            if len(json_rulesets) == 1:
                # 只有一个JSON文件，直接使用
                merged_ruleset = json_rulesets[0]
                print(f"使用单个JSON规则集")
            else:
                # 多个JSON文件，需要合并
                merged_ruleset = merge_json_rulesets(json_rulesets, config_version)
                
                # 统计合并结果
                rule_count = 0
                rule_types = []
                for rule in merged_ruleset.get('rules', []):
                    for rule_type, rule_values in rule.items():
                        rule_types.append(f"{rule_type}({len(rule_values)})")
                        rule_count += len(rule_values)
                
                print(f"已合并 {len(json_rulesets)} 个JSON规则集")
                print(f"合并结果: {', '.join(rule_types)}，总计 {rule_count} 条规则")
            
            # 保存合并后的规则集
            output_file = f"{ruleset_name}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(merged_ruleset, f, indent=4)
            
            print(f"JSON规则集已保存到: {output_file}")
            return True
        else:
            print("所有JSON规则集下载失败，尝试处理文本规则集")
    
    # 处理文本规则集（原有逻辑）
    if not txt_urls:
        print("没有可处理的文本规则集")
        return False
    
    # 创建临时目录
    temp_dir = f"temp/{ruleset_name}"
    os.makedirs(temp_dir, exist_ok=True)

    # 下载所有源文件
    temp_files = []
    for i, url in enumerate(txt_urls, start=1):
        temp_file = f"{temp_dir}/file_{i}.txt"
        if download_file(url, temp_file):
            temp_files.append(temp_file)

    # 合并文件 - 输出名字就是规则集名字
    output_file = f"temp/{ruleset_name}.txt"
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for infile in temp_files:
            try:
                with open(infile, 'r', encoding='utf-8') as f:
                    for line in f:
                        cleaned_line = line.strip()
                        if cleaned_line:
                            outfile.write(f"{cleaned_line}\n")
            except Exception as e:
                print(f"处理文件 {infile} 时出错: {e}")

    print(f"已合并 {len(temp_files)} 个文件到 {output_file}")
    return len(temp_files) > 0

def main():
    # 加载配置
    config = load_config()
    if not config:
        return

    # 创建临时目录
    os.makedirs("temp", exist_ok=True)

    # 处理所有规则集
    for ruleset_name, urls in config['rulesets'].items():
        if not process_ruleset(ruleset_name, urls, config['version']):
            print(f"规则集 {ruleset_name} 处理失败")

if __name__ == "__main__":
    main()