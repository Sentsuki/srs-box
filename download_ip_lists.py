import os
import json
import requests

def load_config():
    """加载配置文件"""
    try:
        with open("link.json", 'r', encoding='utf-8') as f:
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

def process_ruleset(ruleset_name, ruleset_config):
    """处理单个规则集"""
    print(f"\n处理规则集: {ruleset_name}")
    print(f"描述: {ruleset_config.get('description', '无描述')}")
    
    # 创建临时目录
    temp_dir = f"temp/{ruleset_name}"
    os.makedirs(temp_dir, exist_ok=True)

    # 下载所有源文件
    temp_files = []
    for i, url in enumerate(ruleset_config['sources'], start=1):
        temp_file = f"{temp_dir}/file_{i}.txt"
        if download_file(url, temp_file):
            temp_files.append(temp_file)

    # 合并文件 - 使用临时文件名
    output_file = f"temp/{ruleset_config['output']}.txt"
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
    for ruleset_name, ruleset_config in config['rulesets'].items():
        if not process_ruleset(ruleset_name, ruleset_config):
            print(f"规则集 {ruleset_name} 处理失败")

if __name__ == "__main__":
    main()