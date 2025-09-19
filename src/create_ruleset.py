import json
import os

def load_config():
    """加载配置文件"""
    try:
        with open("config.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

def create_ruleset_json(ruleset_name, config_version):
    """根据配置创建sing-box规则集JSON文件"""
    try:
        txt_file = f"temp/{ruleset_name}.txt"
        json_file = f"{ruleset_name}.json"
        
        # 检查JSON文件是否已经存在（由download_ip_lists.py生成）
        if os.path.exists(json_file):
            print(f"规则集JSON文件已存在: {json_file}（由JSON规则集生成）")
            return True
        
        # 检查txt文件是否存在
        if not os.path.exists(txt_file):
            print(f"错误: {txt_file} 文件不存在，且没有预生成的JSON文件")
            return False
        
        # 读取IP地址列表
        ip_cidrs = []
        with open(txt_file, 'r', encoding='utf-8') as f:
            for line in f:
                # 去掉首尾空格
                line = line.strip()
                if line:
                    ip_cidrs.append(line)
        
        # 创建规则集JSON结构，使用配置文件中的version
        ruleset = {
            "version": config_version,
            "rules": [
                {
                    "ip_cidr": ip_cidrs
                }
            ]
        }
        
        # 保存到文件
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(ruleset, f, indent=4)
        
        print(f"已创建规则集JSON文件: {json_file}，包含 {len(ip_cidrs)} 条IP规则")
        return True
    
    except Exception as e:
        print(f"创建规则集JSON时出错: {e}")
        return False

def main():
    # 加载配置
    config = load_config()
    if not config:
        return

    # 处理所有规则集
    for ruleset_name, urls in config['rulesets'].items():
        print(f"\n创建规则集: {ruleset_name}")
        if not create_ruleset_json(ruleset_name, config['version']):
            print(f"规则集 {ruleset_name} 创建失败")

if __name__ == "__main__":
    main()
