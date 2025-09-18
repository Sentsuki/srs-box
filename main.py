#!/usr/bin/env python3

import json
import sys
import subprocess
import os

def load_config():
    """加载配置文件"""
    try:
        with open("link.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

def run_script(script_name):
    """运行Python脚本"""
    try:
        print(f"\n{'='*50}")
        print(f"运行脚本: {script_name}")
        print(f"{'='*50}")
        
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, text=True)
        
        if result.returncode != 0:
            print(f"脚本 {script_name} 执行失败，返回码: {result.returncode}")
            return False
        
        print(f"脚本 {script_name} 执行成功")
        return True
        
    except Exception as e:
        print(f"运行脚本 {script_name} 时出错: {e}")
        return False

def main():
    """主函数"""
    print("🌏 One-GeoIP 规则集生成器")
    print("根据配置文件动态生成sing-box规则集")
    
    # 检查配置文件
    config = load_config()
    if not config:
        print("❌ 配置文件加载失败，程序退出")
        return
    
    print(f"\n📋 配置信息:")
    print(f"   规则集数量: {len(config['rulesets'])}")
    print(f"   sing-box版本: {config['sing_box']['version']}")
    print(f"   平台: {config['sing_box']['platform']}")
    
    for name, ruleset in config['rulesets'].items():
        print(f"   - {name}: {ruleset['description']}")
    
    # 执行步骤
    steps = [
        ("下载IP列表", "download_ip_lists.py"),
        ("创建规则集JSON", "create_ruleset.py"),
        ("下载并编译规则集", "download_and_compile.py")
    ]
    
    for step_name, script_name in steps:
        print(f"\n🚀 步骤: {step_name}")
        if not run_script(script_name):
            print(f"❌ 步骤失败: {step_name}")
            return
    
    print(f"\n✅ 所有步骤完成！")
    
    # 显示生成的文件
    print(f"\n📁 生成的文件:")
    for name, ruleset in config['rulesets'].items():
        output_name = ruleset['output']
        files = [f"{output_name}.json", f"{output_name}.srs"]
        for file in files:
            if os.path.exists(file):
                size = os.path.getsize(file)
                print(f"   ✓ {file} ({size:,} bytes)")
            else:
                print(f"   ✗ {file} (未找到)")

if __name__ == "__main__":
    main()