import os
import json
import subprocess
import time
import glob
from datetime import datetime

def load_config():
    """加载配置文件"""
    try:
        with open("config.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

def run_command(command):
    """运行shell命令并返回结果"""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"命令执行失败: {command}")
            print(f"错误: {stderr}")
            return False
            
        return True
    except Exception as e:
        print(f"执行命令时出错: {e}")
        return False

def get_ruleset_files():
    """获取所有生成的规则集文件"""
    config = load_config()
    if not config:
        return []
    
    files_to_add = []
    for ruleset_name, urls in config['rulesets'].items():
        json_file = f"{ruleset_name}.json"
        srs_file = f"{ruleset_name}.srs"
        
        if os.path.exists(json_file):
            files_to_add.append(json_file)
            print(f"找到文件: {json_file}")
        else:
            print(f"警告: {json_file} 文件不存在")
            
        if os.path.exists(srs_file):
            files_to_add.append(srs_file)
            print(f"找到文件: {srs_file}")
        else:
            print(f"警告: {srs_file} 文件不存在")
    
    return files_to_add

def push_to_ruleset_branch():
    """推送所有规则集文件到rule-set分支"""
    try:
        # 获取要推送的文件列表
        files_to_add = get_ruleset_files()
        
        if not files_to_add:
            print("错误: 没有找到任何规则集文件")
            return False
        
        print(f"准备推送 {len(files_to_add)} 个文件: {', '.join(files_to_add)}")
        
        # 配置Git
        print("配置Git")
        run_command('git config --global user.name "GitHub Actions"')
        run_command('git config --global user.email "actions@github.com"')
        
        # 获取当前分支名
        current_branch = subprocess.check_output("git rev-parse --abbrev-ref HEAD", shell=True).decode().strip()
        print(f"当前分支: {current_branch}")
        
        # 检查rule-set分支是否存在
        remote_branches = subprocess.check_output("git ls-remote --heads origin", shell=True).decode()
        branch_exists = "refs/heads/rule-set" in remote_branches
        
        if branch_exists:
            # 如果分支存在，则删除它
            print("正在删除现有的rule-set分支")
            run_command("git push origin --delete rule-set")
            # 等待一些时间确保分支被删除
            time.sleep(2)
        
        # 创建并切换到新的rule-set分支
        print("创建rule-set分支")
        run_command("git checkout --orphan rule-set")
        
        # 清除工作区
        run_command("git rm -rf --cached .")
        
        # 添加所有规则集文件
        print(f"添加规则集文件: {' '.join(files_to_add)}")
        for file in files_to_add:
            run_command(f"git add {file} -f")
        
        # 提交更改
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Update IP rulesets - {current_time}"
        run_command(f'git commit -m "{commit_message}"')
        
        # 推送到远程
        print("推送到远程rule-set分支")
        run_command("git push -u origin rule-set")
                
        print("成功推送到rule-set分支")
        return True
        
    except Exception as e:
        print(f"推送到rule-set分支时出错: {e}")
        return False

if __name__ == "__main__":
    push_to_ruleset_branch()
