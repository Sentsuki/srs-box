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
    """获取所有生成的规则集文件，分别返回JSON和SRS文件"""
    config = load_config()
    if not config:
        return [], []
    
    json_files = []
    srs_files = []
    
    for ruleset_name, urls in config['rulesets'].items():
        json_file = f"{ruleset_name}.json"
        srs_file = f"{ruleset_name}.srs"
        
        if os.path.exists(json_file):
            json_files.append(json_file)
            print(f"找到JSON文件: {json_file}")
        else:
            print(f"警告: {json_file} 文件不存在")
            
        if os.path.exists(srs_file):
            srs_files.append(srs_file)
            print(f"找到SRS文件: {srs_file}")
        else:
            print(f"警告: {srs_file} 文件不存在")
    
    return json_files, srs_files

def push_files_to_branch(files, branch_name, file_type):
    """推送指定文件到指定分支"""
    if not files:
        print(f"没有找到任何{file_type}文件，跳过{branch_name}分支推送")
        return True
    
    try:
        print(f"准备推送 {len(files)} 个{file_type}文件到{branch_name}分支: {', '.join(files)}")
        
        # 获取当前分支名
        current_branch = subprocess.check_output("git rev-parse --abbrev-ref HEAD", shell=True).decode().strip()
        print(f"当前分支: {current_branch}")
        
        # 检查分支是否存在
        remote_branches = subprocess.check_output("git ls-remote --heads origin", shell=True).decode()
        branch_exists = f"refs/heads/{branch_name}" in remote_branches
        
        if branch_exists:
            # 如果分支存在，则删除它
            print(f"正在删除现有的{branch_name}分支")
            run_command(f"git push origin --delete {branch_name}")
            # 等待一些时间确保分支被删除
            time.sleep(2)
        
        # 创建并切换到新分支
        print(f"创建{branch_name}分支")
        run_command(f"git checkout --orphan {branch_name}")
        
        # 清除工作区
        run_command("git rm -rf --cached .")
        
        # 添加指定类型的文件
        print(f"添加{file_type}文件: {' '.join(files)}")
        for file in files:
            run_command(f"git add {file} -f")
        
        # 提交更改
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Update {file_type} rulesets - {current_time}"
        run_command(f'git commit -m "{commit_message}"')
        
        # 推送到远程
        print(f"推送到远程{branch_name}分支")
        run_command(f"git push -u origin {branch_name}")
        
        # 切换回原分支
        run_command(f"git checkout {current_branch}")
                
        print(f"成功推送{file_type}文件到{branch_name}分支")
        return True
        
    except Exception as e:
        print(f"推送{file_type}文件到{branch_name}分支时出错: {e}")
        return False

def push_to_separate_branches():
    """推送JSON和SRS文件到不同的分支"""
    try:
        # 获取要推送的文件列表
        json_files, srs_files = get_ruleset_files()
        
        if not json_files and not srs_files:
            print("错误: 没有找到任何规则集文件")
            return False
        
        # 配置Git
        print("配置Git")
        run_command('git config --global user.name "GitHub Actions"')
        run_command('git config --global user.email "actions@github.com"')
        
        success = True
        
        # 推送JSON文件到json-rules分支
        if not push_files_to_branch(json_files, "json-rules", "JSON"):
            success = False
        
        # 推送SRS文件到srs-rules分支  
        if not push_files_to_branch(srs_files, "srs-rules", "SRS"):
            success = False
        
        return success
        
    except Exception as e:
        print(f"推送文件时出错: {e}")
        return False

if __name__ == "__main__":
    push_to_separate_branches()
