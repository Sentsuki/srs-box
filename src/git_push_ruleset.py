import os
import subprocess
import shutil
import glob

def run_command(command, cwd=None):
    """运行shell命令"""
    try:
        result = subprocess.run(command, shell=True, cwd=cwd, check=True, 
                              capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {command}")
        print(f"错误: {e.stderr}")
        return False

def create_folders_and_move_files():
    """创建文件夹并移动文件"""
    print("创建文件夹并移动文件")
    
    # 创建文件夹
    os.makedirs("./rule_json", exist_ok=True)
    os.makedirs("./rule_srs", exist_ok=True)
    
    # 移动所有.srs文件到rule_srs文件夹
    srs_files = glob.glob("*.srs")
    for srs_file in srs_files:
        dest_path = f"./rule_srs/{srs_file}"
        shutil.move(srs_file, dest_path)
        print(f"移动SRS文件: {srs_file} -> {dest_path}")
    
    # 移动所有.json文件到rule_json文件夹
    json_files = glob.glob("*.json")
    # 排除config.json
    json_files = [f for f in json_files if f != "config.json"]
    for json_file in json_files:
        dest_path = f"./rule_json/{json_file}"
        shutil.move(json_file, dest_path)
        print(f"移动JSON文件: {json_file} -> {dest_path}")
    
    return len(json_files) > 0, len(srs_files) > 0

def push_srs_files():
    """推送SRS文件到srs_release分支"""
    print("推送SRS文件到srs_release分支")
    
    if not os.path.exists("./rule_srs") or not os.listdir("./rule_srs"):
        print("没有SRS文件需要推送")
        return True
    
    commands = [
        "git init",
        'git config --local user.name "github-actions[bot]"',
        'git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"',
        "git checkout -b srs_release",
        "git add -A",
        'git commit -m "update srs"'
    ]
    
    for cmd in commands:
        if not run_command(cmd, cwd="./rule_srs"):
            return False
    
    # 获取环境变量
    github_actor = os.environ.get('GITHUB_ACTOR', 'github-actions')
    github_token = os.environ.get('GITHUB_TOKEN', '')
    github_repository = os.environ.get('GITHUB_REPOSITORY', '')
    
    if not github_repository or not github_token:
        print("缺少必要的GitHub环境变量")
        return False
    
    remote_url = f"https://{github_actor}:{github_token}@github.com/{github_repository}"
    
    push_commands = [
        f'git remote add sing-box-geosite "{remote_url}"',
        "git push -f -u sing-box-geosite srs_release"
    ]
    
    for cmd in push_commands:
        if not run_command(cmd, cwd="./rule_srs"):
            return False
    
    print("SRS文件推送成功")
    return True

def push_json_files():
    """推送JSON文件到json_release分支"""
    print("推送JSON文件到json_release分支")
    
    if not os.path.exists("./rule_json") or not os.listdir("./rule_json"):
        print("没有JSON文件需要推送")
        return True
    
    commands = [
        "git init",
        'git config --local user.name "github-actions[bot]"',
        'git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"',
        "git checkout -b json_release",
        "git add -A",
        'git commit -m "update json"'
    ]
    
    for cmd in commands:
        if not run_command(cmd, cwd="./rule_json"):
            return False
    
    # 获取环境变量
    github_actor = os.environ.get('GITHUB_ACTOR', 'github-actions')
    github_token = os.environ.get('GITHUB_TOKEN', '')
    github_repository = os.environ.get('GITHUB_REPOSITORY', '')
    
    if not github_repository or not github_token:
        print("缺少必要的GitHub环境变量")
        return False
    
    remote_url = f"https://{github_actor}:{github_token}@github.com/{github_repository}"
    
    push_commands = [
        f'git remote add sing-box-geosite "{remote_url}"',
        "git push -f -u sing-box-geosite json_release"
    ]
    
    for cmd in push_commands:
        if not run_command(cmd, cwd="./rule_json"):
            return False
    
    print("JSON文件推送成功")
    return True

def main():
    """主函数"""
    try:
        # 创建文件夹并移动文件
        has_json, has_srs = create_folders_and_move_files()
        
        if not has_json and not has_srs:
            print("没有找到任何规则集文件")
            return False
        
        success = True
        
        # 推送SRS文件
        if has_srs:
            if not push_srs_files():
                success = False
        
        # 推送JSON文件  
        if has_json:
            if not push_json_files():
                success = False
        
        return success
        
    except Exception as e:
        print(f"推送过程中出错: {e}")
        return False

if __name__ == "__main__":
    main()