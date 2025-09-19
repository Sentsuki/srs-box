import os
import json
import requests
import tarfile
import subprocess
import shutil

def load_config():
    """加载配置文件"""
    try:
        with open("config.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None

def download_singbox(config):
    """下载并解压sing-box工具"""
    try:
        # 创建临时目录
        os.makedirs("temp", exist_ok=True)
        
        # 从配置文件获取版本和平台信息
        version = config['sing_box']['version']
        platform = config['sing_box']['platform']
        
        url = f"https://github.com/SagerNet/sing-box/releases/download/v{version}/sing-box-{version}-{platform}.tar.gz"
        output_file = "temp/sing-box.tar.gz"
        
        print(f"正在下载sing-box: {url}")
        
        # 下载文件
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # 解压文件
        print("正在解压sing-box")
        with tarfile.open(output_file, 'r:gz') as tar:
            tar.extractall(path="temp")
        
        # 移动二进制文件
        binary_name = "sing-box"
        extracted_dir = f"temp/sing-box-{version}-{platform}"
        shutil.copy(f"{extracted_dir}/{binary_name}", binary_name)
        
        # 修改权限使其可执行
        os.chmod(binary_name, 0o755)
        
        print(f"已准备好sing-box: {binary_name}")
        return binary_name
        
    except Exception as e:
        print(f"下载或解压sing-box时出错: {e}")
        return None

def compile_ruleset(binary_name, ruleset_name):
    """使用sing-box编译规则集"""
    try:
        json_file = f"{ruleset_name}.json"
        srs_file = f"{ruleset_name}.srs"
        
        # 检查规则集JSON是否存在
        if not os.path.exists(json_file):
            print(f"错误: {json_file} 文件不存在")
            return False
        
        # 构建命令
        cmd = [f"./{binary_name}", "rule-set", "compile", json_file]
        
        # 运行命令
        print(f"正在编译规则集 {ruleset_name}: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"编译失败: {result.stderr}")
            return False
            
        print(f"规则集编译成功: {srs_file}")
        return True
        
    except Exception as e:
        print(f"编译规则集时出错: {e}")
        return False

def main():
    # 加载配置
    config = load_config()
    if not config:
        return
    
    # 下载sing-box
    binary_name = download_singbox(config)
    if not binary_name:
        return
    
    # 编译所有规则集
    success_count = 0
    for ruleset_name, urls in config['rulesets'].items():
        if compile_ruleset(binary_name, ruleset_name):
            success_count += 1
    
    print(f"\n编译完成，成功编译 {success_count}/{len(config['rulesets'])} 个规则集")
    
    # 清理临时文件
    try:
        if os.path.exists("temp"):
            shutil.rmtree("temp")
        
        # 删除二进制文件
        if os.path.exists(binary_name):
            os.remove(binary_name)
    except Exception as e:
        print(f"清理临时文件时出错: {e}")

if __name__ == "__main__":
    main()
