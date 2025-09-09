import os
import requests

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

def main():
    # 创建临时目录
    os.makedirs("temp", exist_ok=True)

    # 需要下载的文件列表
    urls = [
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cernet.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cernet6.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/chinanet.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/chinanet6.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cmcc.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cmcc6.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cstnet.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/cstnet6.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/drpeng.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/drpeng6.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/unicom.txt",
        "https://github.com/gaoyifan/china-operator-ip/raw/refs/heads/ip-lists/unicom6.txt",
    ]

    temp_files = []
    for i, url in enumerate(urls, start=1):
        temp_file = f"temp/file_{i}.txt"
        if download_file(url, temp_file):
            temp_files.append(temp_file)

    output_file = "china.txt"
    # 合并文件
    with open(output_file, 'w') as outfile:
        for infile in temp_files:
            try:
                with open(infile, 'r') as f:
                    for line in f:
                        cleaned_line = line.strip()
                        if cleaned_line:
                            outfile.write(f"{cleaned_line}\n")
            except Exception as e:
                print(f"处理文件 {infile} 时出错: {e}")

    print(f"已合并 {len(temp_files)} 个文件到 {output_file}")

if __name__ == "__main__":
    main()