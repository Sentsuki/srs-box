"""
转换服务
处理convert配置中的链接，使用原有的转换逻辑生成JSON规则集
融入现有的下载-处理-编译架构
"""

import os
import json
import pandas as pd
import re
import concurrent.futures
import requests
import yaml
import ipaddress
from io import StringIO
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from ..utils.config import ConfigManager
from ..utils.logger import Logger
from ..utils.file_utils import FileUtils
from ..utils.network import NetworkUtils


class ConvertedData:
    """转换数据结果类"""
    
    def __init__(self, convert_name: str):
        self.convert_name = convert_name
        self.json_files: List[str] = []
        self.srs_files: List[str] = []
        self.success_count = 0
        self.total_count = 0
        self.errors: List[str] = []
    
    def add_converted_file(self, json_file: str, srs_file: str) -> None:
        """添加转换成功的文件"""
        self.json_files.append(json_file)
        self.srs_files.append(srs_file)
        self.success_count += 1
    
    def add_error(self, error: str) -> None:
        """添加错误信息"""
        self.errors.append(error)
    
    def set_total_count(self, count: int) -> None:
        """设置总数量"""
        self.total_count = count
    
    def is_successful(self) -> bool:
        """是否有成功转换的数据"""
        return self.success_count > 0


class ConverterService:
    """转换服务类"""
    
    def __init__(self, config_manager: ConfigManager, logger: Logger, 
                 network_utils: NetworkUtils, file_utils: FileUtils):
        """
        初始化转换服务
        
        Args:
            config_manager: 配置管理器
            logger: 日志记录器
            network_utils: 网络工具
            file_utils: 文件工具
        """
        self.config_manager = config_manager
        self.logger = logger
        self.network_utils = network_utils
        self.file_utils = file_utils
        
        # 映射字典 - 从原有convert.py移植
        self.MAP_DICT = {
            'DOMAIN-SUFFIX': 'domain_suffix', 'HOST-SUFFIX': 'domain_suffix', 
            'host-suffix': 'domain_suffix', 'DOMAIN': 'domain', 'HOST': 'domain', 
            'host': 'domain', 'DOMAIN-KEYWORD': 'domain_keyword', 
            'HOST-KEYWORD': 'domain_keyword', 'host-keyword': 'domain_keyword', 
            'IP-CIDR': 'ip_cidr', 'ip-cidr': 'ip_cidr', 'IP-CIDR6': 'ip_cidr', 
            'IP6-CIDR': 'ip_cidr', 'SRC-IP-CIDR': 'source_ip_cidr', 
            'GEOIP': 'geoip', 'DST-PORT': 'port', 'SRC-PORT': 'source_port', 
            "URL-REGEX": "domain_regex", "DOMAIN-REGEX": "domain_regex"
        }
    
    def read_yaml_from_url(self, url: str) -> Any:
        """
        从URL读取YAML数据
        
        Args:
            url: YAML文件URL
            
        Returns:
            解析后的YAML数据
        """
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        yaml_data = yaml.safe_load(response.text)
        return yaml_data
    
    def read_list_from_url(self, url: str) -> Tuple[Optional[pd.DataFrame], List[Dict]]:
        """
        从URL读取列表数据
        
        Args:
            url: 列表文件URL
            
        Returns:
            (DataFrame数据, 逻辑规则列表)
        """
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return None, []
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, header=None, 
                        names=['pattern', 'address', 'other', 'other2', 'other3'], 
                        on_bad_lines='skip')
        
        filtered_rows = []
        rules = []
        
        # 处理逻辑规则
        if 'AND' in df['pattern'].values:
            and_rows = df[df['pattern'].str.contains('AND', na=False)]
            for _, row in and_rows.iterrows():
                rule = {
                    "type": "logical",
                    "mode": "and",
                    "rules": []
                }
                pattern = ",".join(row.values.astype(str))
                components = re.findall(r'\((.*?)\)', pattern)
                for component in components:
                    for keyword in self.MAP_DICT.keys():
                        if keyword in component:
                            match = re.search(f'{keyword},(.*)', component)
                            if match:
                                value = match.group(1)
                                rule["rules"].append({
                                    self.MAP_DICT[keyword]: value
                                })
                rules.append(rule)
        
        for index, row in df.iterrows():
            if 'AND' not in row['pattern']:
                filtered_rows.append(row)
        
        df_filtered = pd.DataFrame(filtered_rows, columns=['pattern', 'address', 'other', 'other2', 'other3'])
        return df_filtered, rules
    
    def is_ipv4_or_ipv6(self, address: str) -> Optional[str]:
        """
        检查地址是否为IPv4或IPv6
        
        Args:
            address: 要检查的地址
            
        Returns:
            'ipv4', 'ipv6' 或 None
        """
        try:
            ipaddress.IPv4Network(address)
            return 'ipv4'
        except ValueError:
            try:
                ipaddress.IPv6Network(address)
                return 'ipv6'
            except ValueError:
                return None
    
    def parse_and_convert_to_dataframe(self, link: str) -> Tuple[Optional[pd.DataFrame], List[Dict]]:
        """
        解析链接并转换为DataFrame
        
        Args:
            link: 要解析的链接
            
        Returns:
            (DataFrame数据, 逻辑规则列表)
        """
        rules = []
        
        # 根据链接扩展名分情况处理
        if link.endswith('.yaml') or link.endswith('.txt'):
            try:
                yaml_data = self.read_yaml_from_url(link)
                rows = []
                if not isinstance(yaml_data, str):
                    items = yaml_data.get('payload', [])
                else:
                    lines = yaml_data.splitlines()
                    line_content = lines[0]
                    items = line_content.split()
                
                for item in items:
                    address = item.strip("'")
                    if ',' not in item:
                        if self.is_ipv4_or_ipv6(item):
                            pattern = 'IP-CIDR'
                        else:
                            if address.startswith('+') or address.startswith('.'):
                                pattern = 'DOMAIN-SUFFIX'
                                address = address[1:]
                                if address.startswith('.'):
                                    address = address[1:]
                            else:
                                pattern = 'DOMAIN'
                    else:
                        pattern, address = item.split(',', 1)
                    
                    if ',' in address:
                        address = address.split(',', 1)[0]
                    
                    rows.append({'pattern': pattern.strip(), 'address': address.strip(), 'other': None})
                
                df = pd.DataFrame(rows, columns=['pattern', 'address', 'other'])
            except:
                df, rules = self.read_list_from_url(link)
        else:
            df, rules = self.read_list_from_url(link)
        
        return df, rules
    
    def sort_dict(self, obj: Any) -> Any:
        """
        对字典进行排序，含list of dict
        
        Args:
            obj: 要排序的对象
            
        Returns:
            排序后的对象
        """
        if isinstance(obj, dict):
            return {k: self.sort_dict(obj[k]) for k in sorted(obj)}
        elif isinstance(obj, list) and all(isinstance(elem, dict) for elem in obj):
            return sorted([self.sort_dict(x) for x in obj], key=lambda d: sorted(d.keys())[0])
        elif isinstance(obj, list):
            return sorted(self.sort_dict(x) for x in obj)
        else:
            return obj
    
    def convert_single_link(self, link: str, output_directory: Path) -> Optional[str]:
        """
        转换单个链接为JSON和SRS文件
        
        Args:
            link: 要转换的链接
            output_directory: 输出目录
            
        Returns:
            生成的JSON文件路径，失败返回None
        """
        try:
            self.logger.info(f"🔄 转换链接: {link}")
            
            # 解析链接数据
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = list(executor.map(self.parse_and_convert_to_dataframe, [link]))
                dfs = [df for df, rules in results]
                rules_list = [rules for df, rules in results]
                df = pd.concat(dfs, ignore_index=True)
            
            # 数据清理
            df = df[~df['pattern'].str.contains('#')].reset_index(drop=True)
            df = df[df['pattern'].isin(self.MAP_DICT.keys())].reset_index(drop=True)
            df = df.drop_duplicates().reset_index(drop=True)
            df['pattern'] = df['pattern'].replace(self.MAP_DICT)
            
            # 确保输出目录存在
            self.file_utils.ensure_dir(output_directory)
            
            # 构建规则集
            result_rules = {"version": 2, "rules": []}
            domain_entries = []
            
            for pattern, addresses in df.groupby('pattern')['address'].apply(list).to_dict().items():
                if pattern == 'domain_suffix':
                    rule_entry = {pattern: [address.strip() for address in addresses]}
                    result_rules["rules"].append(rule_entry)
                elif pattern == 'domain':
                    domain_entries.extend([address.strip() for address in addresses])
                else:
                    rule_entry = {pattern: [address.strip() for address in addresses]}
                    result_rules["rules"].append(rule_entry)
            
            # 删除domain_entries中的重复值
            domain_entries = list(set(domain_entries))
            if domain_entries:
                result_rules["rules"].insert(0, {'domain': domain_entries})
            
            # 生成文件名
            file_name = output_directory / f"{os.path.basename(link).split('.')[0]}.json"
            
            # 写入JSON文件
            with open(file_name, 'w', encoding='utf-8') as output_file:
                result_rules_str = json.dumps(self.sort_dict(result_rules), ensure_ascii=False, indent=2)
                result_rules_str = result_rules_str.replace('\\\\', '\\')
                output_file.write(result_rules_str)
            
            self.logger.success(f"✅ 转换完成: {file_name}")
            return str(file_name)
                
        except Exception as e:
            self.logger.error(f"❌ 转换链接失败: {link} - {str(e)}")
            return None
    
    def convert_ruleset(self, convert_name: str, urls: List[str]) -> ConvertedData:
        """
        转换单个convert规则集的所有链接
        
        Args:
            convert_name: convert规则集名称
            urls: URL列表
            
        Returns:
            转换数据结果
        """
        self.logger.info(f"🔄 开始转换规则集: {convert_name}")
        self.logger.info(f"📋 链接数量: {len(urls)}")
        
        # 创建转换结果对象
        converted_data = ConvertedData(convert_name)
        converted_data.set_total_count(len(urls))
        
        # 获取输出目录配置
        output_config = self.config_manager.get_output_config()
        json_dir = Path(output_config["json_dir"])
        
        # 为每个convert规则集创建子目录
        convert_json_dir = json_dir / "convert" / convert_name
        
        # 转换每个链接
        for i, url in enumerate(urls, 1):
            self.logger.info(f"🔄 转换链接 ({i}/{len(urls)}): {url}")
            
            json_file = self.convert_single_link(url, convert_json_dir)
            if json_file:
                converted_data.add_converted_file(json_file, "")  # SRS文件将在编译阶段统一生成
            else:
                converted_data.add_error(f"转换失败: {url}")
        
        # 输出转换结果摘要
        if converted_data.is_successful():
            self.logger.success(f"✅ 规则集 {convert_name} 转换完成")
            self.logger.info(f"📊 成功: {converted_data.success_count}/{converted_data.total_count}")
            self.logger.info(f"📄 JSON文件: {len(converted_data.json_files)} 个")
        else:
            self.logger.error(f"❌ 规则集 {convert_name} 转换失败")
        
        # 输出错误信息
        for error in converted_data.errors:
            self.logger.warning(f"⚠️ {error}")
        
        return converted_data
    
    def convert_all_rulesets(self) -> Dict[str, ConvertedData]:
        """
        转换所有convert配置中的规则集
        
        Returns:
            convert规则集名称到转换数据的映射
        """
        # 获取convert配置
        config = self.config_manager.load_config()
        convert_config = config.get('convert', {})
        
        if not convert_config:
            self.logger.info("📋 没有发现convert配置，跳过转换阶段")
            return {}
        
        results = {}
        
        self.logger.header("开始转换阶段")
        self.logger.info(f"📋 发现 {len(convert_config)} 个convert规则集")
        
        for i, (convert_name, urls) in enumerate(convert_config.items(), 1):
            self.logger.step(f"转换规则集: {convert_name}", i, len(convert_config))
            
            try:
                converted_data = self.convert_ruleset(convert_name, urls)
                results[convert_name] = converted_data
                
            except Exception as e:
                self.logger.error(f"❌ 规则集 {convert_name} 转换异常: {str(e)}")
                # 创建失败的转换数据
                failed_data = ConvertedData(convert_name)
                failed_data.set_total_count(len(urls))
                failed_data.add_error(f"转换异常: {str(e)}")
                results[convert_name] = failed_data
            
            # 添加分隔线（除了最后一个）
            if i < len(convert_config):
                self.logger.info("─" * 50)
        
        # 输出总体统计
        successful_converts = sum(1 for data in results.values() if data.is_successful())
        self.logger.separator("转换阶段完成")
        self.logger.success(f"✅ 转换完成: {successful_converts}/{len(convert_config)} 个规则集成功")
        
        return results
    
    def get_convert_statistics(self, results: Dict[str, ConvertedData]) -> Dict[str, Any]:
        """
        获取转换统计信息
        
        Args:
            results: 转换结果字典
            
        Returns:
            统计信息字典
        """
        total_converts = len(results)
        successful_converts = sum(1 for data in results.values() if data.is_successful())
        total_links = sum(data.total_count for data in results.values())
        successful_links = sum(data.success_count for data in results.values())
        
        total_json_files = sum(len(data.json_files) for data in results.values())
        return {
            'total_converts': total_converts,
            'successful_converts': successful_converts,
            'total_links': total_links,
            'successful_links': successful_links,
            'total_json_files': total_json_files,
            'success_rate': (successful_links / total_links * 100) if total_links > 0 else 0
        }