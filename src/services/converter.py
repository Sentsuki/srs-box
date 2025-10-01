"""
转换服务
处理convert配置中的链接，使用原有的转换逻辑生成JSON规则集
融入现有的下载-处理-编译架构
"""

import json
import pandas as pd
import re
import yaml
import ipaddress
from io import StringIO
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from ..utils.config import ConfigManager
from ..utils.logger import Logger
from ..utils.file_utils import FileUtils
from ..utils.network import NetworkUtils
from .downloader import DownloadedData


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
    

    
    def convert_all_rulesets_from_downloads(self, convert_download_results: Dict[str, DownloadedData]) -> Dict[str, ConvertedData]:
        """
        从已下载的数据转换convert规则集
        
        Args:
            convert_download_results: convert类型的下载结果
            
        Returns:
            convert规则集名称到转换数据的映射
        """
        if not convert_download_results:
            self.logger.info("📋 没有convert下载数据，跳过转换")
            return {}
        
        results = {}
        
        self.logger.header("开始convert转换阶段")
        self.logger.info(f"📋 处理 {len(convert_download_results)} 个convert规则集")
        
        for i, (convert_name, download_data) in enumerate(convert_download_results.items(), 1):
            self.logger.step(f"转换规则集: {convert_name}", i, len(convert_download_results))
            
            try:
                converted_data = self.convert_ruleset_from_download(convert_name, download_data)
                results[convert_name] = converted_data
                
            except Exception as e:
                self.logger.error(f"❌ 规则集 {convert_name} 转换异常: {str(e)}")
                # 创建失败的转换数据
                failed_data = ConvertedData(convert_name)
                failed_data.set_total_count(download_data.total_count)
                failed_data.add_error(f"转换异常: {str(e)}")
                results[convert_name] = failed_data
            
            # 添加分隔线（除了最后一个）
            if i < len(convert_download_results):
                self.logger.info("─" * 50)
        
        # 输出总体统计
        stats = self.get_convert_statistics(results)
        self.logger.separator("convert转换阶段完成")
        self.logger.success(f"✅ 转换完成: {stats['successful_urls']}/{stats['total_urls']} 个配置成功")
        
        return results
    
    def convert_ruleset_from_download(self, convert_name: str, download_data: DownloadedData) -> ConvertedData:
        """
        从已下载的数据转换单个convert规则集
        
        Args:
            convert_name: convert规则集名称
            download_data: 已下载的数据
            
        Returns:
            转换数据结果
        """
        self.logger.info(f"🔄 开始转换规则集: {convert_name}")
        self.logger.info(f"📋 已下载文件数量: {len(download_data.text_files)}")
        
        # 创建转换结果对象
        converted_data = ConvertedData(convert_name)
        converted_data.set_total_count(download_data.total_count)
        
        # 获取输出目录配置
        output_config = self.config_manager.get_output_config()
        json_dir = Path(output_config["json_dir"])
        self.file_utils.ensure_dir(json_dir)
        
        # 初始化合并结构
        merged_by_type = {}  # pattern -> set of stripped addresses (for dedup)
        all_logic_rules = []  # 收集所有逻辑规则
        domain_entries = set()  # 单独收集domain，用于去重并插入开头
        
        # 处理已下载的文本文件
        for i, file_path in enumerate(download_data.text_files, 1):
            self.logger.info(f"🔄 处理文件 ({i}/{len(download_data.text_files)}): {file_path}")
            
            try:
                # 读取文件内容并解析
                df, logic_rules = self.parse_downloaded_file(file_path)
                
                # 收集逻辑规则
                all_logic_rules.extend(logic_rules)
                
                # 过滤df
                filtered_rows = []
                for index, row in df.iterrows():
                    if 'AND' not in str(row.get('pattern', '')):
                        filtered_rows.append(row)
                
                if filtered_rows:
                    df_filtered = pd.DataFrame(filtered_rows)
                    
                    # groupby并合并到merged_by_type
                    if 'pattern' in df_filtered.columns and 'address' in df_filtered.columns:
                        for pattern, addresses in df_filtered.groupby('pattern')['address'].apply(list).to_dict().items():
                            stripped = {str(addr).strip() for addr in addresses}  # set for dedup
                            mapped_pattern = self.MAP_DICT.get(pattern, pattern)  # 映射到标准类型
                            
                            if mapped_pattern == 'domain':
                                domain_entries.update(stripped)
                            else:
                                if mapped_pattern not in merged_by_type:
                                    merged_by_type[mapped_pattern] = set()
                                merged_by_type[mapped_pattern].update(stripped)
                
                converted_data.success_count += 1
                
            except Exception as e:
                self.logger.warning(f"⚠️ 文件处理失败: {file_path} - {str(e)}")
                converted_data.add_error(f"文件处理失败: {file_path} - {str(e)}")
                continue
        
        # 如果有成功处理的文件，构建合并的规则集
        if merged_by_type or all_logic_rules or domain_entries:
            merged_ruleset = {"version": self.config_manager.get_version(), "rules": []}
            
            # 添加非domain规则
            for pattern, values in merged_by_type.items():
                if values:
                    sorted_values = sorted(list(values))  # 可选：排序
                    merged_ruleset["rules"].append({pattern: sorted_values})
            
            # 添加domain（插入开头，去重）
            if domain_entries:
                sorted_domains = sorted(list(domain_entries))
                merged_ruleset["rules"].insert(0, {'domain': sorted_domains})
            
            # 添加逻辑规则（追加到末尾）
            merged_ruleset["rules"].extend(all_logic_rules)
            
            # 生成文件名（使用convert_name）
            file_name = json_dir / f"{convert_name}.json"
            
            # 写入JSON文件
            with open(file_name, 'w', encoding='utf-8') as output_file:
                result_rules_str = json.dumps(self.sort_dict(merged_ruleset), ensure_ascii=False, indent=2)
                result_rules_str = result_rules_str.replace('\\\\', '\\')
                output_file.write(result_rules_str)
            
            converted_data.add_converted_file(str(file_name), "")
            self.logger.info(f"✅ 转换完成（合并到单个文件）: {file_name}")
        else:
            self.logger.error(f"❌ 规则集 {convert_name} 无有效数据")
        
        # 输出转换结果摘要
        if converted_data.is_successful():
            self.logger.info(f"✅ 规则集 {convert_name} 转换完成")
            self.logger.info(f"📊 成功: {converted_data.success_count}/{converted_data.total_count}")
            self.logger.info(f"📄 JSON文件: {len(converted_data.json_files)} 个")
        else:
            self.logger.error(f"❌ 规则集 {convert_name} 转换失败")
        
        # 输出错误信息
        for error in converted_data.errors:
            self.logger.warning(f"⚠️ {error}")
        
        return converted_data
    
    def parse_downloaded_file(self, file_path: str) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        解析已下载的文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            (DataFrame数据, 逻辑规则列表)
        """
        # 读取文件内容
        content_lines = self.file_utils.read_text_file(file_path)
        
        # 判断文件类型并解析
        if file_path.endswith('.yaml') or file_path.endswith('.yml'):
            # YAML文件处理
            yaml_content = '\n'.join(content_lines)
            yaml_data = yaml.safe_load(yaml_content)
            
            rows = []
            if not isinstance(yaml_data, str):
                items = yaml_data.get('payload', []) if isinstance(yaml_data, dict) else yaml_data
            else:
                items = yaml_data.splitlines()
            
            for item in items:
                if isinstance(item, str):
                    # 简单处理，假设每行是一个pattern:address对
                    parts = item.split(',', 1)
                    if len(parts) == 2:
                        rows.append({'pattern': parts[0].strip(), 'address': parts[1].strip()})
                    else:
                        rows.append({'pattern': 'domain', 'address': item.strip()})
                elif isinstance(item, dict):
                    for key, value in item.items():
                        if isinstance(value, list):
                            for v in value:
                                rows.append({'pattern': key, 'address': v})
                        else:
                            rows.append({'pattern': key, 'address': value})
            
            df = pd.DataFrame(rows)
            return df, []  # YAML通常没有逻辑规则
        else:
            # 文本文件处理（CSV格式）
            csv_content = StringIO('\n'.join(content_lines))
            df = pd.read_csv(csv_content, header=None, 
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
                if 'AND' not in str(row['pattern']):
                    filtered_rows.append(row)
            
            df_filtered = pd.DataFrame(filtered_rows, columns=['pattern', 'address', 'other', 'other2', 'other3'])
            return df_filtered, rules
    
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
        total_urls = sum(data.total_count for data in results.values())
        successful_urls = sum(data.success_count for data in results.values())
        total_json_files = sum(len(data.json_files) for data in results.values())
        
        return {
            'total_converts': total_converts,
            'successful_converts': successful_converts,
            'total_urls': total_urls,
            'successful_urls': successful_urls,
            'total_json_files': total_json_files,
            'success_rate': (successful_urls / total_urls * 100) if total_urls > 0 else 0
        }

    def sort_dict(self, data: Dict) -> Dict:
        """
        递归排序字典（保持原有逻辑）
        
        Args:
            data: 要排序的字典或数据
            
        Returns:
            排序后的字典
        """
        if isinstance(data, dict):
            return {k: self.sort_dict(v) for k, v in sorted(data.items())}
        elif isinstance(data, list):
            return [self.sort_dict(item) if isinstance(item, (dict, list)) else item for item in data]
        else:
            return data