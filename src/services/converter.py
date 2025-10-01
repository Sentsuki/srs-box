"""
转换服务
处理convert配置中的链接，使用原有的转换逻辑生成JSON规则集
融入现有的下载-处理-编译架构
"""

import json
import pandas as pd
import re
import yaml
from io import StringIO
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from ..utils.config import ConfigManager
from ..utils.logger import Logger
from ..utils.file_utils import FileUtils
from ..utils.network import NetworkUtils
from .downloader import DownloadedData





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
    

    

    

    

    

    
    def process_convert_data(self, convert_download_results: Dict[str, DownloadedData]) -> Dict[str, Any]:
        """
        处理已下载的convert数据，转换为JSON格式并返回ProcessedData格式
        
        Args:
            convert_download_results: convert数据的下载结果
            
        Returns:
            转换后的处理结果（格式与ProcessedData兼容）
        """
        from .processor import ProcessedData  # 避免循环导入
        
        if not convert_download_results:
            self.logger.info("📋 没有convert数据需要处理")
            return {}
        
        results = {}
        
        self.logger.info(f"🔄 开始处理convert数据: {len(convert_download_results)} 个转换源")
        
        for convert_name, download_data in convert_download_results.items():
            self.logger.info(f"🔄 处理转换源: {convert_name}")
            
            try:
                if not download_data.is_successful():
                    # 创建失败的处理结果
                    failed_result = ProcessedData(convert_name, "", 0, False, f"下载失败: {', '.join(download_data.errors)}")
                    results[convert_name] = failed_result
                    continue
                
                # 获取输出目录配置
                output_config = self.config_manager.get_output_config()
                json_dir = Path(output_config["json_dir"])
                self.file_utils.ensure_dir(json_dir)
                
                # 初始化合并结构
                merged_by_type = {}
                all_logic_rules = []
                domain_entries = set()
                
                # 处理下载的文本文件
                for text_file in download_data.text_files:
                    try:
                        # 根据文件扩展名判断处理方式
                        if text_file.endswith('.yaml') or text_file.endswith('.yml'):
                            yaml_content = self.file_utils.read_text_file(text_file)
                            yaml_data = yaml.safe_load('\n'.join(yaml_content))
                            df, logic_rules = self._process_yaml_data(yaml_data)
                        else:
                            # 处理为列表文件
                            df, logic_rules = self._process_text_file(text_file)
                        
                        # 收集逻辑规则
                        all_logic_rules.extend(logic_rules)
                        
                        # 合并规则
                        self._merge_rules_to_dict(df, merged_by_type, domain_entries)
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ 处理文件失败: {text_file} - {str(e)}")
                        continue
                
                # 构建合并的规则集
                if merged_by_type or all_logic_rules or domain_entries:
                    merged_ruleset = {"version": self.config_manager.get_version(), "rules": []}
                    
                    # 添加非domain规则
                    for pattern, values in merged_by_type.items():
                        if values:
                            sorted_values = sorted(list(values))
                            merged_ruleset["rules"].append({pattern: sorted_values})
                    
                    # 添加domain（插入开头）
                    if domain_entries:
                        sorted_domains = sorted(list(domain_entries))
                        merged_ruleset["rules"].insert(0, {'domain': sorted_domains})
                    
                    # 添加逻辑规则
                    merged_ruleset["rules"].extend(all_logic_rules)
                    
                    # 生成JSON文件
                    json_file = json_dir / f"{convert_name}.json"
                    with open(json_file, 'w', encoding='utf-8') as output_file:
                        result_rules_str = json.dumps(self.sort_dict(merged_ruleset), ensure_ascii=False, indent=2)
                        result_rules_str = result_rules_str.replace('\\\\', '\\')
                        output_file.write(result_rules_str)
                    
                    # 统计规则数量
                    rule_count = sum(len(rule_dict) for rule in merged_ruleset["rules"] for rule_dict in [rule] if isinstance(rule_dict, dict))
                    
                    # 创建成功的处理结果
                    success_result = ProcessedData(convert_name, str(json_file), rule_count, True, None)
                    results[convert_name] = success_result
                    
                    self.logger.info(f"✅ 转换源 {convert_name} 处理完成: {rule_count} 条规则")
                else:
                    # 创建失败的处理结果
                    failed_result = ProcessedData(convert_name, "", 0, False, "没有有效的规则数据")
                    results[convert_name] = failed_result
                    self.logger.warning(f"⚠️ 转换源 {convert_name} 没有有效数据")
                
            except Exception as e:
                self.logger.error(f"❌ 转换源 {convert_name} 处理异常: {str(e)}")
                failed_result = ProcessedData(convert_name, "", 0, False, f"处理异常: {str(e)}")
                results[convert_name] = failed_result
        
        # 输出统计
        successful_converts = sum(1 for data in results.values() if data.success)
        self.logger.info(f"✅ convert数据处理完成: {successful_converts}/{len(convert_download_results)} 个转换源成功")
        
        return results
    
    def _process_yaml_data(self, yaml_data: Any) -> Tuple[pd.DataFrame, List[Dict]]:
        """处理YAML数据"""
        rows = []
        if not isinstance(yaml_data, str):
            items = yaml_data.get('payload', [])
        else:
            items = yaml_data.splitlines()
        
        for item in items:
            if isinstance(item, str):
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
    
    def _process_text_file(self, text_file: str) -> Tuple[pd.DataFrame, List[Dict]]:
        """处理文本文件"""
        csv_content = self.file_utils.read_text_file(text_file)
        csv_data = StringIO('\n'.join(csv_content))
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
    
    def _merge_rules_to_dict(self, df: pd.DataFrame, merged_by_type: Dict, domain_entries: set) -> None:
        """将DataFrame中的规则合并到字典中"""
        for pattern, addresses in df.groupby('pattern')['address'].apply(list).to_dict().items():
            stripped = {addr.strip() for addr in addresses}
            mapped_pattern = self.MAP_DICT.get(pattern, pattern)
            
            if mapped_pattern == 'domain':
                domain_entries.update(stripped)
            else:
                if mapped_pattern not in merged_by_type:
                    merged_by_type[mapped_pattern] = set()
                merged_by_type[mapped_pattern].update(stripped)
    


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