"""
处理服务
实现JSON规则集合并、IP列表处理、规则过滤功能
优化内存使用，支持大文件处理
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Generator, Tuple

from ..utils.config import ConfigManager
from ..utils.logger import Logger
from ..utils.file_utils import FileUtils
from .downloader import DownloadedData


class ProcessedData:
    """处理后的数据结果类"""
    
    def __init__(self, ruleset_name: str):
        self.ruleset_name = ruleset_name
        self.ruleset_data: Optional[Dict[str, Any]] = None
        self.output_file: Optional[str] = None
        self.rule_count = 0
        self.rule_types: List[str] = []
        self.filtered_count = 0
        self.success = False
        self.error: Optional[str] = None
    
    def set_success(self, ruleset_data: Dict[str, Any], output_file: str, 
                   rule_count: int, rule_types: List[str], filtered_count: int = 0) -> None:
        """设置成功结果"""
        self.ruleset_data = ruleset_data
        self.output_file = output_file
        self.rule_count = rule_count
        self.rule_types = rule_types
        self.filtered_count = filtered_count
        self.success = True
    
    def set_error(self, error: str) -> None:
        """设置错误结果"""
        self.error = error
        self.success = False


class ProcessorService:
    """处理服务类"""
    
    def __init__(self, config_manager: ConfigManager, logger: Logger, file_utils: FileUtils):
        """
        初始化处理服务
        
        Args:
            config_manager: 配置管理器
            logger: 日志记录器
            file_utils: 文件工具
        """
        self.config_manager = config_manager
        self.logger = logger
        self.file_utils = file_utils
        
        # 过滤关键词配置
        self.filter_keywords = ['ruleset.skk.moe']
    
    def should_filter_rule_value(self, value: str) -> bool:
        """
        检查规则值是否应该被过滤掉
        
        Args:
            value: 规则值
            
        Returns:
            是否应该过滤
        """
        if not isinstance(value, str):
            return False
        
        value_lower = value.lower()
        return any(keyword in value_lower for keyword in self.filter_keywords)
    
    def filter_rules(self, rules: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        过滤规则列表，移除包含特定关键字的规则
        
        Args:
            rules: 规则列表
            
        Returns:
            (过滤后的规则列表, 被过滤的规则数量)
        """
        filtered_rules = []
        filtered_count = 0
        
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            
            filtered_rule = {}
            
            for rule_type, rule_values in rule.items():
                if not isinstance(rule_values, list):
                    continue
                
                # 过滤规则值
                original_count = len(rule_values)
                filtered_values = [
                    value for value in rule_values 
                    if not self.should_filter_rule_value(value)
                ]
                filtered_count += original_count - len(filtered_values)
                
                # 只添加非空的规则
                if filtered_values:
                    filtered_rule[rule_type] = filtered_values
            
            # 只添加非空的规则对象
            if filtered_rule:
                filtered_rules.append(filtered_rule)
        
        return filtered_rules, filtered_count
    
    def merge_json_rulesets(self, json_data_list: List[Dict[str, Any]], 
                           config_version: int) -> Dict[str, Any]:
        """
        智能合并多个JSON规则集，将相同类型的规则合并在一起
        
        Args:
            json_data_list: JSON数据列表
            config_version: 配置版本号
            
        Returns:
            合并后的规则集
        """
        # 用于存储合并后的规则，按规则类型分组
        rule_groups: Dict[str, Set[str]] = {}
        
        for json_data in json_data_list:
            rules = []
            
            # 提取规则列表
            if 'rules' in json_data and isinstance(json_data['rules'], list):
                rules = json_data['rules']
            else:
                # 如果JSON结构不标准，尝试直接作为规则处理
                rules = [json_data]
            
            # 处理每个规则
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                
                # 遍历规则中的每个字段
                for rule_type, rule_values in rule.items():
                    if not isinstance(rule_values, list):
                        continue
                    
                    # 如果这个规则类型还没有，创建新的集合
                    if rule_type not in rule_groups:
                        rule_groups[rule_type] = set()
                    
                    # 合并规则值，自动去重
                    for value in rule_values:
                        if isinstance(value, str):
                            rule_groups[rule_type].add(value)
        
        # 将分组的规则转换为最终格式
        merged_rules = []
        for rule_type, rule_values in rule_groups.items():
            if rule_values:  # 只添加非空的规则
                # 转换为列表并排序（保证输出一致性）
                sorted_values = sorted(list(rule_values))
                merged_rules.append({rule_type: sorted_values})
        
        # 创建合并后的规则集
        merged_ruleset = {
            "version": config_version,
            "rules": merged_rules
        }
        
        return merged_ruleset
    
    def create_ip_ruleset_from_text_files(self, text_files: List[str], 
                                         config_version: int) -> Dict[str, Any]:
        """
        从文本文件创建IP规则集
        
        Args:
            text_files: 文本文件路径列表
            config_version: 配置版本号
            
        Returns:
            IP规则集数据
        """
        ip_list = []
        
        # 使用生成器处理大文件，优化内存使用
        def read_ip_lines() -> Generator[str, None, None]:
            for file_path in text_files:
                try:
                    lines = self.file_utils.read_text_file(file_path)
                    for line in lines:
                        cleaned_line = line.strip()
                        if cleaned_line and not cleaned_line.startswith('#'):
                            yield cleaned_line
                except Exception as e:
                    self.logger.warning(f"⚠️ 读取文件失败: {file_path} - {str(e)}")
        
        # 收集所有IP，使用集合自动去重
        ip_set = set()
        for ip in read_ip_lines():
            ip_set.add(ip)
        
        # 转换为排序列表
        ip_list = sorted(list(ip_set))
        
        # 创建规则集
        ruleset = {
            "version": config_version,
            "rules": [
                {
                    "ip_cidr": ip_list
                }
            ]
        }
        
        return ruleset
    
    def process_ruleset(self, ruleset_name: str, downloaded_data: DownloadedData) -> ProcessedData:
        """
        处理单个规则集的下载数据
        
        Args:
            ruleset_name: 规则集名称
            downloaded_data: 下载的数据
            
        Returns:
            处理结果
        """
        self.logger.info(f"🔄 开始处理规则集: {ruleset_name}")
        
        processed_data = ProcessedData(ruleset_name)
        
        try:
            config_version = self.config_manager.get_version()
            
            # 优先处理JSON数据
            if downloaded_data.has_json_data():
                self.logger.info(f"📄 处理JSON规则集数据: {len(downloaded_data.json_data)} 个")
                
                if len(downloaded_data.json_data) == 1:
                    # 只有一个JSON文件，直接使用
                    ruleset_data = downloaded_data.json_data[0]
                    self.logger.info("📋 使用单个JSON规则集")
                else:
                    # 多个JSON文件，需要合并
                    self.logger.info(f"🔀 合并 {len(downloaded_data.json_data)} 个JSON规则集")
                    ruleset_data = self.merge_json_rulesets(downloaded_data.json_data, config_version)
                
                # 过滤规则
                if 'rules' in ruleset_data and isinstance(ruleset_data['rules'], list):
                    original_rules = ruleset_data['rules']
                    filtered_rules, filtered_count = self.filter_rules(original_rules)
                    ruleset_data['rules'] = filtered_rules
                    
                    if filtered_count > 0:
                        self.logger.info(f"🚫 已过滤 {filtered_count} 条包含过滤关键字的规则")
                else:
                    filtered_count = 0
                
                # 统计规则信息
                rule_count = 0
                rule_types = []
                
                for rule in ruleset_data.get('rules', []):
                    for rule_type, rule_values in rule.items():
                        if isinstance(rule_values, list):
                            rule_types.append(f"{rule_type}({len(rule_values)})")
                            rule_count += len(rule_values)
                
                self.logger.success(f"✅ JSON规则集处理完成")
                self.logger.info(f"📊 规则统计: {', '.join(rule_types)}，总计 {rule_count} 条规则")
                
            elif downloaded_data.has_text_files():
                # 处理文本文件（IP列表）
                self.logger.info(f"📄 处理文本文件: {len(downloaded_data.text_files)} 个")
                
                ruleset_data = self.create_ip_ruleset_from_text_files(
                    downloaded_data.text_files, 
                    config_version
                )
                
                # 统计IP数量
                rule_count = 0
                rule_types = []
                filtered_count = 0
                
                for rule in ruleset_data.get('rules', []):
                    for rule_type, rule_values in rule.items():
                        if isinstance(rule_values, list):
                            rule_types.append(f"{rule_type}({len(rule_values)})")
                            rule_count += len(rule_values)
                
                self.logger.success(f"✅ 文本规则集处理完成")
                self.logger.info(f"📊 规则统计: {', '.join(rule_types)}，总计 {rule_count} 条规则")
                
            else:
                # 没有可处理的数据
                processed_data.set_error("没有可处理的下载数据")
                return processed_data
            
            # 保存处理后的规则集
            output_file = f"{ruleset_name}.json"
            self.file_utils.write_json_file(output_file, ruleset_data)
            
            processed_data.set_success(
                ruleset_data, 
                output_file, 
                rule_count, 
                rule_types, 
                filtered_count
            )
            
            self.logger.success(f"✅ 规则集已保存到: {output_file}")
            
        except Exception as e:
            error_msg = f"处理规则集时发生异常: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            processed_data.set_error(error_msg)
        
        return processed_data
    
    def process_all_rulesets(self, download_results: Dict[str, DownloadedData]) -> Dict[str, ProcessedData]:
        """
        处理所有规则集
        
        Args:
            download_results: 下载结果字典
            
        Returns:
            处理结果字典
        """
        results = {}
        
        self.logger.header("开始处理阶段")
        
        # 只处理成功下载的规则集
        successful_downloads = {
            name: data for name, data in download_results.items() 
            if data.is_successful()
        }
        
        if not successful_downloads:
            self.logger.warning("⚠️ 没有成功下载的规则集需要处理")
            return results
        
        self.logger.info(f"📋 需要处理 {len(successful_downloads)} 个规则集")
        
        for i, (ruleset_name, downloaded_data) in enumerate(successful_downloads.items(), 1):
            self.logger.step(f"处理规则集: {ruleset_name}", i, len(successful_downloads))
            
            try:
                processed_data = self.process_ruleset(ruleset_name, downloaded_data)
                results[ruleset_name] = processed_data
                
            except Exception as e:
                self.logger.error(f"❌ 规则集 {ruleset_name} 处理异常: {str(e)}")
                # 创建失败的处理数据
                failed_data = ProcessedData(ruleset_name)
                failed_data.set_error(f"处理异常: {str(e)}")
                results[ruleset_name] = failed_data
            
            # 添加分隔线（除了最后一个）
            if i < len(successful_downloads):
                self.logger.info("─" * 50)
        
        # 输出总体统计
        successful_processed = sum(1 for data in results.values() if data.success)
        self.logger.separator("处理阶段完成")
        self.logger.success(f"✅ 处理完成: {successful_processed}/{len(successful_downloads)} 个规则集成功")
        
        return results
    
    def get_processing_statistics(self, results: Dict[str, ProcessedData]) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Args:
            results: 处理结果字典
            
        Returns:
            统计信息字典
        """
        total_rulesets = len(results)
        successful_rulesets = sum(1 for data in results.values() if data.success)
        total_rules = sum(data.rule_count for data in results.values() if data.success)
        total_filtered = sum(data.filtered_count for data in results.values() if data.success)
        
        # 统计规则类型
        rule_type_counts = {}
        for data in results.values():
            if data.success:
                for rule_type in data.rule_types:
                    # 提取规则类型名称（去掉数量）
                    type_name = rule_type.split('(')[0]
                    if type_name not in rule_type_counts:
                        rule_type_counts[type_name] = 0
                    rule_type_counts[type_name] += 1
        
        return {
            'total_rulesets': total_rulesets,
            'successful_rulesets': successful_rulesets,
            'total_rules': total_rules,
            'total_filtered': total_filtered,
            'rule_type_counts': rule_type_counts,
            'success_rate': (successful_rulesets / total_rulesets * 100) if total_rulesets > 0 else 0
        }