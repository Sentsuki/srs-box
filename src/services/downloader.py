"""
下载服务
处理不同类型数据源（JSON规则集、文本IP列表）的下载
集成网络工具和文件工具，消除重复代码
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..utils.config import ConfigManager
from ..utils.logger import Logger
from ..utils.network import NetworkUtils, DownloadResult
from ..utils.file_utils import FileUtils


class DownloadedData:
    """下载数据结果类"""
    
    def __init__(self, ruleset_name: str):
        self.ruleset_name = ruleset_name
        self.json_data: List[Dict[str, Any]] = []
        self.text_files: List[str] = []
        self.success_count = 0
        self.total_count = 0
        self.errors: List[str] = []
    
    def add_json_data(self, data: Dict[str, Any]) -> None:
        """添加JSON数据"""
        self.json_data.append(data)
        self.success_count += 1
    
    def add_text_file(self, file_path: str) -> None:
        """添加文本文件路径"""
        self.text_files.append(file_path)
        self.success_count += 1
    
    def add_error(self, error: str) -> None:
        """添加错误信息"""
        self.errors.append(error)
    
    def set_total_count(self, count: int) -> None:
        """设置总数量"""
        self.total_count = count
    
    def is_successful(self) -> bool:
        """是否有成功下载的数据"""
        return self.success_count > 0
    
    def has_json_data(self) -> bool:
        """是否有JSON数据"""
        return len(self.json_data) > 0
    
    def has_text_files(self) -> bool:
        """是否有文本文件"""
        return len(self.text_files) > 0


class DownloadService:
    """下载服务类"""
    
    def __init__(self, config_manager: ConfigManager, logger: Logger, 
                 network_utils: NetworkUtils, file_utils: FileUtils):
        """
        初始化下载服务
        
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
        
        # 创建临时目录
        self.temp_dir = Path("temp")
        self.file_utils.ensure_dir(self.temp_dir)
        
        # 显示缓存信息
        cache_info = self.network_utils.get_cache_info()
        if cache_info['total_files'] > 0:
            self.logger.info(f"💾 缓存状态: {cache_info['total_files']} 个文件, "
                           f"{cache_info['total_size_mb']:.2f} MB")
        
        # 清理过期缓存
        cleared = self.network_utils.clear_cache(older_than_hours=48)  # 清理48小时前的缓存
        if cleared > 0:
            self.logger.info(f"🧹 已清理 {cleared} 个过期缓存文件")
    
    def is_json_ruleset(self, url: str) -> bool:
        """
        检查URL是否为JSON规则集
        
        Args:
            url: 要检查的URL
            
        Returns:
            是否为JSON规则集
        """
        return self.network_utils.is_json_url(url)
    
    def download_json_rulesets(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        下载JSON规则集列表
        
        Args:
            urls: JSON规则集URL列表
            
        Returns:
            成功下载的JSON数据列表
        """
        json_data_list = []
        
        for i, url in enumerate(urls, 1):
            self.logger.info(f"下载JSON规则集 ({i}/{len(urls)}): {url}")
            
            json_data = self.network_utils.download_json(url)
            if json_data:
                json_data_list.append(json_data)
                self.logger.info(f"✅ JSON规则集下载成功")
            else:
                self.logger.warning(f"⚠️ JSON规则集下载失败: {url}")
        
        return json_data_list
    
    def download_text_rulesets(self, urls: List[str], temp_dir: Path) -> List[str]:
        """
        下载文本规则集列表，使用优化的并发下载
        
        Args:
            urls: 文本规则集URL列表
            temp_dir: 临时目录
            
        Returns:
            成功下载的文件路径列表
        """
        # 准备下载任务
        download_tasks = []
        for i, url in enumerate(urls, 1):
            filename = self.network_utils.get_filename_from_url(url)
            if not filename.endswith('.txt'):
                filename = f"file_{i}.txt"
            
            output_path = temp_dir / filename
            download_tasks.append((url, output_path))
        
        # 并发下载with enhanced progress and speed display
        self.logger.info(f"🚀 开始并发下载 {len(urls)} 个文本文件 (并发数: {self.network_utils.max_concurrent})")
        
        # 进度跟踪变量
        last_progress_time = time.time()
        
        def progress_callback(completed: int, total: int, current_file: str, speed_mbps: float, elapsed_time: float):
            """增强的进度回调，显示速度和统计信息"""
            nonlocal last_progress_time
            current_time = time.time()
            
            # 限制进度更新频率（每0.5秒更新一次）
            if current_time - last_progress_time >= 0.5 or completed == total:
                last_progress_time = current_time
                
                # 构建进度消息
                if speed_mbps > 0:
                    speed_text = f"速度: {speed_mbps:.2f} MB/s"
                else:
                    speed_text = "计算速度中..."
                
                time_text = f"已用时: {elapsed_time:.1f}s"
                progress_msg = f"{speed_text}, {time_text}"
                
                self.logger.progress(completed, total, progress_msg)
        
        # 使用增强的并发下载
        results, stats = self.network_utils.download_multiple_with_stats(
            download_tasks, 
            max_workers=self.network_utils.max_concurrent
        )
        
        # 收集成功下载的文件
        successful_files = []
        failed_urls = []
        
        for result in results:
            if result.success:
                successful_files.append(result.file_path)
            else:
                failed_urls.append(result.url)
                self.logger.warning(f"⚠️ 文件下载失败: {result.url} - {result.error}")
        
        # 显示详细的下载统计
        self.logger.info(f"✅ 文本文件下载完成: {stats['successful_files']}/{stats['total_files']} 成功")
        self.logger.info(f"📊 下载统计:")
        self.logger.info(f"   • 成功率: {stats['success_rate']:.1f}%")
        self.logger.info(f"   • 总大小: {stats['total_size_mb']:.2f} MB")
        self.logger.info(f"   • 总耗时: {stats['total_time_seconds']:.1f} 秒")
        self.logger.info(f"   • 平均速度: {stats['average_speed_mbps']:.2f} MB/s")
        self.logger.info(f"   • 并发数: {stats['max_concurrent']}")
        
        if stats['failed_files'] > 0:
            self.logger.warning(f"⚠️ {stats['failed_files']} 个文件下载失败")
        
        return successful_files
    
    def download_ruleset(self, ruleset_name: str, urls: List[str]) -> DownloadedData:
        """
        下载单个规则集的所有数据源
        
        Args:
            ruleset_name: 规则集名称
            urls: URL列表
            
        Returns:
            下载数据结果
        """
        self.logger.info(f"📥 开始下载规则集: {ruleset_name}")
        self.logger.info(f"📋 数据源数量: {len(urls)}")
        
        # 创建下载结果对象
        downloaded_data = DownloadedData(ruleset_name)
        downloaded_data.set_total_count(len(urls))
        
        # 分类URL
        json_urls = [url for url in urls if self.is_json_ruleset(url)]
        text_urls = [url for url in urls if not self.is_json_ruleset(url)]
        
        self.logger.info(f"📊 JSON规则集: {len(json_urls)} 个, 文本规则集: {len(text_urls)} 个")
        
        # 下载JSON规则集
        if json_urls:
            self.logger.info(f"🔄 开始下载JSON规则集")
            json_data_list = self.download_json_rulesets(json_urls)
            
            for json_data in json_data_list:
                downloaded_data.add_json_data(json_data)
            
            if len(json_data_list) != len(json_urls):
                failed_json = len(json_urls) - len(json_data_list)
                downloaded_data.add_error(f"{failed_json} 个JSON规则集下载失败")
        
        # 下载文本规则集
        if text_urls:
            self.logger.info(f"🔄 开始下载文本规则集")
            
            # 为每个规则集创建独立的临时目录
            ruleset_temp_dir = self.temp_dir / ruleset_name
            self.file_utils.ensure_dir(ruleset_temp_dir)
            
            text_files = self.download_text_rulesets(text_urls, ruleset_temp_dir)
            
            for file_path in text_files:
                downloaded_data.add_text_file(file_path)
            
            if len(text_files) != len(text_urls):
                failed_text = len(text_urls) - len(text_files)
                downloaded_data.add_error(f"{failed_text} 个文本文件下载失败")
        
        # 输出下载结果摘要
        if downloaded_data.is_successful():
            self.logger.info(f"✅ 规则集 {ruleset_name} 下载完成")
            self.logger.info(f"📊 成功: {downloaded_data.success_count}/{downloaded_data.total_count}")
            
            if downloaded_data.has_json_data():
                self.logger.info(f"📄 JSON数据: {len(downloaded_data.json_data)} 个")
            
            if downloaded_data.has_text_files():
                self.logger.info(f"📄 文本文件: {len(downloaded_data.text_files)} 个")
        else:
            self.logger.error(f"❌ 规则集 {ruleset_name} 下载失败")
        
        # 输出错误信息
        for error in downloaded_data.errors:
            self.logger.warning(f"⚠️ {error}")
        
        return downloaded_data
    
    def download_all_rulesets(self) -> Dict[str, DownloadedData]:
        """
        下载所有规则集
        
        Returns:
            规则集名称到下载数据的映射
        """
        rulesets = self.config_manager.get_rulesets()
        results = {}
        
        self.logger.header("开始下载阶段")
        self.logger.info(f"📋 发现 {len(rulesets)} 个规则集")
        
        for i, (ruleset_name, urls) in enumerate(rulesets.items(), 1):
            self.logger.step(f"下载规则集: {ruleset_name}", i, len(rulesets))
            
            try:
                downloaded_data = self.download_ruleset(ruleset_name, urls)
                results[ruleset_name] = downloaded_data
                
            except Exception as e:
                self.logger.error(f"❌ 规则集 {ruleset_name} 下载异常: {str(e)}")
                # 创建失败的下载数据
                failed_data = DownloadedData(ruleset_name)
                failed_data.set_total_count(len(urls))
                failed_data.add_error(f"下载异常: {str(e)}")
                results[ruleset_name] = failed_data
            
            # 添加分隔线（除了最后一个）
            if i < len(rulesets):
                self.logger.info("─" * 50)
        
        # 输出总体统计
        successful_rulesets = sum(1 for data in results.values() if data.is_successful())
        self.logger.separator("下载阶段完成")
        self.logger.success(f"✅ 下载完成: {successful_rulesets}/{len(rulesets)} 个规则集成功")
        
        return results
    
    def cleanup_temp_files(self, keep_patterns: Optional[List[str]] = None) -> None:
        """
        清理临时文件
        
        Args:
            keep_patterns: 要保留的文件模式列表
        """
        if self.temp_dir.exists():
            deleted_count = self.file_utils.cleanup_temp_files(self.temp_dir, keep_patterns)
            if deleted_count > 0:
                self.logger.info(f"🧹 已清理 {deleted_count} 个临时文件")
    
    def get_download_statistics(self, results: Dict[str, DownloadedData]) -> Dict[str, Any]:
        """
        获取下载统计信息
        
        Args:
            results: 下载结果字典
            
        Returns:
            统计信息字典
        """
        total_rulesets = len(results)
        successful_rulesets = sum(1 for data in results.values() if data.is_successful())
        total_sources = sum(data.total_count for data in results.values())
        successful_sources = sum(data.success_count for data in results.values())
        
        json_rulesets = sum(1 for data in results.values() if data.has_json_data())
        text_rulesets = sum(1 for data in results.values() if data.has_text_files())
        
        return {
            'total_rulesets': total_rulesets,
            'successful_rulesets': successful_rulesets,
            'total_sources': total_sources,
            'successful_sources': successful_sources,
            'json_rulesets': json_rulesets,
            'text_rulesets': text_rulesets,
            'success_rate': (successful_sources / total_sources * 100) if total_sources > 0 else 0
        }