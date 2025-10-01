"""
应用主逻辑类
协调各个服务，控制下载-处理-编译的整体流程
实现统一的错误处理和状态管理
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

from .utils.config import ConfigManager
from .utils.logger import Logger
from .utils.network import NetworkUtils
from .utils.file_utils import FileUtils
from .services.downloader import DownloadService, DownloadedData
from .services.processor import ProcessorService, ProcessedData
from .services.compiler import CompilerService, CompileResult
from .services.converter import ConverterService, ConvertedData


class ExecutionSummary:
    """执行摘要类"""
    
    def __init__(self):
        self.total_rulesets = 0
        self.successful_downloads = 0
        self.successful_processes = 0
        self.successful_compiles = 0
        self.total_rules = 0
        self.total_output_size = 0
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, error: str) -> None:
        """添加错误信息"""
        self.errors.append(error)
    
    def add_warning(self, warning: str) -> None:
        """添加警告信息"""
        self.warnings.append(warning)


class RulesetGenerator:
    """规则集生成器主应用类"""
    
    def __init__(self, config_path: str = "config.json"):
        """
        初始化规则集生成器
        
        Args:
            config_path: 配置文件路径，默认为 config.json
        """
        self.config_path = config_path
        
        # 初始化配置管理器
        self.config_manager = ConfigManager(config_path)
        
        # 加载配置并初始化日志系统
        config = self.config_manager.load_config()
        logging_config = self.config_manager.get_logging_config()
        
        # 初始化日志系统
        from .utils.logger import LogLevel
        log_level = LogLevel.from_string(logging_config.get("level", "INFO"))
        self.logger = Logger(
            enable_color=logging_config.get("enable_color", True),
            log_level=log_level,
            show_progress=logging_config.get("show_progress", True)
        )
        
        # 初始化其他工具类
        self.file_utils = FileUtils()
        self.network_utils = NetworkUtils()
        
        # 初始化服务类
        self.download_service = DownloadService(
            self.config_manager, self.logger, self.network_utils, self.file_utils
        )
        self.processor_service = ProcessorService(
            self.config_manager, self.logger, self.file_utils
        )
        self.compiler_service = CompilerService(
            self.config_manager, self.logger, self.network_utils, self.file_utils
        )
        self.converter_service = ConverterService(
            self.config_manager, self.logger, self.network_utils, self.file_utils
        )
        
        # 执行结果存储
        self.download_results: Dict[str, DownloadedData] = {}
        self.process_results: Dict[str, ProcessedData] = {}
        self.compile_results: Dict[str, CompileResult] = {}
        self.convert_results: Dict[str, ConvertedData] = {}
        
        # 执行摘要
        self.summary = ExecutionSummary()
    
    def _load_and_validate_config(self) -> bool:
        """
        加载并验证配置文件
        
        Returns:
            是否加载成功
        """
        try:
            self.logger.info("📋 正在加载配置文件...")
            config = self.config_manager.load_config()
            
            rulesets = self.config_manager.get_rulesets()
            convert_config = config.get('convert', {})
            sing_box_config = self.config_manager.get_sing_box_config()
            
            # 计算总规则集数量（rulesets + convert）
            total_rulesets = len(rulesets) + len(convert_config)
            
            self.logger.success("✅ 配置文件加载成功")
            self.logger.info(f"📊 配置信息:")
            self.logger.info(f"   rulesets规则集: {len(rulesets)} 个")
            self.logger.info(f"   convert规则集: {len(convert_config)} 个")
            self.logger.info(f"   总规则集数量: {total_rulesets} 个")
            self.logger.info(f"   sing-box版本: {sing_box_config['version']}")
            self.logger.info(f"   平台: {sing_box_config['platform']}")
            
            # 显示rulesets规则集详情
            if rulesets:
                self.logger.info(f"📋 rulesets规则集详情:")
                for name, urls in rulesets.items():
                    self.logger.info(f"   - {name}: {len(urls)} 个数据源")
            
            # 显示convert规则集详情
            if convert_config:
                self.logger.info(f"📋 convert规则集详情:")
                for name, urls in convert_config.items():
                    self.logger.info(f"   - {name}: {len(urls)} 个数据源")
            
            self.summary.total_rulesets = total_rulesets
            return True
            
        except FileNotFoundError:
            self.logger.error(f"❌ 配置文件不存在: {self.config_path}")
            self.summary.add_error(f"配置文件不存在: {self.config_path}")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 配置文件加载失败: {str(e)}")
            self.summary.add_error(f"配置文件加载失败: {str(e)}")
            return False
    
    def download_phase(self) -> bool:
        """
        执行下载阶段 - 统一下载所有规则集（rulesets + convert）
        
        Returns:
            是否有成功的下载
        """
        try:
            self.logger.separator("开始下载阶段")
            
            # 获取所有配置
            config = self.config_manager.load_config()
            rulesets = self.config_manager.get_rulesets()
            convert_config = config.get('convert', {})
            
            # 合并所有规则集配置
            all_rulesets = {}
            all_rulesets.update(rulesets)  # 添加rulesets
            all_rulesets.update(convert_config)  # 添加convert
            
            self.logger.info(f"📋 开始下载所有规则集: rulesets({len(rulesets)}) + convert({len(convert_config)}) = {len(all_rulesets)} 个")
            
            # 执行统一下载
            self.download_results = self.download_service.download_all_rulesets_unified(all_rulesets)
            
            # 统计结果
            successful_downloads = sum(
                1 for data in self.download_results.values() if data.is_successful()
            )
            
            self.summary.successful_downloads = successful_downloads
            
            if successful_downloads == 0:
                self.logger.error("❌ 没有成功下载任何规则集")
                self.summary.add_error("没有成功下载任何规则集")
                return False
            
            # 记录失败的下载
            failed_downloads = []
            for name, data in self.download_results.items():
                if not data.is_successful():
                    failed_downloads.append(name)
                    for error in data.errors:
                        self.summary.add_warning(f"规则集 {name}: {error}")
            
            if failed_downloads:
                self.summary.add_warning(f"部分规则集下载失败: {', '.join(failed_downloads)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 下载阶段异常: {str(e)}")
            self.summary.add_error(f"下载阶段异常: {str(e)}")
            return False
    
    def process_phase(self) -> bool:
        """
        执行处理阶段 - 只处理rulesets类型的规则集
        
        Returns:
            是否有成功的处理
        """
        try:
            self.logger.separator("开始处理阶段")
            
            # 获取rulesets配置，只处理rulesets类型的下载结果
            rulesets = self.config_manager.get_rulesets()
            
            # 过滤出rulesets类型的下载结果
            rulesets_download_results = {
                name: data for name, data in self.download_results.items() 
                if name in rulesets
            }
            
            self.logger.info(f"📋 处理rulesets规则集: {len(rulesets_download_results)} 个")
            
            # 执行处理
            self.process_results = self.processor_service.process_all_rulesets(rulesets_download_results)
            
            # 统计结果
            successful_processes = sum(
                1 for data in self.process_results.values() if data.success
            )
            
            self.summary.successful_processes = successful_processes
            
            if len(rulesets) > 0 and successful_processes == 0:
                self.logger.error("❌ 没有成功处理任何rulesets规则集")
                self.summary.add_error("没有成功处理任何rulesets规则集")
                return False
            
            # 统计规则数量
            total_rules = sum(
                data.rule_count for data in self.process_results.values() if data.success
            )
            self.summary.total_rules = total_rules
            
            # 记录失败的处理
            failed_processes = []
            for name, data in self.process_results.items():
                if not data.success:
                    failed_processes.append(name)
                    if data.error:
                        self.summary.add_warning(f"rulesets规则集 {name}: {data.error}")
            
            if failed_processes:
                self.summary.add_warning(f"部分rulesets规则集处理失败: {', '.join(failed_processes)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 处理阶段异常: {str(e)}")
            self.summary.add_error(f"处理阶段异常: {str(e)}")
            return False
    
    def compile_phase(self) -> bool:
        """
        执行编译阶段
        
        Returns:
            是否有成功的编译
        """
        try:
            self.logger.separator("开始编译阶段")
            
            # 执行编译（包括rulesets处理的和convert转换的JSON文件）
            self.compile_results = self.compiler_service.compile_all_rulesets(
                self.process_results, self.convert_results
            )
            
            # 统计结果
            successful_compiles = sum(
                1 for result in self.compile_results.values() if result.success
            )
            
            self.summary.successful_compiles = successful_compiles
            
            if successful_compiles == 0:
                self.logger.error("❌ 没有成功编译任何规则集")
                self.summary.add_error("没有成功编译任何规则集")
                return False
            
            # 统计输出文件大小
            total_size = sum(
                result.file_size for result in self.compile_results.values() if result.success
            )
            self.summary.total_output_size = total_size
            
            # 记录失败的编译
            failed_compiles = []
            for name, result in self.compile_results.items():
                if not result.success:
                    failed_compiles.append(name)
                    if result.error:
                        self.summary.add_warning(f"规则集 {name}: {result.error}")
            
            if failed_compiles:
                self.summary.add_warning(f"部分规则集编译失败: {', '.join(failed_compiles)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 编译阶段异常: {str(e)}")
            self.summary.add_error(f"编译阶段异常: {str(e)}")
            return False
    
    def convert_phase(self) -> bool:
        """
        执行转换阶段 - 只转换convert类型的规则集
        
        Returns:
            是否有成功的转换
        """
        try:
            self.logger.separator("开始转换阶段")
            
            # 获取convert配置
            config = self.config_manager.load_config()
            convert_config = config.get('convert', {})
            
            # 如果没有convert配置，跳过此阶段
            if not convert_config:
                self.logger.info("📋 没有convert配置，跳过转换阶段")
                return True  # 没有convert配置不算失败
            
            # 过滤出convert类型的下载结果
            convert_download_results = {
                name: data for name, data in self.download_results.items() 
                if name in convert_config
            }
            
            self.logger.info(f"📋 转换convert规则集: {len(convert_download_results)} 个")
            
            # 执行转换（使用已下载的数据）
            self.convert_results = self.converter_service.convert_all_rulesets_from_downloads(convert_download_results)
            
            # 统计结果
            successful_converts = sum(
                1 for data in self.convert_results.values() if data.is_successful()
            )
            
            if successful_converts == 0:
                self.logger.error("❌ 没有成功转换任何convert规则集")
                self.summary.add_error("没有成功转换任何convert规则集")
                return False
            
            # 记录失败的转换
            failed_converts = []
            for name, data in self.convert_results.items():
                if not data.is_successful():
                    failed_converts.append(name)
                    for error in data.errors:
                        self.summary.add_warning(f"convert规则集 {name}: {error}")
            
            if failed_converts:
                self.summary.add_warning(f"部分convert规则集转换失败: {', '.join(failed_converts)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 转换阶段异常: {str(e)}")
            self.summary.add_error(f"转换阶段异常: {str(e)}")
            return False
    
    def cleanup_phase(self) -> None:
        """
        执行清理阶段
        """
        try:
            self.logger.separator("开始清理阶段")
            
            # 清理临时文件
            self.download_service.cleanup_temp_files()
            
            # 清理sing-box文件
            self.compiler_service.cleanup_sing_box()
            
            self.logger.success("✅ 清理完成")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 清理阶段出现问题: {str(e)}")
            self.summary.add_warning(f"清理阶段出现问题: {str(e)}")
    
    def show_summary(self) -> None:
        """
        显示执行摘要
        """
        self.logger.separator("执行摘要")
        
        # 基本统计
        self.logger.info(f"📊 执行统计:")
        self.logger.info(f"   总规则集数量: {self.summary.total_rulesets}")
        self.logger.info(f"   成功下载: {self.summary.successful_downloads}")
        self.logger.info(f"   成功处理: {self.summary.successful_processes}")
        self.logger.info(f"   成功编译: {self.summary.successful_compiles}")
        
        if self.summary.total_rules > 0:
            self.logger.info(f"   总规则数量: {self.summary.total_rules:,}")
        
        if self.summary.total_output_size > 0:
            formatted_size = self.file_utils.format_file_size(self.summary.total_output_size)
            self.logger.info(f"   输出文件总大小: {formatted_size}")
        
        # 显示转换统计
        if self.convert_results:
            convert_stats = self.get_convert_statistics()
            self.logger.info(f"   转换规则集: {convert_stats['successful_converts']}/{convert_stats['total_converts']}")
            self.logger.info(f"   转换链接: {convert_stats['successful_urls']}/{convert_stats['total_urls']}")
            self.logger.info(f"   生成JSON文件: {convert_stats['total_json_files']}")
        
        # 显示生成的文件
        self._show_generated_files()
        
        # 显示警告和错误
        if self.summary.warnings:
            self.logger.info(f"\n⚠️ 警告信息 ({len(self.summary.warnings)} 个):")
            for warning in self.summary.warnings:
                self.logger.warning(f"   {warning}")
        
        if self.summary.errors:
            self.logger.info(f"\n❌ 错误信息 ({len(self.summary.errors)} 个):")
            for error in self.summary.errors:
                self.logger.error(f"   {error}")
        
        # 最终状态
        self.logger.info("")  # 添加空行分隔
        if self.summary.successful_compiles > 0:
            self.logger.success(f"🎉 规则集生成完成！成功生成 {self.summary.successful_compiles} 个规则集")
        else:
            self.logger.error(f"💥 规则集生成失败！没有成功生成任何规则集")
    
    def _show_generated_files(self) -> None:
        """
        显示生成的文件信息
        """
        self.logger.info(f"\n📁 生成的文件:")
        
        # 获取输出目录配置
        output_config = self.config_manager.get_output_config()
        json_dir = output_config["json_dir"]
        srs_dir = output_config["srs_dir"]
        
        # 检查所有可能的输出文件
        rulesets = self.config_manager.get_rulesets()
        
        for ruleset_name in rulesets.keys():
            json_file = Path(json_dir) / f"{ruleset_name}.json"
            srs_file = Path(srs_dir) / f"{ruleset_name}.srs"
            
            # 检查JSON文件
            if json_file.exists():
                size = json_file.stat().st_size
                formatted_size = self.file_utils.format_file_size(size)
                self.logger.info(f"   ✓ {json_file} ({formatted_size})")
            else:
                self.logger.info(f"   ✗ {json_file} (未找到)")
            
            # 检查SRS文件
            if srs_file.exists():
                size = srs_file.stat().st_size
                formatted_size = self.file_utils.format_file_size(size)
                self.logger.info(f"   ✓ {srs_file} ({formatted_size})")
            else:
                self.logger.info(f"   ✗ {srs_file} (未找到)")
        
        # 检查转换生成的文件
        if self.convert_results:
            self.logger.info(f"\n📁 转换生成的JSON文件:")
            for convert_name, convert_data in self.convert_results.items():
                if convert_data.is_successful():
                    self.logger.info(f"   📂 {convert_name}:")
                    for json_file in convert_data.json_files:
                        if Path(json_file).exists():
                            size = Path(json_file).stat().st_size
                            formatted_size = self.file_utils.format_file_size(size)
                            self.logger.info(f"     ✓ {json_file} ({formatted_size})")
        
        # 检查所有编译生成的SRS文件
        if self.compile_results:
            self.logger.info(f"\n📁 编译生成的SRS文件:")
            for task_name, compile_result in self.compile_results.items():
                if compile_result.success and compile_result.output_file:
                    if Path(compile_result.output_file).exists():
                        size = Path(compile_result.output_file).stat().st_size
                        formatted_size = self.file_utils.format_file_size(size)
                        self.logger.info(f"   ✓ {compile_result.output_file} ({formatted_size})")
    
    def run(self) -> bool:
        """
        运行完整的规则集生成流程
        
        Returns:
            是否成功完成
        """
        try:
            # 显示启动信息
            self.logger.separator()
            self.logger.info("🌏 srs-box 规则集生成器启动")
            self.logger.info("根据配置文件动态生成sing-box规则集")
            self.logger.separator()
            
            # 1. 加载配置
            if not self._load_and_validate_config():
                return False
            
            # 2. 下载阶段（统一下载所有规则集）
            if not self.download_phase():
                return False
            
            # 3. 处理阶段（处理rulesets）
            if not self.process_phase():
                return False
            
            # 4. 转换阶段（转换convert）
            if not self.convert_phase():
                return False
            
            # 5. 编译阶段
            if not self.compile_phase():
                return False
            
            # 6. 清理阶段
            self.cleanup_phase()
            
            # 7. 显示摘要
            self.show_summary()
            
            # 判断整体是否成功
            return self.summary.successful_compiles > 0
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ 用户中断执行")
            self.summary.add_warning("用户中断执行")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 程序执行异常: {str(e)}")
            self.summary.add_error(f"程序执行异常: {str(e)}")
            return False
    
    def get_execution_summary(self) -> ExecutionSummary:
        """
        获取执行摘要
        
        Returns:
            执行摘要对象
        """
        return self.summary
    
    def get_download_statistics(self) -> Dict[str, Any]:
        """
        获取下载统计信息
        
        Returns:
            下载统计字典
        """
        return self.download_service.get_download_statistics(self.download_results)
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            处理统计字典
        """
        return self.processor_service.get_processing_statistics(self.process_results)
    
    def get_compile_statistics(self) -> Dict[str, Any]:
        """
        获取编译统计信息
        
        Returns:
            编译统计字典
        """
        return self.compiler_service.get_compile_statistics(self.compile_results)
    
    def get_convert_statistics(self) -> Dict[str, Any]:
        """
        获取转换统计信息
        
        Returns:
            转换统计字典
        """
        return self.converter_service.get_convert_statistics(self.convert_results)