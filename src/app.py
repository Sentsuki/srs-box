"""
应用主逻辑类
协调各个服务，控制下载-处理-编译的整体流程
实现统一的错误处理和状态管理
"""

from pathlib import Path
from typing import Any, Dict, List

from .services.compiler import CompileResult, CompilerService
from .services.converter import ConvertedData, ConverterService
from .services.downloader import DownloadedData, DownloadService
from .services.ip_processor import IpProcessedData, IpProcessorService
from .services.processor import ProcessedData, ProcessorService
from .utils.config import ConfigManager
from .utils.file_utils import FileUtils
from .utils.logger import Logger
from .utils.network import NetworkUtils


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
        logging_config = self.config_manager.get_logging_config()

        # 初始化日志系统
        from .utils.logger import LogLevel

        log_level = LogLevel.from_string(logging_config.get("level", "INFO"))
        self.logger = Logger(
            enable_color=logging_config.get("enable_color", True),
            log_level=log_level,
            show_progress=logging_config.get("show_progress", True),
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
        self.ip_processor_service = IpProcessorService(
            self.config_manager, self.logger, self.file_utils
        )

        # 执行结果存储
        self.download_results: Dict[str, DownloadedData] = {}
        self.convert_download_results: Dict[str, DownloadedData] = {}  # convert下载结果
        self.ip_download_results: Dict[str, DownloadedData] = {}  # ip_only下载结果
        self.process_results: Dict[str, ProcessedData] = {}
        self.ip_process_results: Dict[str, IpProcessedData] = {}  # IP处理结果
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
            self.config_manager.load_config()

            ip_only = self.config_manager.get_ip_only()
            rulesets = self.config_manager.get_rulesets()
            sing_box_config = self.config_manager.get_sing_box_config()

            self.logger.success("✅ 配置文件加载成功")
            self.logger.info("📊 配置信息:")
            self.logger.info(f"   IP规则集数量: {len(ip_only)}")
            self.logger.info(f"   JSON规则集数量: {len(rulesets)}")
            self.logger.info(f"   sing-box版本: {sing_box_config['version']}")
            self.logger.info(f"   平台: {sing_box_config['platform']}")

            # 显示 IP 规则集详情
            if ip_only:
                self.logger.info("   🌐 IP规则集:")
                for name, urls in ip_only.items():
                    self.logger.info(f"      - {name}: {len(urls)} 个数据源")

            # 显示JSON规则集详情
            if rulesets:
                self.logger.info("   📄 JSON规则集:")
                for name, urls in rulesets.items():
                    self.logger.info(f"      - {name}: {len(urls)} 个数据源")

            self.summary.total_rulesets = len(ip_only) + len(rulesets)
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
        统一下载阶段 - 同时下载 ip_only、rulesets 和 convert 链接

        Returns:
            是否有成功的下载
        """
        try:
            self.logger.separator("开始统一下载阶段")

            config = self.config_manager.load_config()

            # 1. 下载 ip_only 配置
            ip_only_config = config.get("ip_only", {})
            if ip_only_config:
                self.logger.info("🌐 下载 ip_only 配置")
                self.ip_download_results = self._download_ip_sources(ip_only_config)
            else:
                self.logger.info("📋 没有 ip_only 配置，跳过")
                self.ip_download_results = {}

            # 2. 下载 rulesets 配置
            rulesets_config = config.get("rulesets", {})
            if rulesets_config:
                self.logger.info("📄 下载 rulesets 配置")
                self.download_results = self._download_rulesets_sources(rulesets_config)
            else:
                self.logger.info("📋 没有 rulesets 配置，跳过")
                self.download_results = {}

            # 3. 下载 convert 配置
            convert_config = config.get("convert", {})
            if convert_config:
                self.logger.info("🔄 下载 convert 配置")
                self.convert_download_results = self._download_convert_sources(
                    convert_config
                )
            else:
                self.logger.info("📋 没有 convert 配置，跳过")
                self.convert_download_results = {}

            # 统计下载结果
            successful_ip = sum(
                1 for data in self.ip_download_results.values() if data.is_successful()
            )
            successful_rulesets = sum(
                1 for data in self.download_results.values() if data.is_successful()
            )
            successful_convert = sum(
                1
                for data in self.convert_download_results.values()
                if data.is_successful()
            )

            total_successful = successful_ip + successful_rulesets + successful_convert
            self.summary.successful_downloads = total_successful

            # 检查是否有成功的下载
            if total_successful == 0:
                self.logger.error("❌ 没有成功下载任何数据源")
                self.summary.add_error("没有成功下载任何数据源")
                return False

            # 记录失败的下载
            failed_downloads = []
            for name, data in self.ip_download_results.items():
                if not data.is_successful():
                    failed_downloads.append(f"ip_only:{name}")
                    for error in data.errors:
                        self.summary.add_warning(f"IP规则集 {name}: {error}")

            for name, data in self.download_results.items():
                if not data.is_successful():
                    failed_downloads.append(f"ruleset:{name}")
                    for error in data.errors:
                        self.summary.add_warning(f"规则集 {name}: {error}")

            for name, data in self.convert_download_results.items():
                if not data.is_successful():
                    failed_downloads.append(f"convert:{name}")
                    for error in data.errors:
                        self.summary.add_warning(f"转换配置 {name}: {error}")

            if failed_downloads:
                self.summary.add_warning(
                    f"部分数据源下载失败: {', '.join(failed_downloads)}"
                )

            # 输出统计信息
            total_sources = (
                len(self.ip_download_results)
                + len(self.download_results)
                + len(self.convert_download_results)
            )

            self.logger.separator("统一下载阶段完成")
            self.logger.success(
                f"✅ 下载完成: {total_successful}/{total_sources} 个源成功"
            )
            self.logger.info("📊 详细统计:")
            if ip_only_config:
                self.logger.info(
                    f"   IP规则集: {successful_ip}/{len(self.ip_download_results)} 成功"
                )
            if rulesets_config:
                self.logger.info(
                    f"   JSON规则集: {successful_rulesets}/{len(self.download_results)} 成功"
                )
            if convert_config:
                convert_total = len(self.convert_download_results)
                self.logger.info(
                    f"   Convert: {successful_convert}/{convert_total} 成功"
                )

            return True

        except Exception as e:
            self.logger.error(f"❌ 统一下载阶段异常: {str(e)}")
            self.summary.add_error(f"统一下载阶段异常: {str(e)}")
            return False

    def _download_ip_sources(
        self, ip_config: Dict[str, List[str]]
    ) -> Dict[str, DownloadedData]:
        """
        下载 ip_only 配置中的所有源文件

        Args:
            ip_config: ip_only 配置字典

        Returns:
            IP规则集名称到下载数据的映射
        """
        results = {}

        for ip_name, urls in ip_config.items():
            self.logger.info(f"📥 下载 IP 规则集: {ip_name}")

            try:
                # 使用 download_service 下载 IP 的源文件（按文本模式下载）
                downloaded_data = self.download_service.download_ruleset(
                    f"ip_{ip_name}", urls, download_as="text"
                )
                results[ip_name] = downloaded_data

            except Exception as e:
                self.logger.error(f"❌ IP 规则集 {ip_name} 下载异常: {str(e)}")
                # 创建失败的下载数据
                failed_data = DownloadedData(f"ip_{ip_name}")
                failed_data.set_total_count(len(urls))
                failed_data.add_error(f"下载异常: {str(e)}")
                results[ip_name] = failed_data

        return results

    def _download_rulesets_sources(
        self, rulesets_config: Dict[str, List[str]]
    ) -> Dict[str, DownloadedData]:
        """
        下载 rulesets 配置中的所有源文件

        Args:
            rulesets_config: rulesets 配置字典

        Returns:
            规则集名称到下载数据的映射
        """
        results = {}

        for ruleset_name, urls in rulesets_config.items():
            self.logger.info(f"📥 下载 JSON 规则集: {ruleset_name}")

            try:
                # 使用 download_service 下载规则集的源文件（按 JSON 模式下载）
                downloaded_data = self.download_service.download_ruleset(
                    ruleset_name, urls, download_as="json"
                )
                results[ruleset_name] = downloaded_data

            except Exception as e:
                self.logger.error(f"❌ 规则集 {ruleset_name} 下载异常: {str(e)}")
                # 创建失败的下载数据
                failed_data = DownloadedData(ruleset_name)
                failed_data.set_total_count(len(urls))
                failed_data.add_error(f"下载异常: {str(e)}")
                results[ruleset_name] = failed_data

        return results

    def _download_convert_sources(
        self, convert_config: Dict[str, List[str]]
    ) -> Dict[str, DownloadedData]:
        """
        下载 convert 配置中的所有源文件

        Args:
            convert_config: convert 配置字典

        Returns:
            convert 名称到下载数据的映射
        """
        results = {}

        for convert_name, urls in convert_config.items():
            self.logger.info(f"📥 下载 convert 配置: {convert_name}")

            try:
                # 使用 download_service 下载 convert 的源文件（按文本模式下载）
                downloaded_data = self.download_service.download_ruleset(
                    f"convert_{convert_name}", urls, download_as="text"
                )
                results[convert_name] = downloaded_data

            except Exception as e:
                self.logger.error(f"❌ Convert 配置 {convert_name} 下载异常: {str(e)}")
                # 创建失败的下载数据
                failed_data = DownloadedData(f"convert_{convert_name}")
                failed_data.set_total_count(len(urls))
                failed_data.add_error(f"下载异常: {str(e)}")
                results[convert_name] = failed_data

        return results

    def process_phase(self) -> bool:
        """
        执行JSON规则集处理阶段

        Returns:
            是否有成功的处理
        """
        try:
            # 如果没有 rulesets 下载结果，跳过此阶段
            if not self.download_results:
                self.logger.info("📋 没有JSON规则集需要处理，跳过")
                return True

            self.logger.separator("开始JSON规则集处理阶段")

            # 执行处理
            self.process_results = self.processor_service.process_all_rulesets(
                self.download_results
            )

            # 统计结果
            successful_processes = sum(
                1 for data in self.process_results.values() if data.success
            )

            self.summary.successful_processes = successful_processes

            if successful_processes == 0:
                self.logger.error("❌ 没有成功处理任何JSON规则集")
                self.summary.add_error("没有成功处理任何JSON规则集")
                return False

            # 统计规则数量
            total_rules = sum(
                data.rule_count
                for data in self.process_results.values()
                if data.success
            )
            self.summary.total_rules = total_rules

            # 记录失败的处理
            failed_processes = []
            for name, data in self.process_results.items():
                if not data.success:
                    failed_processes.append(name)
                    if data.error:
                        self.summary.add_warning(f"规则集 {name}: {data.error}")

            if failed_processes:
                self.summary.add_warning(
                    f"部分规则集处理失败: {', '.join(failed_processes)}"
                )

            return True

        except Exception as e:
            self.logger.error(f"❌ JSON规则集处理阶段异常: {str(e)}")
            self.summary.add_error(f"JSON规则集处理阶段异常: {str(e)}")
            return False

    def ip_process_phase(self) -> bool:
        """
        执行IP规则集处理阶段

        Returns:
            是否有成功的处理
        """
        try:
            # 如果没有 ip_only 下载结果，跳过此阶段
            if not self.ip_download_results:
                self.logger.info("📋 没有IP规则集需要处理，跳过")
                return True

            self.logger.separator("开始IP规则集处理阶段")

            # 执行IP处理
            self.ip_process_results = self.ip_processor_service.process_all_ip_rulesets(
                self.ip_download_results
            )

            # 统计结果
            successful_ip_processes = sum(
                1 for data in self.ip_process_results.values() if data.success
            )

            if successful_ip_processes == 0:
                self.logger.error("❌ 没有成功处理任何IP规则集")
                self.summary.add_error("没有成功处理任何IP规则集")
                return False

            # 统计IP数量
            total_ips = sum(
                data.ip_count
                for data in self.ip_process_results.values()
                if data.success
            )
            self.summary.total_rules += total_ips

            # 记录失败的处理
            failed_processes = []
            for name, data in self.ip_process_results.items():
                if not data.success:
                    failed_processes.append(name)
                    if data.error:
                        self.summary.add_warning(f"IP规则集 {name}: {data.error}")

            if failed_processes:
                self.summary.add_warning(
                    f"部分IP规则集处理失败: {', '.join(failed_processes)}"
                )

            return True

        except Exception as e:
            self.logger.error(f"❌ IP规则集处理阶段异常: {str(e)}")
            self.summary.add_error(f"IP规则集处理阶段异常: {str(e)}")
            return False

    def compile_phase(self) -> bool:
        """
        执行编译阶段

        Returns:
            是否有成功的编译
        """
        try:
            self.logger.separator("开始编译阶段")

            # 执行编译（包括rulesets处理的、convert转换的和ip_only处理的JSON文件）
            self.compile_results = self.compiler_service.compile_all_rulesets(
                self.process_results, self.convert_results, self.ip_process_results
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
                result.file_size
                for result in self.compile_results.values()
                if result.success
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
                self.summary.add_warning(
                    f"部分规则集编译失败: {', '.join(failed_compiles)}"
                )

            return True

        except Exception as e:
            self.logger.error(f"❌ 编译阶段异常: {str(e)}")
            self.summary.add_error(f"编译阶段异常: {str(e)}")
            return False

    def convert_phase(self) -> bool:
        """
        执行转换阶段 - 使用已下载的 convert 数据

        Returns:
            是否有成功的转换
        """
        try:
            self.logger.separator("开始转换阶段")

            # 如果没有convert下载结果，跳过此阶段
            if not self.convert_download_results:
                self.logger.info("📋 没有convert配置，跳过转换阶段")
                return True  # 没有convert配置不算失败

            # 使用已下载的数据执行转换
            self.convert_results = self.converter_service.convert_downloaded_rulesets(
                self.convert_download_results
            )

            # 统计结果
            successful_converts = sum(
                1 for data in self.convert_results.values() if data.is_successful()
            )

            if successful_converts == 0:
                self.logger.error("❌ 没有成功转换任何规则集")
                self.summary.add_error("没有成功转换任何规则集")
                return False

            # 记录失败的转换
            failed_converts = []
            for name, data in self.convert_results.items():
                if not data.is_successful():
                    failed_converts.append(name)
                    for error in data.errors:
                        self.summary.add_warning(f"转换规则集 {name}: {error}")

            if failed_converts:
                self.summary.add_warning(
                    f"部分转换规则集失败: {', '.join(failed_converts)}"
                )

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
        self.logger.info("📊 执行统计:")
        self.logger.info(f"   总规则集数量: {self.summary.total_rulesets}")
        self.logger.info(f"   成功下载: {self.summary.successful_downloads}")
        self.logger.info(f"   成功处理: {self.summary.successful_processes}")
        self.logger.info(f"   成功编译: {self.summary.successful_compiles}")

        if self.summary.total_rules > 0:
            self.logger.info(f"   总规则数量: {self.summary.total_rules:,}")

        if self.summary.total_output_size > 0:
            formatted_size = self.file_utils.format_file_size(
                self.summary.total_output_size
            )
            self.logger.info(f"   输出文件总大小: {formatted_size}")

        # 显示转换统计
        if self.convert_results:
            convert_stats = self.get_convert_statistics()
            self.logger.info(
                f"   转换规则集: {convert_stats['successful_converts']}/"
                f"{convert_stats['total_converts']}"
            )
            self.logger.info(f"   生成JSON文件: {convert_stats['total_json_files']}")

        # 显示下载统计（包含convert下载）
        if self.convert_download_results:
            convert_download_stats = self.download_service.get_download_statistics(
                self.convert_download_results
            )
            self.logger.info(
                f"   Convert下载: {convert_download_stats['successful_sources']}/"
                f"{convert_download_stats['total_sources']}"
            )

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
            self.logger.success(
                f"🎉 规则集生成完成！成功生成 {self.summary.successful_compiles} 个规则集"
            )
        else:
            self.logger.error("💥 规则集生成失败！没有成功生成任何规则集")

    def _show_generated_files(self) -> None:
        """
        显示生成的文件信息
        """
        self.logger.info("\n📁 生成的文件:")

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
            self.logger.info("\n📁 转换生成的JSON文件:")
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
            self.logger.info("\n📁 编译生成的SRS文件:")
            for task_name, compile_result in self.compile_results.items():
                if compile_result.success and compile_result.output_file:
                    if Path(compile_result.output_file).exists():
                        size = Path(compile_result.output_file).stat().st_size
                        formatted_size = self.file_utils.format_file_size(size)
                        self.logger.info(
                            f"   ✓ {compile_result.output_file} ({formatted_size})"
                        )

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
            self.logger.info("优化流程：下载 → IP处理 → JSON处理 → 转换 → 编译")
            self.logger.separator()

            # 1. 加载配置
            if not self._load_and_validate_config():
                return False

            # 2. 统一下载阶段（同时下载 ip_only、rulesets 和 convert）
            if not self.download_phase():
                return False

            # 3. IP处理阶段（处理 ip_only）
            if not self.ip_process_phase():
                return False

            # 4. JSON处理阶段（处理 rulesets）
            if not self.process_phase():
                return False

            # 5. 转换阶段（转换 convert）
            if not self.convert_phase():
                return False

            # 6. 编译阶段（编译所有规则集）
            if not self.compile_phase():
                return False

            # 7. 清理阶段
            self.cleanup_phase()

            # 8. 显示摘要
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
