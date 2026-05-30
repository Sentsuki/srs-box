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
    """执行摘要数据类"""

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
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        self.warnings.append(warning)


class SummaryReporter:
    """执行摘要展示类，负责将执行结果格式化输出"""

    def __init__(
        self,
        logger: Logger,
        file_utils: FileUtils,
        config_manager: ConfigManager,
    ):
        self.logger = logger
        self.file_utils = file_utils
        self.config_manager = config_manager

    def report(
        self,
        summary: ExecutionSummary,
        compile_results: Dict[str, CompileResult],
        convert_results: Dict[str, ConvertedData],
        convert_download_results: Dict[str, DownloadedData],
        download_service: DownloadService,
        converter_service: ConverterService,
    ) -> None:
        """输出完整的执行摘要"""
        self.logger.separator("执行摘要")

        self.logger.info("📊 执行统计:")
        self.logger.info(f"   总规则集数量: {summary.total_rulesets}")
        self.logger.info(f"   成功下载: {summary.successful_downloads}")
        self.logger.info(f"   成功处理: {summary.successful_processes}")
        self.logger.info(f"   成功编译: {summary.successful_compiles}")

        if summary.total_rules > 0:
            self.logger.info(f"   总规则数量: {summary.total_rules:,}")

        if summary.total_output_size > 0:
            formatted_size = self.file_utils.format_file_size(summary.total_output_size)
            self.logger.info(f"   输出文件总大小: {formatted_size}")

        if convert_results:
            stats = converter_service.get_convert_statistics(convert_results)
            self.logger.info(
                f"   转换规则集: {stats['successful_converts']}/{stats['total_converts']}"
            )
            self.logger.info(f"   生成JSON文件: {stats['total_json_files']}")

        if convert_download_results:
            dl_stats = download_service.get_download_statistics(
                convert_download_results
            )
            self.logger.info(
                f"   Convert下载: {dl_stats['successful_sources']}"
                f"/{dl_stats['total_sources']}"
            )

        self._show_generated_files(compile_results, convert_results)

        if summary.warnings:
            self.logger.info(f"\n⚠️ 警告信息 ({len(summary.warnings)} 个):")
            for warning in summary.warnings:
                self.logger.warning(f"   {warning}")

        if summary.errors:
            self.logger.info(f"\n❌ 错误信息 ({len(summary.errors)} 个):")
            for error in summary.errors:
                self.logger.error(f"   {error}")

        self.logger.info("")
        if summary.successful_compiles > 0:
            self.logger.success(
                f"🎉 规则集生成完成！成功生成 {summary.successful_compiles} 个规则集"
            )
        else:
            self.logger.error("💥 规则集生成失败！没有成功生成任何规则集")

    def _show_generated_files(
        self,
        compile_results: Dict[str, CompileResult],
        convert_results: Dict[str, ConvertedData],
    ) -> None:
        """显示生成的文件信息"""
        self.logger.info("\n📁 生成的文件:")

        output_config = self.config_manager.get_output_config()
        json_dir = output_config["json_dir"]
        srs_dir = output_config["srs_dir"]

        for ruleset_name in self.config_manager.get_rulesets().keys():
            json_file = Path(json_dir) / f"{ruleset_name}.json"
            srs_file = Path(srs_dir) / f"{ruleset_name}.srs"

            for path in (json_file, srs_file):
                if path.exists():
                    size = self.file_utils.format_file_size(path.stat().st_size)
                    self.logger.info(f"   ✓ {path} ({size})")
                else:
                    self.logger.info(f"   ✗ {path} (未找到)")

        if convert_results:
            self.logger.info("\n📁 转换生成的JSON文件:")
            for convert_name, convert_data in convert_results.items():
                if convert_data.is_successful():
                    self.logger.info(f"   📂 {convert_name}:")
                    for json_file in convert_data.json_files:
                        p = Path(json_file)
                        if p.exists():
                            size = self.file_utils.format_file_size(p.stat().st_size)
                            self.logger.info(f"     ✓ {json_file} ({size})")

        if compile_results:
            self.logger.info("\n📁 编译生成的SRS文件:")
            for task_name, compile_result in compile_results.items():
                if compile_result.success and compile_result.output_file:
                    p = Path(compile_result.output_file)
                    if p.exists():
                        size = self.file_utils.format_file_size(p.stat().st_size)
                        self.logger.info(f"   ✓ {compile_result.output_file} ({size})")


class RulesetGenerator:
    """规则集生成器主应用类"""

    def __init__(self, config_path: str = "config.json"):
        """
        初始化规则集生成器

        Args:
            config_path: 配置文件路径，默认为 config.json
        """
        self.config_path = config_path

        self.config_manager = ConfigManager(config_path)

        logging_config = self.config_manager.get_logging_config()
        from .utils.logger import LogLevel

        log_level = LogLevel.from_string(logging_config.get("level", "INFO"))
        self.logger = Logger(
            enable_color=logging_config.get("enable_color", True),
            log_level=log_level,
            show_progress=logging_config.get("show_progress", True),
        )

        self.file_utils = FileUtils()
        self.network_utils = NetworkUtils()

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
        self.reporter = SummaryReporter(
            self.logger, self.file_utils, self.config_manager
        )

        # 执行结果存储
        self.download_results: Dict[str, DownloadedData] = {}
        self.convert_download_results: Dict[str, DownloadedData] = {}
        self.ip_download_results: Dict[str, DownloadedData] = {}
        self.process_results: Dict[str, ProcessedData] = {}
        self.ip_process_results: Dict[str, IpProcessedData] = {}
        self.compile_results: Dict[str, CompileResult] = {}
        self.convert_results: Dict[str, ConvertedData] = {}

        self.summary = ExecutionSummary()

    # ------------------------------------------------------------------
    # 配置加载
    # ------------------------------------------------------------------

    def _load_and_validate_config(self) -> bool:
        """加载并验证配置文件"""
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

            if ip_only:
                self.logger.info("   🌐 IP规则集:")
                for name, urls in ip_only.items():
                    self.logger.info(f"      - {name}: {len(urls)} 个数据源")

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

    # ------------------------------------------------------------------
    # 流水线各阶段
    # ------------------------------------------------------------------

    def download_phase(self) -> bool:
        """统一下载阶段 - 同时下载 ip_only、rulesets 和 convert 链接"""
        try:
            self.logger.separator("开始统一下载阶段")
            config = self.config_manager.load_config()

            ip_only_config = config.get("ip_only", {})
            rulesets_config = config.get("rulesets", {})
            convert_config = config.get("convert", {})

            if ip_only_config:
                self.logger.info("🌐 下载 ip_only 配置")
                self.ip_download_results = self.download_service.download_ip_sources(
                    ip_only_config
                )
            else:
                self.logger.info("📋 没有 ip_only 配置，跳过")

            if rulesets_config:
                self.logger.info("📄 下载 rulesets 配置")
                self.download_results = self.download_service.download_rulesets_sources(
                    rulesets_config
                )
            else:
                self.logger.info("📋 没有 rulesets 配置，跳过")

            if convert_config:
                self.logger.info("🔄 下载 convert 配置")
                self.convert_download_results = (
                    self.download_service.download_convert_sources(convert_config)
                )
            else:
                self.logger.info("📋 没有 convert 配置，跳过")

            successful_ip = sum(
                1 for d in self.ip_download_results.values() if d.is_successful()
            )
            successful_rulesets = sum(
                1 for d in self.download_results.values() if d.is_successful()
            )
            successful_convert = sum(
                1 for d in self.convert_download_results.values() if d.is_successful()
            )
            total_successful = successful_ip + successful_rulesets + successful_convert
            self.summary.successful_downloads = total_successful

            if total_successful == 0:
                self.logger.error("❌ 没有成功下载任何数据源")
                self.summary.add_error("没有成功下载任何数据源")
                return False

            # 记录失败警告
            for name, data in self.ip_download_results.items():
                if not data.is_successful():
                    for error in data.errors:
                        self.summary.add_warning(f"IP规则集 {name}: {error}")

            for name, data in self.download_results.items():
                if not data.is_successful():
                    for error in data.errors:
                        self.summary.add_warning(f"规则集 {name}: {error}")

            for name, data in self.convert_download_results.items():
                if not data.is_successful():
                    for error in data.errors:
                        self.summary.add_warning(f"转换配置 {name}: {error}")

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
                self.logger.info(
                    f"   Convert: {successful_convert}"
                    f"/{len(self.convert_download_results)} 成功"
                )

            return True

        except Exception as e:
            self.logger.error(f"❌ 统一下载阶段异常: {str(e)}")
            self.summary.add_error(f"统一下载阶段异常: {str(e)}")
            return False

    def process_phase(self) -> bool:
        """执行JSON规则集处理阶段"""
        try:
            if not self.download_results:
                self.logger.info("📋 没有JSON规则集需要处理，跳过")
                return True

            self.logger.separator("开始JSON规则集处理阶段")
            self.process_results = self.processor_service.process_all_rulesets(
                self.download_results
            )

            successful_processes = sum(
                1 for d in self.process_results.values() if d.success
            )
            self.summary.successful_processes = successful_processes

            if successful_processes == 0:
                self.logger.error("❌ 没有成功处理任何JSON规则集")
                self.summary.add_error("没有成功处理任何JSON规则集")
                return False

            self.summary.total_rules = sum(
                d.rule_count for d in self.process_results.values() if d.success
            )

            for name, data in self.process_results.items():
                if not data.success and data.error:
                    self.summary.add_warning(f"规则集 {name}: {data.error}")

            return True

        except Exception as e:
            self.logger.error(f"❌ JSON规则集处理阶段异常: {str(e)}")
            self.summary.add_error(f"JSON规则集处理阶段异常: {str(e)}")
            return False

    def ip_process_phase(self) -> bool:
        """执行IP规则集处理阶段"""
        try:
            if not self.ip_download_results:
                self.logger.info("📋 没有IP规则集需要处理，跳过")
                return True

            self.logger.separator("开始IP规则集处理阶段")
            self.ip_process_results = self.ip_processor_service.process_all_ip_rulesets(
                self.ip_download_results
            )

            successful_ip_processes = sum(
                1 for d in self.ip_process_results.values() if d.success
            )

            if successful_ip_processes == 0:
                self.logger.error("❌ 没有成功处理任何IP规则集")
                self.summary.add_error("没有成功处理任何IP规则集")
                return False

            self.summary.total_rules += sum(
                d.ip_count for d in self.ip_process_results.values() if d.success
            )

            for name, data in self.ip_process_results.items():
                if not data.success and data.error:
                    self.summary.add_warning(f"IP规则集 {name}: {data.error}")

            return True

        except Exception as e:
            self.logger.error(f"❌ IP规则集处理阶段异常: {str(e)}")
            self.summary.add_error(f"IP规则集处理阶段异常: {str(e)}")
            return False

    def convert_phase(self) -> bool:
        """执行转换阶段"""
        try:
            self.logger.separator("开始转换阶段")

            if not self.convert_download_results:
                self.logger.info("📋 没有convert配置，跳过转换阶段")
                return True

            self.convert_results = self.converter_service.convert_downloaded_rulesets(
                self.convert_download_results
            )

            successful_converts = sum(
                1 for d in self.convert_results.values() if d.is_successful()
            )

            if successful_converts == 0:
                self.logger.error("❌ 没有成功转换任何规则集")
                self.summary.add_error("没有成功转换任何规则集")
                return False

            for name, data in self.convert_results.items():
                if not data.is_successful():
                    for error in data.errors:
                        self.summary.add_warning(f"转换规则集 {name}: {error}")

            return True

        except Exception as e:
            self.logger.error(f"❌ 转换阶段异常: {str(e)}")
            self.summary.add_error(f"转换阶段异常: {str(e)}")
            return False

    def compile_phase(self) -> bool:
        """执行编译阶段"""
        try:
            self.logger.separator("开始编译阶段")
            self.compile_results = self.compiler_service.compile_all_rulesets(
                self.process_results, self.convert_results, self.ip_process_results
            )

            successful_compiles = sum(
                1 for r in self.compile_results.values() if r.success
            )
            self.summary.successful_compiles = successful_compiles

            if successful_compiles == 0:
                self.logger.error("❌ 没有成功编译任何规则集")
                self.summary.add_error("没有成功编译任何规则集")
                return False

            self.summary.total_output_size = sum(
                r.file_size for r in self.compile_results.values() if r.success
            )

            for name, result in self.compile_results.items():
                if not result.success and result.error:
                    self.summary.add_warning(f"规则集 {name}: {result.error}")

            return True

        except Exception as e:
            self.logger.error(f"❌ 编译阶段异常: {str(e)}")
            self.summary.add_error(f"编译阶段异常: {str(e)}")
            return False

    def cleanup_phase(self) -> None:
        """执行清理阶段"""
        try:
            self.logger.separator("开始清理阶段")
            self.download_service.cleanup_temp_files()
            self.compiler_service.cleanup_sing_box()
            self.logger.success("✅ 清理完成")
        except Exception as e:
            self.logger.warning(f"⚠️ 清理阶段出现问题: {str(e)}")
            self.summary.add_warning(f"清理阶段出现问题: {str(e)}")

    def show_summary(self) -> None:
        """显示执行摘要"""
        self.reporter.report(
            self.summary,
            self.compile_results,
            self.convert_results,
            self.convert_download_results,
            self.download_service,
            self.converter_service,
        )

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------

    def run(self) -> bool:
        """运行完整的规则集生成流程"""
        try:
            self.logger.separator()
            self.logger.info("🌏 srs-box 规则集生成器启动")
            self.logger.info("优化流程：下载 → IP处理 → JSON处理 → 转换 → 编译")
            self.logger.separator()

            if not self._load_and_validate_config():
                return False
            if not self.download_phase():
                return False
            if not self.ip_process_phase():
                return False
            if not self.process_phase():
                return False
            if not self.convert_phase():
                return False
            if not self.compile_phase():
                return False

            self.cleanup_phase()
            self.show_summary()

            return self.summary.successful_compiles > 0

        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ 用户中断执行")
            self.summary.add_warning("用户中断执行")
            return False

        except Exception as e:
            self.logger.error(f"❌ 程序执行异常: {str(e)}")
            self.summary.add_error(f"程序执行异常: {str(e)}")
            return False

    # ------------------------------------------------------------------
    # 统计信息访问器
    # ------------------------------------------------------------------

    def get_execution_summary(self) -> ExecutionSummary:
        return self.summary

    def get_download_statistics(self) -> Dict[str, Any]:
        return self.download_service.get_download_statistics(self.download_results)

    def get_processing_statistics(self) -> Dict[str, Any]:
        return self.processor_service.get_processing_statistics(self.process_results)

    def get_compile_statistics(self) -> Dict[str, Any]:
        return self.compiler_service.get_compile_statistics(self.compile_results)

    def get_convert_statistics(self) -> Dict[str, Any]:
        return self.converter_service.get_convert_statistics(self.convert_results)
