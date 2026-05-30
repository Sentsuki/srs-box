"""
IP规则集处理服务
专门处理纯IP/CIDR列表文件，生成sing-box格式的JSON规则集
"""

from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set

from ..utils.config import ConfigManager
from ..utils.file_utils import FileUtils
from ..utils.logger import Logger
from .downloader import DownloadedData


class IpProcessedData:
    """IP规则集处理结果类"""

    def __init__(self, ruleset_name: str):
        self.ruleset_name = ruleset_name
        self.ruleset_data: Optional[Dict[str, Any]] = None
        self.output_file: Optional[str] = None
        self.ip_count = 0
        self.success = False
        self.error: Optional[str] = None

    def set_success(
        self,
        ruleset_data: Dict[str, Any],
        output_file: str,
        ip_count: int,
    ) -> None:
        """设置成功结果"""
        self.ruleset_data = ruleset_data
        self.output_file = output_file
        self.ip_count = ip_count
        self.success = True

    def set_error(self, error: str) -> None:
        """设置错误结果"""
        self.error = error
        self.success = False


class IpProcessorService:
    """IP规则集处理服务类"""

    def __init__(
        self, config_manager: ConfigManager, logger: Logger, file_utils: FileUtils
    ):
        """
        初始化IP处理服务

        Args:
            config_manager: 配置管理器
            logger: 日志记录器
            file_utils: 文件工具
        """
        self.config_manager = config_manager
        self.logger = logger
        self.file_utils = file_utils

    def create_ip_ruleset_from_text_files(
        self, text_files: List[str], config_version: int
    ) -> Dict[str, Any]:
        """
        从文本文件创建IP规则集，使用流式处理优化内存使用

        Args:
            text_files: 文本文件路径列表
            config_version: 配置版本号

        Returns:
            IP规则集数据
        """

        # 使用生成器流式处理大文件，优化内存使用
        def read_ip_lines_streaming() -> Generator[str, None, None]:
            """流式读取IP行，逐行处理避免加载整个文件到内存"""
            for file_path in text_files:
                try:
                    # 使用流式读取，一次只读取一行
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        for line_num, line in enumerate(f, 1):
                            cleaned_line = line.strip()
                            if cleaned_line and not cleaned_line.startswith("#"):
                                yield cleaned_line

                            # 每处理1000行显示一次进度（对于大文件）
                            if line_num % 1000 == 0:
                                self.logger.info(
                                    f"📖 处理文件 {Path(file_path).name}: {line_num} 行"
                                )

                except Exception as e:
                    self.logger.warning(f"⚠️ 读取文件失败: {file_path} - {str(e)}")

        # 使用集合进行内存高效的去重，分批处理避免内存峰值
        ip_set: Set[str] = set()
        batch_size = 10000  # 每批处理10000个IP
        batch_count = 0

        current_batch: List[str] = []
        for ip in read_ip_lines_streaming():
            current_batch.append(ip)

            # 当批次达到指定大小时，处理这一批
            if len(current_batch) >= batch_size:
                ip_set.update(current_batch)
                current_batch.clear()  # 清空当前批次，释放内存
                batch_count += 1

                # 显示处理进度
                self.logger.info(f"🔄 已处理 {batch_count * batch_size} 个IP地址")

        # 处理最后一批
        if current_batch:
            ip_set.update(current_batch)
            current_batch.clear()

        self.logger.info(f"📊 去重后共有 {len(ip_set)} 个唯一IP地址")
        self.logger.info("🔄 开始排序IP地址...")

        ip_list: List[str] = sorted(ip_set)
        del ip_set  # 释放集合内存

        # 创建规则集
        ruleset = {"version": config_version, "rules": [{"ip_cidr": ip_list}]}

        self.logger.info(f"✅ IP规则集创建完成，共 {len(ip_list)} 条规则")

        return ruleset

    def process_ip_ruleset(
        self, ruleset_name: str, downloaded_data: DownloadedData
    ) -> IpProcessedData:
        """
        处理单个IP规则集

        Args:
            ruleset_name: 规则集名称
            downloaded_data: 下载的数据

        Returns:
            处理结果
        """
        self.logger.info(f"🔄 开始处理IP规则集: {ruleset_name}")

        processed_data = IpProcessedData(ruleset_name)

        try:
            config_version = self.config_manager.get_version()

            if not downloaded_data.has_text_files():
                processed_data.set_error("没有可处理的文本文件")
                return processed_data

            self.logger.info(f"📄 处理文本文件: {len(downloaded_data.text_files)} 个")

            ruleset_data = self.create_ip_ruleset_from_text_files(
                downloaded_data.text_files, config_version
            )

            # 统计IP数量
            ip_count = 0
            for rule in ruleset_data.get("rules", []):
                if "ip_cidr" in rule:
                    ip_count = len(rule["ip_cidr"])

            # 获取输出目录配置
            output_config = self.config_manager.get_output_config()
            json_dir = output_config["json_dir"]

            # 确保输出目录存在
            json_path = Path(json_dir)
            json_path.mkdir(parents=True, exist_ok=True)

            # 保存处理后的规则集
            output_file = json_path / f"{ruleset_name}.json"
            self.file_utils.write_json_file(str(output_file), ruleset_data)

            processed_data.set_success(ruleset_data, str(output_file), ip_count)

            self.logger.info(f"✅ IP规则集已保存到: {output_file}")
            self.logger.info(f"📊 IP数量: {ip_count} 条")

        except Exception as e:
            error_msg = f"处理IP规则集时发生异常: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            processed_data.set_error(error_msg)

        return processed_data

    def process_all_ip_rulesets(
        self, download_results: Dict[str, DownloadedData]
    ) -> Dict[str, IpProcessedData]:
        """
        处理所有IP规则集

        Args:
            download_results: 下载结果字典

        Returns:
            处理结果字典
        """
        results: Dict[str, IpProcessedData] = {}

        if not download_results:
            self.logger.info("📋 没有IP规则集需要处理")
            return results

        self.logger.header("开始IP规则集处理阶段")

        # 只处理成功下载的规则集
        successful_downloads = {
            name: data
            for name, data in download_results.items()
            if data.is_successful()
        }

        if not successful_downloads:
            self.logger.warning("⚠️ 没有成功下载的IP规则集需要处理")
            return results

        self.logger.info(f"📋 需要处理 {len(successful_downloads)} 个IP规则集")

        for i, (ruleset_name, downloaded_data) in enumerate(
            successful_downloads.items(), 1
        ):
            self.logger.step(
                f"处理IP规则集: {ruleset_name}", i, len(successful_downloads)
            )

            try:
                processed_data = self.process_ip_ruleset(ruleset_name, downloaded_data)
                results[ruleset_name] = processed_data

            except Exception as e:
                self.logger.error(f"❌ IP规则集 {ruleset_name} 处理异常: {str(e)}")
                failed_data = IpProcessedData(ruleset_name)
                failed_data.set_error(f"处理异常: {str(e)}")
                results[ruleset_name] = failed_data

            # 添加分隔线（除了最后一个）
            if i < len(successful_downloads):
                self.logger.info("─" * 50)

        # 输出总体统计
        successful_processed = sum(1 for data in results.values() if data.success)
        total_ips = sum(data.ip_count for data in results.values() if data.success)

        self.logger.separator("IP组 处理阶段完成")
        self.logger.success(
            f"✅ IP组 处理完成: {successful_processed}/{len(successful_downloads)} 个成功"
        )
        self.logger.info(f"📊 总IP数量: {total_ips} 条")

        return results

    def get_ip_processing_statistics(
        self, results: Dict[str, IpProcessedData]
    ) -> Dict[str, Any]:
        """
        获取IP处理统计信息

        Args:
            results: 处理结果字典

        Returns:
            统计信息字典
        """
        total_rulesets = len(results)
        successful_rulesets = sum(1 for data in results.values() if data.success)
        total_ips = sum(data.ip_count for data in results.values() if data.success)

        return {
            "total_rulesets": total_rulesets,
            "successful_rulesets": successful_rulesets,
            "total_ips": total_ips,
            "success_rate": (
                (successful_rulesets / total_rulesets * 100)
                if total_rulesets > 0
                else 0
            ),
        }
