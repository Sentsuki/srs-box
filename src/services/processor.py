"""
处理服务
实现JSON规则集合并、IP列表处理、规则过滤功能
优化内存使用，支持大文件处理
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ..utils.config import ConfigManager
from ..utils.file_utils import FileUtils
from ..utils.logger import Logger
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

    def set_success(
        self,
        ruleset_data: Dict[str, Any],
        output_file: str,
        rule_count: int,
        rule_types: List[str],
        filtered_count: int = 0,
    ) -> None:
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

    def __init__(
        self, config_manager: ConfigManager, logger: Logger, file_utils: FileUtils
    ):
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
        self.filter_keywords = ["ruleset.skk.moe"]

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

    def filter_rules(
        self, rules: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        过滤规则列表，移除包含特定关键字的规则，使用内存优化的流式处理

        Args:
            rules: 规则列表

        Returns:
            (过滤后的规则列表, 被过滤的规则数量)
        """
        filtered_rules = []
        filtered_count = 0

        # 分批处理规则，避免内存峰值
        batch_size = 100

        for batch_start in range(0, len(rules), batch_size):
            batch_end = min(batch_start + batch_size, len(rules))
            rule_batch = rules[batch_start:batch_end]

            for rule in rule_batch:
                if not isinstance(rule, dict):
                    continue

                filtered_rule = {}

                for rule_type, rule_values in rule.items():
                    if not isinstance(rule_values, list):
                        continue

                    # 分批过滤规则值，避免大量规则值同时在内存中
                    original_count = len(rule_values)
                    filtered_values = []

                    value_batch_size = 1000
                    for value_start in range(0, len(rule_values), value_batch_size):
                        value_end = min(
                            value_start + value_batch_size, len(rule_values)
                        )
                        value_batch = rule_values[value_start:value_end]

                        # 过滤当前批次的值
                        batch_filtered = [
                            value
                            for value in value_batch
                            if not self.should_filter_rule_value(value)
                        ]
                        filtered_values.extend(batch_filtered)

                        # 清理已处理的批次
                        del value_batch
                        del batch_filtered

                    filtered_count += original_count - len(filtered_values)

                    # 只添加非空的规则
                    if filtered_values:
                        filtered_rule[rule_type] = filtered_values

                # 只添加非空的规则对象
                if filtered_rule:
                    filtered_rules.append(filtered_rule)

            # 显示处理进度（对于大规则集）
            if batch_end % 1000 == 0:
                self.logger.info(f"🔄 过滤进度: {batch_end}/{len(rules)} 规则")

        return filtered_rules, filtered_count

    def cleanup_temporary_data(self) -> None:
        """
        清理临时数据和文件，释放内存
        """
        try:
            # 清理临时文件
            temp_dir = Path("temp")
            if temp_dir.exists():
                # 清理处理过程中的临时文件
                for temp_file in temp_dir.glob("*.tmp"):
                    try:
                        temp_file.unlink()
                    except OSError:
                        pass

                # 清理空的子目录
                for subdir in temp_dir.iterdir():
                    if subdir.is_dir():
                        try:
                            # 如果目录为空，删除它
                            subdir.rmdir()
                        except OSError:
                            pass  # 目录不为空或无法删除

            # 强制垃圾回收
            import gc

            gc.collect()

            self.logger.info("🧹 临时数据清理完成")

        except Exception as e:
            self.logger.warning(f"⚠️ 清理临时数据时出错: {str(e)}")

    def get_memory_usage_info(self) -> Dict[str, Any]:
        """
        获取内存使用信息

        Returns:
            内存使用信息字典
        """
        try:
            import os

            import psutil

            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()

            return {
                "rss_mb": memory_info.rss / (1024 * 1024),  # 物理内存使用
                "vms_mb": memory_info.vms / (1024 * 1024),  # 虚拟内存使用
                "percent": process.memory_percent(),  # 内存使用百分比
                "available_mb": psutil.virtual_memory().available / (1024 * 1024),
            }
        except ImportError:
            # 如果没有psutil，返回基本信息
            return {
                "rss_mb": 0,
                "vms_mb": 0,
                "percent": 0,
                "available_mb": 0,
                "note": "psutil not available",
            }

    def merge_json_rulesets(
        self, json_data_list: List[Dict[str, Any]], config_version: int
    ) -> Dict[str, Any]:
        """
        智能合并多个JSON规则集，使用内存优化的流式处理

        Args:
            json_data_list: JSON数据列表
            config_version: 配置版本号

        Returns:
            合并后的规则集
        """
        # 用于存储合并后的规则，按规则类型分组
        rule_groups: Dict[str, Set[str]] = {}

        # 流式处理每个JSON数据，避免同时在内存中保存所有数据
        # 创建索引列表，避免在迭代时修改原列表
        for i, json_data in enumerate(json_data_list, 1):
            if json_data is None:  # 跳过已清理的数据
                continue

            self.logger.info(f"🔄 合并JSON规则集 {i}/{len(json_data_list)}")

            try:
                rules = []

                # 提取规则列表
                if "rules" in json_data and isinstance(json_data["rules"], list):
                    rules = json_data["rules"]
                else:
                    # 如果JSON结构不标准，尝试直接作为规则处理
                    rules = [json_data]

                # 流式处理每个规则，避免一次性加载所有规则到内存
                for rule_index, rule in enumerate(rules):
                    if not isinstance(rule, dict):
                        continue

                    # 遍历规则中的每个字段
                    for rule_type, rule_values in rule.items():
                        if not isinstance(rule_values, list):
                            continue

                        # 如果这个规则类型还没有，创建新的集合
                        if rule_type not in rule_groups:
                            rule_groups[rule_type] = set()

                        # 分批处理规则值，避免内存峰值
                        batch_size = 1000
                        for batch_start in range(0, len(rule_values), batch_size):
                            batch_end = min(batch_start + batch_size, len(rule_values))
                            batch_values = rule_values[batch_start:batch_end]

                            # 合并规则值，自动去重
                            for value in batch_values:
                                if isinstance(value, str):
                                    rule_groups[rule_type].add(value)

                    # 显示处理进度（对于大规则集）
                    if rule_index > 0 and rule_index % 100 == 0:
                        self.logger.info(f"   处理规则: {rule_index + 1}/{len(rules)}")

            except Exception as e:
                self.logger.warning(f"⚠️ 处理JSON规则集 {i} 时出错: {str(e)}")
                continue

        # 在处理完所有数据后清理列表
        json_data_list.clear()

        # 将分组的规则转换为最终格式，使用内存优化的方式
        merged_rules = []

        # 创建规则类型列表的副本，避免在迭代时修改字典
        rule_types_to_process = list(rule_groups.keys())

        for rule_type in rule_types_to_process:
            rule_values = rule_groups[rule_type]
            if rule_values:  # 只添加非空的规则
                self.logger.info(
                    f"🔄 排序规则类型 {rule_type}: {len(rule_values)} 条规则"
                )

                # 对于大量规则，使用分块排序
                if len(rule_values) > 10000:
                    # 分块处理大量数据
                    sorted_values = []
                    chunk_size = 5000
                    value_list = list(rule_values)

                    for chunk_start in range(0, len(value_list), chunk_size):
                        chunk_end = min(chunk_start + chunk_size, len(value_list))
                        chunk = value_list[chunk_start:chunk_end]
                        sorted_chunk = sorted(chunk)
                        sorted_values.extend(sorted_chunk)

                        # 清理已处理的块
                        del chunk
                        del sorted_chunk

                    # 清理临时列表
                    del value_list
                else:
                    # 小量数据直接排序
                    sorted_values = sorted(list(rule_values))

                merged_rules.append({rule_type: sorted_values})

                # 清理已处理的规则组，释放内存
                del rule_groups[rule_type]

        # 创建合并后的规则集
        merged_ruleset = {"version": config_version, "rules": merged_rules}

        self.logger.info(f"✅ JSON规则集合并完成，共 {len(merged_rules)} 种规则类型")

        return merged_ruleset

    def process_ruleset(
        self, ruleset_name: str, downloaded_data: DownloadedData
    ) -> ProcessedData:
        """
        处理单个规则集的下载数据，使用内存优化的处理方式

        Args:
            ruleset_name: 规则集名称
            downloaded_data: 下载的数据

        Returns:
            处理结果
        """
        self.logger.info(f"🔄 开始处理规则集: {ruleset_name}")

        # 显示处理前的内存使用情况
        memory_info = self.get_memory_usage_info()
        if memory_info["rss_mb"] > 0:
            self.logger.info(f"💾 处理前内存使用: {memory_info['rss_mb']:.1f} MB")

        processed_data = ProcessedData(ruleset_name)

        try:
            config_version = self.config_manager.get_version()

            # 优先处理JSON数据
            if downloaded_data.has_json_data():
                self.logger.info(
                    f"📄 处理JSON规则集数据: {len(downloaded_data.json_data)} 个"
                )

                if len(downloaded_data.json_data) == 1:
                    # 只有一个JSON文件，使用并覆盖版本号
                    ruleset_data = downloaded_data.json_data[0]
                    # 确保使用配置文件中指定的版本号
                    ruleset_data["version"] = config_version
                    self.logger.info("📋 使用单个JSON规则集并覆盖版本号")
                else:
                    # 多个JSON文件，需要合并
                    self.logger.info(
                        f"🔀 合并 {len(downloaded_data.json_data)} 个JSON规则集"
                    )
                    ruleset_data = self.merge_json_rulesets(
                        downloaded_data.json_data, config_version
                    )

                # 清理原始JSON数据，释放内存
                downloaded_data.json_data.clear()

                # 过滤规则
                if "rules" in ruleset_data and isinstance(ruleset_data["rules"], list):
                    self.logger.info("🔄 开始过滤规则...")
                    original_rules = ruleset_data["rules"]
                    filtered_rules, filtered_count = self.filter_rules(original_rules)

                    # 替换规则并清理原始数据
                    ruleset_data["rules"] = filtered_rules
                    del original_rules  # 显式删除原始规则，释放内存

                    if filtered_count > 0:
                        self.logger.info(
                            f"🚫 已过滤 {filtered_count} 条包含过滤关键字的规则"
                        )
                else:
                    filtered_count = 0

                # 统计规则信息
                rule_count = 0
                rule_types = []

                for rule in ruleset_data.get("rules", []):
                    for rule_type, rule_values in rule.items():
                        if isinstance(rule_values, list):
                            rule_types.append(f"{rule_type}({len(rule_values)})")
                            rule_count += len(rule_values)

                self.logger.info("✅ JSON规则集处理完成")
                self.logger.info(
                    f"📊 规则统计: {', '.join(rule_types)}，总计 {rule_count} 条规则"
                )

            elif downloaded_data.has_text_files():
                # 纯文本IP列表应由 IpProcessorService 处理，这里跳过
                self.logger.warning(
                    "⚠️ 发现文本文件但 ProcessorService 不处理纯IP列表，请使用 IpProcessorService"
                )
                processed_data.set_error("文本IP列表应由 IpProcessorService 处理")
                return processed_data

            else:
                # 没有可处理的数据
                processed_data.set_error("没有可处理的下载数据")
                return processed_data

            # 显示处理后的内存使用情况
            memory_info_after = self.get_memory_usage_info()
            if memory_info_after["rss_mb"] > 0:
                self.logger.info(
                    f"💾 处理后内存使用: {memory_info_after['rss_mb']:.1f} MB"
                )

            # 获取输出目录配置
            output_config = self.config_manager.get_output_config()
            json_dir = output_config["json_dir"]

            # 确保输出目录存在
            json_path = Path(json_dir)
            json_path.mkdir(parents=True, exist_ok=True)

            # 保存处理后的规则集
            output_file = json_path / f"{ruleset_name}.json"
            self.file_utils.write_json_file(str(output_file), ruleset_data)

            processed_data.set_success(
                ruleset_data, str(output_file), rule_count, rule_types, filtered_count
            )

            self.logger.info(f"✅ 规则集已保存到: {output_file}")

            # 清理临时数据
            self.cleanup_temporary_data()

        except Exception as e:
            error_msg = f"处理规则集时发生异常: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            processed_data.set_error(error_msg)

            # 即使出错也要清理临时数据
            self.cleanup_temporary_data()

        return processed_data

    def process_all_rulesets(
        self, download_results: Dict[str, DownloadedData]
    ) -> Dict[str, ProcessedData]:
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
            name: data
            for name, data in download_results.items()
            if data.is_successful()
        }

        if not successful_downloads:
            self.logger.warning("⚠️ 没有成功下载的规则集需要处理")
            return results

        self.logger.info(f"📋 需要处理 {len(successful_downloads)} 个规则集")

        for i, (ruleset_name, downloaded_data) in enumerate(
            successful_downloads.items(), 1
        ):
            self.logger.step(
                f"处理规则集: {ruleset_name}", i, len(successful_downloads)
            )

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
        self.logger.separator("ruleset组 处理阶段完成")
        self.logger.success(
            f"✅ ruleset组 处理完成: {successful_processed}/{len(successful_downloads)} 个成功"
        )

        return results

    def get_processing_statistics(
        self, results: Dict[str, ProcessedData]
    ) -> Dict[str, Any]:
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
        total_filtered = sum(
            data.filtered_count for data in results.values() if data.success
        )

        # 统计规则类型
        rule_type_counts = {}
        for data in results.values():
            if data.success:
                for rule_type in data.rule_types:
                    # 提取规则类型名称（去掉数量）
                    type_name = rule_type.split("(")[0]
                    if type_name not in rule_type_counts:
                        rule_type_counts[type_name] = 0
                    rule_type_counts[type_name] += 1

        return {
            "total_rulesets": total_rulesets,
            "successful_rulesets": successful_rulesets,
            "total_rules": total_rules,
            "total_filtered": total_filtered,
            "rule_type_counts": rule_type_counts,
            "success_rate": (
                (successful_rulesets / total_rulesets * 100)
                if total_rulesets > 0
                else 0
            ),
        }
