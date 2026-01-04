"""
转换服务
处理convert配置中的链接，使用原有的转换逻辑生成JSON规则集
融入现有的下载-处理-编译架构
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..utils.config import ConfigManager
from ..utils.file_utils import FileUtils
from ..utils.logger import Logger
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

    def __init__(
        self,
        config_manager: ConfigManager,
        logger: Logger,
        network_utils: NetworkUtils,
        file_utils: FileUtils,
    ):
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
            "DOMAIN-SUFFIX": "domain_suffix",
            "HOST-SUFFIX": "domain_suffix",
            "host-suffix": "domain_suffix",
            "DOMAIN": "domain",
            "HOST": "domain",
            "host": "domain",
            "DOMAIN-KEYWORD": "domain_keyword",
            "HOST-KEYWORD": "domain_keyword",
            "host-keyword": "domain_keyword",
            "IP-CIDR": "ip_cidr",
            "ip-cidr": "ip_cidr",
            "IP-CIDR6": "ip_cidr",
            "IP6-CIDR": "ip_cidr",
            "SRC-IP-CIDR": "source_ip_cidr",
            "GEOIP": "geoip",
            "DST-PORT": "port",
            "SRC-PORT": "source_port",
            "URL-REGEX": "domain_regex",
            "DOMAIN-REGEX": "domain_regex",
        }

    def convert_downloaded_rulesets(
        self, download_results: Dict[str, DownloadedData]
    ) -> Dict[str, ConvertedData]:
        """
        转换已下载的convert规则集数据

        Args:
            download_results: 已下载的convert数据

        Returns:
            convert规则集名称到转换数据的映射
        """
        if not download_results:
            self.logger.info("📋 没有已下载的convert数据，跳过转换阶段")
            return {}

        results = {}

        self.logger.header("开始转换阶段")
        self.logger.info(f"📋 处理 {len(download_results)} 个已下载的convert规则集")

        for i, (convert_name, download_data) in enumerate(download_results.items(), 1):
            self.logger.step(f"转换规则集: {convert_name}", i, len(download_results))

            try:
                converted_data = self._convert_downloaded_data(
                    convert_name, download_data
                )
                results[convert_name] = converted_data

            except Exception as e:
                self.logger.error(f"❌ 规则集 {convert_name} 转换异常: {str(e)}")
                # 创建失败的转换数据
                failed_data = ConvertedData(convert_name)
                failed_data.set_total_count(download_data.total_count)
                failed_data.add_error(f"转换异常: {str(e)}")
                results[convert_name] = failed_data

            # 添加分隔线（除了最后一个）
            if i < len(download_results):
                self.logger.info("─" * 50)

        # 输出总体统计
        stats = self.get_convert_statistics(results)
        self.logger.separator("convert组 转换阶段完成")
        self.logger.success(
            f"✅ convert组 转换完成: {stats['successful_converts']}/{stats['total_converts']} 个规则集成功"
        )

        return results

    def _convert_downloaded_data(
        self, convert_name: str, download_data: DownloadedData
    ) -> ConvertedData:
        """
        转换单个已下载的convert数据

        Args:
            convert_name: convert规则集名称
            download_data: 已下载的数据

        Returns:
            转换数据结果
        """
        self.logger.info(f"🔄 转换已下载的规则集: {convert_name}")

        # 创建转换结果对象
        converted_data = ConvertedData(convert_name)
        converted_data.set_total_count(download_data.total_count)

        if not download_data.is_successful():
            converted_data.add_error("源数据下载失败")
            return converted_data

        # 获取输出目录配置
        output_config = self.config_manager.get_output_config()
        json_dir = Path(output_config["json_dir"])
        self.file_utils.ensure_dir(json_dir)

        # 初始化合并结构
        merged_by_type = {}  # pattern -> set of stripped addresses (for dedup)
        all_logic_rules = []  # 收集所有逻辑规则
        domain_entries = set()  # 单独收集domain，用于去重并插入开头

        # 处理文本文件
        for text_file in download_data.text_files:
            try:
                self.logger.info(f"🔄 处理文本文件: {Path(text_file).name}")

                # 读取文件内容并解析
                content = self.file_utils.read_text_file(text_file)

                # 尝试解析为YAML
                try:
                    import yaml

                    yaml_data = yaml.safe_load("\n".join(content))
                    
                    # 检查 YAML 解析结果是否为有效结构（dict 或 list）
                    # 如果 yaml.safe_load 返回字符串，说明文件不是标准 YAML 结构
                    # （例如 Clash .list 格式文件会被解析为单行字符串）
                    if isinstance(yaml_data, (dict, list)):
                        df, logic_rules = self._parse_yaml_data(yaml_data)
                    else:
                        # 不是有效的 YAML 结构，按文本列表处理
                        self.logger.info("📝 检测到非 YAML 结构格式，使用文本列表解析")
                        df, logic_rules = self._parse_text_list(content)
                except Exception:
                    # 如果 YAML 解析失败，按文本列表处理
                    df, logic_rules = self._parse_text_list(content)

                # 收集逻辑规则
                all_logic_rules.extend(logic_rules)

                # 处理DataFrame数据
                if df is not None and not df.empty:
                    self._merge_dataframe_to_rules(df, merged_by_type, domain_entries)

                converted_data.success_count += 1

            except Exception as e:
                self.logger.warning(
                    f"⚠️ 文件处理失败: {Path(text_file).name} - {str(e)}"
                )
                converted_data.add_error(
                    f"文件处理失败: {Path(text_file).name} - {str(e)}"
                )

        # 如果有成功处理的数据，构建合并的规则集
        if merged_by_type or all_logic_rules or domain_entries:
            merged_ruleset = {"version": self.config_manager.get_version(), "rules": []}

            # 添加非domain规则
            for pattern, values in merged_by_type.items():
                if values:
                    sorted_values = sorted(list(values))
                    merged_ruleset["rules"].append({pattern: sorted_values})

            # 添加domain（插入开头，去重）
            if domain_entries:
                sorted_domains = sorted(list(domain_entries))
                merged_ruleset["rules"].insert(0, {"domain": sorted_domains})

            # 添加逻辑规则（追加到末尾）
            merged_ruleset["rules"].extend(all_logic_rules)

            # 生成文件名
            file_name = json_dir / f"{convert_name}.json"

            # 写入JSON文件
            with open(file_name, "w", encoding="utf-8") as output_file:
                result_rules_str = json.dumps(
                    self.sort_dict(merged_ruleset), ensure_ascii=False, indent=2
                )
                result_rules_str = result_rules_str.replace("\\\\", "\\")
                output_file.write(result_rules_str)

            converted_data.add_converted_file(str(file_name), "")
            self.logger.info(f"✅ 转换完成: {file_name}")
        else:
            self.logger.error(f"❌ 规则集 {convert_name} 无有效数据")

        return converted_data

    def _parse_yaml_data(
        self, yaml_data: Any
    ) -> Tuple[Optional[pd.DataFrame], List[Dict]]:
        """
        解析YAML数据

        Args:
            yaml_data: YAML数据

        Returns:
            (DataFrame数据, 逻辑规则列表)
        """
        rows = []
        if not isinstance(yaml_data, str):
            items = (
                yaml_data.get("payload", [])
                if isinstance(yaml_data, dict)
                else yaml_data
            )
        else:
            items = yaml_data.splitlines()

        for item in items:
            if isinstance(item, str):
                # 简单处理，假设每行是一个pattern:address对
                parts = item.split(",", 1)
                if len(parts) == 2:
                    rows.append(
                        {"pattern": parts[0].strip(), "address": parts[1].strip()}
                    )
                else:
                    rows.append({"pattern": "domain", "address": item.strip()})
            elif isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, list):
                        for v in value:
                            rows.append({"pattern": key, "address": v})
                    else:
                        rows.append({"pattern": key, "address": value})

        df = pd.DataFrame(rows) if rows else None
        return df, []  # YAML通常没有逻辑规则

    def _parse_text_list(
        self, content: List[str]
    ) -> Tuple[Optional[pd.DataFrame], List[Dict]]:
        """
        解析文本列表数据

        Args:
            content: 文本内容行列表

        Returns:
            (DataFrame数据, 逻辑规则列表)
        """
        from io import StringIO

        # 过滤掉注释行（以 # 开头）和空行
        filtered_content = [
            line for line in content 
            if line.strip() and not line.strip().startswith("#")
        ]

        csv_data = StringIO("\n".join(filtered_content))
        df = pd.read_csv(
            csv_data,
            header=None,
            names=["pattern", "address", "other", "other2", "other3"],
            on_bad_lines="skip",
        )

        filtered_rows = []
        rules = []

        # 处理逻辑规则
        if "AND" in df["pattern"].values:
            and_rows = df[df["pattern"].str.contains("AND", na=False)]
            for _, row in and_rows.iterrows():
                rule = {"type": "logical", "mode": "and", "rules": []}
                pattern = ",".join(row.values.astype(str))
                components = re.findall(r"\((.*?)\)", pattern)
                for component in components:
                    for keyword in self.MAP_DICT.keys():
                        if keyword in component:
                            match = re.search(f"{keyword},(.*)", component)
                            if match:
                                value = match.group(1)
                                rule["rules"].append({self.MAP_DICT[keyword]: value})
                rules.append(rule)

        for index, row in df.iterrows():
            if "AND" not in row["pattern"]:
                filtered_rows.append(row)

        df_filtered = pd.DataFrame(
            filtered_rows, columns=["pattern", "address", "other", "other2", "other3"]
        )
        return df_filtered, rules

    def _merge_dataframe_to_rules(
        self, df: pd.DataFrame, merged_by_type: Dict, domain_entries: set
    ) -> None:
        """
        将DataFrame数据合并到规则集中

        Args:
            df: 要处理的DataFrame
            merged_by_type: 按类型分组的规则字典
            domain_entries: domain条目集合
        """
        # 过滤掉包含AND的行
        filtered_rows = []
        for index, row in df.iterrows():
            if "AND" not in str(row.get("pattern", "")):
                filtered_rows.append(row)

        if not filtered_rows:
            return

        df_filtered = pd.DataFrame(filtered_rows)

        # 按pattern分组并合并
        for pattern, addresses in (
            df_filtered.groupby("pattern")["address"].apply(list).to_dict().items()
        ):
            # 检查 pattern 是否在 MAP_DICT 中，不在则跳过
            if pattern not in self.MAP_DICT:
                self.logger.info(f"⏭️ 跳过不支持的规则类型: {pattern}")
                continue
            
            stripped = {str(addr).strip() for addr in addresses}  # set for dedup
            mapped_pattern = self.MAP_DICT[pattern]  # 映射到标准类型

            if mapped_pattern == "domain":
                domain_entries.update(stripped)
            else:
                if mapped_pattern not in merged_by_type:
                    merged_by_type[mapped_pattern] = set()
                merged_by_type[mapped_pattern].update(stripped)

    def get_convert_statistics(
        self, results: Dict[str, ConvertedData]
    ) -> Dict[str, Any]:
        """
        获取转换统计信息

        Args:
            results: 转换结果字典

        Returns:
            统计信息字典
        """
        total_converts = len(results)
        successful_converts = sum(
            1 for data in results.values() if data.is_successful()
        )
        total_urls = sum(data.total_count for data in results.values())
        successful_urls = sum(data.success_count for data in results.values())
        total_json_files = sum(len(data.json_files) for data in results.values())

        return {
            "total_converts": total_converts,
            "successful_converts": successful_converts,
            "total_urls": total_urls,
            "successful_urls": successful_urls,
            "total_json_files": total_json_files,
            "success_rate": (
                (successful_urls / total_urls * 100) if total_urls > 0 else 0
            ),
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
            return [
                self.sort_dict(item) if isinstance(item, (dict, list)) else item
                for item in data
            ]
        else:
            return data
