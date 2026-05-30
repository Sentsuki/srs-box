"""
转换服务
处理convert配置中的链接，使用原有的转换逻辑生成JSON规则集
融入现有的下载-处理-编译架构
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
        self.MAP_DICT: Dict[str, str] = {
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
                failed_data = ConvertedData(convert_name)
                failed_data.set_total_count(download_data.total_count)
                failed_data.add_error(f"转换异常: {str(e)}")
                results[convert_name] = failed_data

            if i < len(download_results):
                self.logger.info("─" * 50)

        stats = self.get_convert_statistics(results)
        self.logger.separator("convert组 转换阶段完成")
        success = stats["successful_converts"]
        total = stats["total_converts"]
        self.logger.success(f"✅ convert组 转换完成: {success}/{total} 个成功")

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

        converted_data = ConvertedData(convert_name)
        converted_data.set_total_count(download_data.total_count)

        if not download_data.is_successful():
            converted_data.add_error("源数据下载失败")
            return converted_data

        output_config = self.config_manager.get_output_config()
        json_dir = Path(output_config["json_dir"])
        self.file_utils.ensure_dir(json_dir)

        # merged_by_type: mapped_pattern -> set of values (for dedup)
        merged_by_type: Dict[str, Set[str]] = defaultdict(set)
        all_logic_rules: List[Dict] = []
        domain_entries: Set[str] = set()

        for text_file in download_data.text_files:
            try:
                self.logger.info(f"🔄 处理文本文件: {Path(text_file).name}")
                content = self.file_utils.read_text_file(text_file)

                rows, logic_rules = self._parse_content(content)
                all_logic_rules.extend(logic_rules)
                self._merge_rows_to_rules(rows, merged_by_type, domain_entries)

                converted_data.success_count += 1

            except Exception as e:
                self.logger.warning(
                    f"⚠️ 文件处理失败: {Path(text_file).name} - {str(e)}"
                )
                converted_data.add_error(
                    f"文件处理失败: {Path(text_file).name} - {str(e)}"
                )

        if not merged_by_type and not all_logic_rules and not domain_entries:
            self.logger.error(f"❌ 规则集 {convert_name} 无有效数据")
            return converted_data

        merged_ruleset: Dict[str, Any] = {
            "version": self.config_manager.get_version(),
            "rules": [],
        }

        # domain 插入开头
        if domain_entries:
            merged_ruleset["rules"].append({"domain": sorted(domain_entries)})

        # 其余规则类型
        for pattern, values in merged_by_type.items():
            if values:
                merged_ruleset["rules"].append({pattern: sorted(values)})

        # 逻辑规则追加到末尾
        merged_ruleset["rules"].extend(all_logic_rules)

        file_name = json_dir / f"{convert_name}.json"
        with open(file_name, "w", encoding="utf-8") as output_file:
            result_str = json.dumps(
                self._sort_dict(merged_ruleset), ensure_ascii=False, indent=2
            )
            result_str = result_str.replace("\\\\", "\\")
            output_file.write(result_str)

        converted_data.add_converted_file(str(file_name), "")
        self.logger.info(f"✅ 转换完成: {file_name}")

        return converted_data

    def _parse_content(
        self, content: List[str]
    ) -> Tuple[List[Tuple[str, str]], List[Dict]]:
        """
        解析文件内容，自动识别 YAML payload 格式或纯文本列表格式。

        Args:
            content: 文件行列表

        Returns:
            (rows, logic_rules)
            rows: List of (pattern, address) tuples
            logic_rules: List of logical rule dicts
        """
        # 尝试 YAML 解析
        try:
            import yaml

            yaml_data = yaml.safe_load("\n".join(content))
            if isinstance(yaml_data, (dict, list)):
                return self._parse_yaml_data(yaml_data)
        except Exception:
            pass

        # 回退到纯文本列表解析
        self.logger.info("📝 检测到非 YAML 结构格式，使用文本列表解析")
        return self._parse_text_list(content)

    def _parse_yaml_data(
        self, yaml_data: Any
    ) -> Tuple[List[Tuple[str, str]], List[Dict]]:
        """
        解析 YAML 数据（dict 或 list）。

        Returns:
            (rows, logic_rules)
        """
        rows: List[Tuple[str, str]] = []

        if isinstance(yaml_data, dict):
            items = yaml_data.get("payload", [])
        else:
            items = yaml_data

        for item in items:
            if isinstance(item, str):
                parts = item.split(",", 1)
                if len(parts) == 2:
                    rows.append((parts[0].strip(), parts[1].strip()))
                else:
                    rows.append(("domain", item.strip()))
            elif isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, list):
                        for v in value:
                            rows.append((str(key), str(v)))
                    else:
                        rows.append((str(key), str(value)))

        return rows, []

    def _parse_text_list(
        self, content: List[str]
    ) -> Tuple[List[Tuple[str, str]], List[Dict]]:
        """
        解析 Clash .list 格式的纯文本规则列表。

        Returns:
            (rows, logic_rules)
        """
        rows: List[Tuple[str, str]] = []
        logic_rules: List[Dict] = []

        for line in content:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # 处理 AND 逻辑规则
            if line.startswith("AND,"):
                rule: Dict[str, Any] = {"type": "logical", "mode": "and", "rules": []}
                components = re.findall(r"\((.*?)\)", line)
                for component in components:
                    for keyword, mapped in self.MAP_DICT.items():
                        if component.startswith(keyword + ","):
                            value = component[len(keyword) + 1:]
                            rule["rules"].append({mapped: value})
                            break
                logic_rules.append(rule)
                continue

            parts = line.split(",", 2)
            if len(parts) >= 2:
                rows.append((parts[0].strip(), parts[1].strip()))

        return rows, logic_rules

    def _merge_rows_to_rules(
        self,
        rows: List[Tuple[str, str]],
        merged_by_type: Dict[str, Set[str]],
        domain_entries: Set[str],
    ) -> None:
        """
        将解析出的 (pattern, address) 行合并到规则集中。

        Args:
            rows: 解析出的行列表
            merged_by_type: 按映射类型分组的规则字典（原地修改）
            domain_entries: domain 条目集合（原地修改）
        """
        for pattern, address in rows:
            if pattern not in self.MAP_DICT:
                self.logger.info(f"⏭️ 跳过不支持的规则类型: {pattern}")
                continue

            mapped = self.MAP_DICT[pattern]
            value = address.strip()

            if mapped == "domain":
                domain_entries.add(value)
            else:
                merged_by_type[mapped].add(value)

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

    def _sort_dict(self, data: Any) -> Any:
        """
        递归排序字典（保持原有逻辑）

        Args:
            data: 要排序的字典或数据

        Returns:
            排序后的字典
        """
        if isinstance(data, dict):
            return {k: self._sort_dict(v) for k, v in sorted(data.items())}
        elif isinstance(data, list):
            return [
                self._sort_dict(item) if isinstance(item, (dict, list)) else item
                for item in data
            ]
        return data
