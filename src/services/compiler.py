"""
编译服务
处理sing-box工具下载、规则集编译和清理工作
集成错误处理和重试机制
"""

import os
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any, Dict, Optional

from ..utils.config import ConfigManager
from ..utils.file_utils import FileUtils
from ..utils.logger import Logger
from ..utils.network import NetworkUtils
from .processor import ProcessedData


class CompileResult:
    """编译结果类"""

    def __init__(self, ruleset_name: str):
        self.ruleset_name = ruleset_name
        self.success = False
        self.input_file: Optional[str] = None
        self.output_file: Optional[str] = None
        self.error: Optional[str] = None
        self.file_size = 0

    def set_success(self, input_file: str, output_file: str, file_size: int) -> None:
        """设置成功结果"""
        self.success = True
        self.input_file = input_file
        self.output_file = output_file
        self.file_size = file_size

    def set_error(self, error: str) -> None:
        """设置错误结果"""
        self.success = False
        self.error = error


class CompilerService:
    """编译服务类"""

    def __init__(
        self,
        config_manager: ConfigManager,
        logger: Logger,
        network_utils: NetworkUtils,
        file_utils: FileUtils,
    ):
        """
        初始化编译服务

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

        # sing-box相关配置
        self.temp_dir = Path("temp")
        self.sing_box_binary: Optional[str] = None

    def _get_sing_box_download_url(self) -> str:
        """
        获取sing-box下载URL

        Returns:
            下载URL
        """
        sing_box_config = self.config_manager.get_sing_box_config()
        version = sing_box_config["version"]
        platform = sing_box_config["platform"]

        return (
            f"https://github.com/SagerNet/sing-box/releases/download/"
            f"v{version}/sing-box-{version}-{platform}.tar.gz"
        )

    def _extract_sing_box(self, archive_path: Path) -> str:
        """
        解压sing-box压缩包

        Args:
            archive_path: 压缩包路径

        Returns:
            二进制文件路径

        Raises:
            Exception: 解压失败
        """
        try:
            self.logger.info("📦 正在解压sing-box")

            # 解压文件
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=self.temp_dir)

            # 查找二进制文件
            sing_box_config = self.config_manager.get_sing_box_config()
            version = sing_box_config["version"]
            platform = sing_box_config["platform"]

            extracted_dir = self.temp_dir / f"sing-box-{version}-{platform}"
            binary_name = "sing-box.exe" if "windows" in platform else "sing-box"
            binary_path = extracted_dir / binary_name

            if not binary_path.exists():
                raise FileNotFoundError(f"找不到sing-box二进制文件: {binary_path}")

            # 复制到工作目录
            target_binary = Path(binary_name)
            self.file_utils.copy_file(binary_path, target_binary)

            # 设置执行权限（Unix系统）
            if os.name != "nt":  # 不是Windows
                os.chmod(target_binary, 0o755)

            self.logger.info(f"✅ sing-box已准备就绪: {target_binary}")
            return str(target_binary)

        except Exception as e:
            raise Exception(f"解压sing-box失败: {str(e)}")

    def setup_sing_box(self) -> str:
        """
        下载并设置sing-box工具

        Returns:
            sing-box二进制文件路径

        Raises:
            Exception: 设置失败
        """
        if self.sing_box_binary and Path(self.sing_box_binary).exists():
            self.logger.info(f"🔧 sing-box已存在: {self.sing_box_binary}")
            return self.sing_box_binary

        try:
            # 确保临时目录存在
            self.file_utils.ensure_dir(self.temp_dir)

            # 获取下载URL
            download_url = self._get_sing_box_download_url()
            archive_path = self.temp_dir / "sing-box.tar.gz"

            self.logger.info("📥 开始下载sing-box")
            self.logger.info(f"🔗 下载地址: {download_url}")

            # 下载文件
            def progress_callback(file_id: str, downloaded: int, total: int) -> None:
                if total > 0:
                    size_mb = downloaded / (1024 * 1024)
                    total_mb = total / (1024 * 1024)
                    self.logger.progress(
                        downloaded,
                        total,
                        f"下载sing-box: {size_mb:.1f}MB / {total_mb:.1f}MB",
                    )

            success = self.network_utils.download_file(
                download_url, archive_path, progress_callback=progress_callback
            )

            if not success:
                raise Exception("sing-box下载失败")

            self.logger.success("✅ sing-box下载完成")

            # 解压并设置
            self.sing_box_binary = self._extract_sing_box(archive_path)

            return self.sing_box_binary

        except Exception as e:
            raise Exception(f"设置sing-box失败: {str(e)}")

    def compile_ruleset(self, ruleset_name: str, input_file: str) -> CompileResult:
        """
        编译单个规则集

        Args:
            ruleset_name: 规则集名称
            input_file: 输入JSON文件路径

        Returns:
            编译结果
        """
        result = CompileResult(ruleset_name)

        try:
            # 检查输入文件是否存在
            input_path = Path(input_file)
            if not input_path.exists():
                result.set_error(f"输入文件不存在: {input_file}")
                return result

            # 确保sing-box已设置
            if not self.sing_box_binary:
                raise Exception("sing-box未设置，请先调用setup_sing_box()")

            # 获取输出目录配置
            output_config = self.config_manager.get_output_config()
            srs_dir = output_config["srs_dir"]

            # 确保输出目录存在
            srs_path = Path(srs_dir)
            srs_path.mkdir(parents=True, exist_ok=True)

            # 构建输出文件路径
            output_file = srs_path / f"{ruleset_name}.srs"

            # 构建编译命令，指定输出文件
            cmd = [
                f"./{self.sing_box_binary}",
                "rule-set",
                "compile",
                input_file,
                "--output",
                str(output_file),
            ]

            self.logger.info(f"🔨 编译规则集: {ruleset_name}")
            self.logger.info(f"📄 输入文件: {input_file}")
            self.logger.info(f"📄 输出文件: {output_file}")

            # 执行编译命令
            process_result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60  # 60秒超时
            )

            if process_result.returncode != 0:
                error_msg = process_result.stderr.strip() or "编译失败，未知错误"
                result.set_error(f"编译失败: {error_msg}")
                return result

            # 检查输出文件是否生成
            output_path = Path(output_file)
            if not output_path.exists():
                result.set_error("编译完成但未生成输出文件")
                return result

            # 获取文件大小
            file_size = self.file_utils.get_file_size(output_path)
            formatted_size = self.file_utils.format_file_size(file_size)

            result.set_success(input_file, str(output_file), file_size)

            self.logger.info(f"✅ 规则集编译成功: {output_file} ({formatted_size})")

        except subprocess.TimeoutExpired:
            result.set_error("编译超时")
        except Exception as e:
            result.set_error(f"编译异常: {str(e)}")

        return result

    def compile_all_rulesets(
        self,
        process_results: Optional[Dict[str, ProcessedData]] = None,
        convert_results: Optional[Dict[str, Any]] = None,
        ip_process_results: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, CompileResult]:
        """
        编译所有规则集（包括rulesets处理的、convert转换的和ip_only处理的）

        Args:
            process_results: JSON规则集处理结果字典（可选）
            convert_results: 转换结果字典（可选）
            ip_process_results: IP规则集处理结果字典（可选）

        Returns:
            编译结果字典
        """
        results = {}

        self.logger.header("开始编译阶段")

        # 收集所有需要编译的JSON文件
        compile_tasks = {}

        # 1. 收集rulesets处理生成的JSON文件
        if process_results:
            for name, data in process_results.items():
                if data.success and data.output_file:
                    compile_tasks[name] = data.output_file

        # 2. 收集convert转换生成的JSON文件
        if convert_results:
            for convert_name, convert_data in convert_results.items():
                if convert_data.is_successful():
                    for json_file in convert_data.json_files:
                        # 修改task_name为convert_name（因为现在合并到一个文件）
                        task_name = convert_name
                        compile_tasks[task_name] = json_file

        # 3. 收集ip_only处理生成的JSON文件
        if ip_process_results:
            for name, data in ip_process_results.items():
                if data.success and data.output_file:
                    compile_tasks[name] = data.output_file

        if not compile_tasks:
            self.logger.warning("⚠️ 没有需要编译的JSON文件")
            return results

        try:
            # 设置sing-box
            self.logger.info("🔧 准备sing-box工具")
            self.setup_sing_box()

        except Exception as e:
            self.logger.error(f"❌ sing-box设置失败: {str(e)}")
            # 为所有任务创建失败结果
            for task_name in compile_tasks.keys():
                failed_result = CompileResult(task_name)
                failed_result.set_error(f"sing-box设置失败: {str(e)}")
                results[task_name] = failed_result
            return results

        self.logger.info(f"📋 需要编译 {len(compile_tasks)} 个JSON文件")

        for i, (task_name, json_file) in enumerate(compile_tasks.items(), 1):
            self.logger.step(f"编译文件: {task_name}", i, len(compile_tasks))

            try:
                compile_result = self.compile_ruleset(task_name, json_file)
                results[task_name] = compile_result

            except Exception as e:
                self.logger.error(f"❌ 文件 {task_name} 编译异常: {str(e)}")
                # 创建失败的编译结果
                failed_result = CompileResult(task_name)
                failed_result.set_error(f"编译异常: {str(e)}")
                results[task_name] = failed_result

            # 添加分隔线（除了最后一个）
            if i < len(compile_tasks):
                self.logger.info("─" * 50)

        # 输出总体统计
        successful_compiled = sum(1 for result in results.values() if result.success)
        total_size = sum(
            result.file_size for result in results.values() if result.success
        )
        formatted_total_size = self.file_utils.format_file_size(total_size)

        self.logger.separator("编译阶段完成")
        self.logger.success(
            f"✅ 编译完成: {successful_compiled}/{len(compile_tasks)} 个文件成功"
        )

        if successful_compiled > 0:
            self.logger.info(f"📊 总输出大小: {formatted_total_size}")

        return results

    def cleanup_sing_box(self) -> None:
        """
        清理sing-box相关文件
        """
        try:
            # 删除二进制文件
            if self.sing_box_binary:
                binary_path = Path(self.sing_box_binary)
                if binary_path.exists():
                    binary_path.unlink()
                    self.logger.info(
                        f"🧹 已删除sing-box二进制文件: {self.sing_box_binary}"
                    )

            # 清理临时目录中的sing-box相关文件
            if self.temp_dir.exists():
                for item in self.temp_dir.iterdir():
                    if item.name.startswith("sing-box"):
                        try:
                            if item.is_file():
                                item.unlink()
                            elif item.is_dir():
                                shutil.rmtree(item)
                        except (OSError, PermissionError):
                            continue

            self.sing_box_binary = None

        except Exception as e:
            self.logger.warning(f"⚠️ 清理sing-box文件时出错: {str(e)}")

    def get_compile_statistics(
        self, results: Dict[str, CompileResult]
    ) -> Dict[str, Any]:
        """
        获取编译统计信息

        Args:
            results: 编译结果字典

        Returns:
            统计信息字典
        """
        total_rulesets = len(results)
        successful_rulesets = sum(1 for result in results.values() if result.success)
        total_size = sum(
            result.file_size for result in results.values() if result.success
        )

        # 计算平均文件大小
        avg_size = total_size / successful_rulesets if successful_rulesets > 0 else 0

        return {
            "total_rulesets": total_rulesets,
            "successful_rulesets": successful_rulesets,
            "total_size_bytes": total_size,
            "total_size_formatted": self.file_utils.format_file_size(total_size),
            "average_size_bytes": avg_size,
            "average_size_formatted": self.file_utils.format_file_size(int(avg_size)),
            "success_rate": (
                (successful_rulesets / total_rulesets * 100)
                if total_rulesets > 0
                else 0
            ),
        }

    def verify_compiled_files(
        self, results: Dict[str, CompileResult]
    ) -> Dict[str, bool]:
        """
        验证编译后的文件是否存在且有效

        Args:
            results: 编译结果字典

        Returns:
            验证结果字典
        """
        verification_results = {}

        for ruleset_name, result in results.items():
            if result.success and result.output_file:
                output_path = Path(result.output_file)
                exists = output_path.exists()
                valid_size = output_path.stat().st_size > 0 if exists else False
                verification_results[ruleset_name] = exists and valid_size
            else:
                verification_results[ruleset_name] = False

        return verification_results
