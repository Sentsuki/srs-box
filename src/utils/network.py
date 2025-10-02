"""
网络工具
支持文件下载、JSON下载、重试机制和并发下载功能
"""

import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from .file_utils import FileUtils


class DownloadResult:
    """下载结果类"""

    def __init__(
        self,
        url: str,
        success: bool,
        file_path: Optional[str] = None,
        error: Optional[str] = None,
        size: int = 0,
        duration: float = 0.0,
    ):
        self.url = url
        self.success = success
        self.file_path = file_path
        self.error = error
        self.size = size
        self.duration = duration  # 下载耗时（秒）

    @property
    def speed_mbps(self) -> float:
        """计算下载速度（MB/s）"""
        if self.duration > 0 and self.size > 0:
            return (self.size / (1024 * 1024)) / self.duration
        return 0.0


class DownloadProgress:
    """下载进度跟踪类"""

    def __init__(self, total_files: int):
        self.total_files = total_files
        self.completed_files = 0
        self.total_bytes = 0
        self.downloaded_bytes = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.file_progresses = {}  # 每个文件的进度

    def update_file_progress(self, file_id: str, downloaded: int, total: int):
        """更新单个文件的进度"""
        with self.lock:
            self.file_progresses[file_id] = (downloaded, total)

    def complete_file(self, file_id: str, size: int):
        """标记文件下载完成"""
        with self.lock:
            self.completed_files += 1
            if file_id in self.file_progresses:
                del self.file_progresses[file_id]

    def get_overall_progress(self) -> Tuple[int, int, float, float]:
        """获取整体进度信息"""
        with self.lock:
            # 计算当前总下载字节数
            current_downloaded = sum(prog[0] for prog in self.file_progresses.values())
            current_total = sum(
                prog[1] for prog in self.file_progresses.values() if prog[1] > 0
            )

            elapsed_time = time.time() - self.start_time
            speed_mbps = 0.0
            if elapsed_time > 0 and current_downloaded > 0:
                speed_mbps = (current_downloaded / (1024 * 1024)) / elapsed_time

            return self.completed_files, self.total_files, speed_mbps, elapsed_time


class NetworkUtils:
    """网络工具类"""

    def __init__(
        self,
        timeout: int = 30,
        user_agent: str = None,
        max_concurrent: int = 5,
        cache_dir: str = "temp/cache",
        cache_ttl_hours: int = 24,
    ):
        """
        初始化网络工具

        Args:
            timeout: 请求超时时间（秒），默认为 30
            user_agent: 用户代理字符串
            max_concurrent: 最大并发下载数，默认为 5
            cache_dir: 缓存目录，默认为 temp/cache
            cache_ttl_hours: 缓存有效期（小时），默认为 24 小时
        """
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        # 确保缓存目录存在
        FileUtils.ensure_dir(self.cache_dir)

    def _create_request(
        self, url: str, range_header: str = None
    ) -> urllib.request.Request:
        """
        创建 HTTP 请求对象

        Args:
            url: 请求 URL
            range_header: Range 头部，用于断点续传

        Returns:
            请求对象
        """
        request = urllib.request.Request(url)
        request.add_header("User-Agent", self.user_agent)

        if range_header:
            request.add_header("Range", range_header)

        return request

    def _get_cache_path(self, url: str) -> Path:
        """
        获取URL对应的缓存文件路径

        Args:
            url: URL字符串

        Returns:
            缓存文件路径
        """
        # 使用URL的MD5哈希作为缓存文件名
        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{url_hash}.cache"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """
        检查缓存文件是否有效

        Args:
            cache_path: 缓存文件路径

        Returns:
            缓存是否有效
        """
        if not cache_path.exists():
            return False

        # 检查文件大小
        if cache_path.stat().st_size == 0:
            return False

        # 检查缓存时间
        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - file_time < self.cache_ttl

    def _supports_range_requests(self, url: str) -> bool:
        """
        检查服务器是否支持Range请求（断点续传）

        Args:
            url: 要检查的URL

        Returns:
            是否支持Range请求
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: "HEAD"

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                accept_ranges = response.headers.get("Accept-Ranges", "")
                return "bytes" in accept_ranges.lower()

        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    def _download_with_progress(
        self,
        url: str,
        output_path: Union[str, Path],
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        file_id: str = None,
        use_cache: bool = True,
        support_resume: bool = True,
    ) -> Tuple[bool, int, float]:
        """
        下载文件并显示进度，支持缓存和断点续传

        Args:
            url: 下载 URL
            output_path: 输出文件路径
            progress_callback: 进度回调函数，参数为 (file_id, 已下载字节数, 总字节数)
            file_id: 文件标识符，用于进度跟踪
            use_cache: 是否使用缓存
            support_resume: 是否支持断点续传

        Returns:
            (是否下载成功, 文件大小, 下载耗时)
        """
        start_time = time.time()
        output_path = Path(output_path)

        # 检查缓存
        if use_cache:
            cache_path = self._get_cache_path(url)
            if self._is_cache_valid(cache_path):
                # 使用缓存文件
                FileUtils.ensure_dir(output_path.parent)
                try:
                    # 复制缓存文件到目标位置
                    import shutil

                    shutil.copy2(cache_path, output_path)
                    size = output_path.stat().st_size
                    duration = time.time() - start_time
                    return True, size, duration
                except OSError:
                    pass  # 缓存复制失败，继续正常下载

        try:
            # 检查是否支持断点续传
            resume_pos = 0
            if support_resume and output_path.exists():
                existing_size = output_path.stat().st_size
                if existing_size > 0 and self._supports_range_requests(url):
                    resume_pos = existing_size

            # 创建请求
            range_header = None
            if resume_pos > 0:
                range_header = f"bytes={resume_pos}-"

            request = self._create_request(url, range_header)

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                # 获取文件大小
                if resume_pos > 0:
                    # 断点续传模式
                    content_range = response.headers.get("Content-Range", "")
                    if content_range:
                        # 解析 Content-Range: bytes 200-1023/1024
                        total_size = int(content_range.split("/")[-1])
                    else:
                        # 服务器不支持Range请求，重新下载
                        resume_pos = 0
                        total_size = int(response.headers.get("Content-Length", 0))
                else:
                    total_size = int(response.headers.get("Content-Length", 0))

                # 确保输出目录存在
                FileUtils.ensure_dir(output_path.parent)

                # 下载文件
                downloaded_size = resume_pos
                chunk_size = 8192  # 8KB chunks

                # 选择文件打开模式
                file_mode = "ab" if resume_pos > 0 else "wb"

                with open(output_path, file_mode) as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break

                        f.write(chunk)
                        downloaded_size += len(chunk)

                        # 调用进度回调
                        if progress_callback and file_id:
                            progress_callback(file_id, downloaded_size, total_size)

                # 保存到缓存
                if use_cache and output_path.exists():
                    cache_path = self._get_cache_path(url)
                    try:
                        import shutil

                        shutil.copy2(output_path, cache_path)
                    except OSError:
                        pass  # 缓存保存失败不影响主要功能

                duration = time.time() - start_time
                final_size = output_path.stat().st_size if output_path.exists() else 0
                return True, final_size, duration

        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            # 如果不是断点续传失败，删除部分下载的文件
            if resume_pos == 0 and output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
            duration = time.time() - start_time
            raise e

    def download_file(
        self,
        url: str,
        output_path: Union[str, Path],
        max_retries: int = 3,
        retry_delay: float = 1.0,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        file_id: str = None,
        use_cache: bool = True,
        support_resume: bool = True,
    ) -> bool:
        """
        下载文件到指定路径，支持缓存、重试和断点续传

        Args:
            url: 下载 URL
            output_path: 输出文件路径
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0
            progress_callback: 进度回调函数
            file_id: 文件标识符
            use_cache: 是否使用缓存
            support_resume: 是否支持断点续传

        Returns:
            是否下载成功
        """
        output_path = Path(output_path)

        # 检查文件是否已存在且不为空
        if output_path.exists() and output_path.stat().st_size > 0:
            # 验证文件完整性（简单检查）
            try:
                # 如果文件可以正常读取，认为是完整的
                with open(output_path, "rb") as f:
                    f.read(1)  # 尝试读取第一个字节
                return True
            except OSError:
                # 文件损坏，删除并重新下载
                try:
                    output_path.unlink()
                except OSError:
                    pass

        last_error = None
        retry_delays = [
            retry_delay * (2**i) for i in range(max_retries + 1)
        ]  # 指数退避

        for attempt in range(max_retries + 1):
            try:
                success, size, duration = self._download_with_progress(
                    url,
                    output_path,
                    progress_callback,
                    file_id,
                    use_cache,
                    support_resume,
                )
                if success and size > 0:
                    return True

            except urllib.error.HTTPError as e:
                last_error = f"HTTP {e.code}: {e.reason}"

                # 对于某些HTTP错误码，不进行重试
                if e.code in [400, 401, 403, 404, 410]:  # 客户端错误，不重试
                    break

                if attempt < max_retries:
                    delay = retry_delays[attempt]
                    time.sleep(delay)
                    continue
                else:
                    break

            except urllib.error.URLError as e:
                last_error = f"网络错误: {str(e.reason)}"

                if attempt < max_retries:
                    delay = retry_delays[attempt]
                    time.sleep(delay)
                    continue
                else:
                    break

            except Exception as e:
                last_error = str(e)

                if attempt < max_retries:
                    delay = retry_delays[attempt]
                    time.sleep(delay)
                    continue
                else:
                    break

        return False

    def clear_cache(self, older_than_hours: int = None) -> int:
        """
        清理缓存文件

        Args:
            older_than_hours: 清理多少小时前的缓存，None表示清理所有

        Returns:
            清理的文件数量
        """
        if not self.cache_dir.exists():
            return 0

        cleared_count = 0
        cutoff_time = None

        if older_than_hours is not None:
            cutoff_time = datetime.now() - timedelta(hours=older_than_hours)

        for cache_file in self.cache_dir.glob("*.cache"):
            try:
                if cutoff_time is None:
                    # 清理所有缓存
                    cache_file.unlink()
                    cleared_count += 1
                else:
                    # 只清理过期缓存
                    file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                    if file_time < cutoff_time:
                        cache_file.unlink()
                        cleared_count += 1
            except OSError:
                continue

        return cleared_count

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息

        Returns:
            缓存信息字典
        """
        if not self.cache_dir.exists():
            return {
                "cache_dir": str(self.cache_dir),
                "total_files": 0,
                "total_size_mb": 0,
                "oldest_file": None,
                "newest_file": None,
            }

        cache_files = list(self.cache_dir.glob("*.cache"))
        total_size = sum(f.stat().st_size for f in cache_files if f.exists())

        oldest_time = None
        newest_time = None

        if cache_files:
            file_times = [f.stat().st_mtime for f in cache_files if f.exists()]
            if file_times:
                oldest_time = datetime.fromtimestamp(min(file_times))
                newest_time = datetime.fromtimestamp(max(file_times))

        return {
            "cache_dir": str(self.cache_dir),
            "total_files": len(cache_files),
            "total_size_mb": total_size / (1024 * 1024),
            "oldest_file": oldest_time.isoformat() if oldest_time else None,
            "newest_file": newest_time.isoformat() if newest_time else None,
            "ttl_hours": self.cache_ttl.total_seconds() / 3600,
        }

    def download_json(
        self, url: str, max_retries: int = 3, retry_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        下载并解析 JSON 数据

        Args:
            url: JSON URL
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0

        Returns:
            JSON 数据字典，失败时返回 None
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                request = self._create_request(url)

                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read().decode("utf-8")
                    return json.loads(data)

            except (
                urllib.error.URLError,
                urllib.error.HTTPError,
                json.JSONDecodeError,
                UnicodeDecodeError,
            ) as e:
                last_error = str(e)

                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    break

        return None

    def download_text(
        self, url: str, max_retries: int = 3, retry_delay: float = 1.0
    ) -> Optional[List[str]]:
        """
        下载文本内容并返回行列表

        Args:
            url: 文本 URL
            max_retries: 最大重试次数，默认为 3
            retry_delay: 重试延迟（秒），默认为 1.0

        Returns:
            文本行列表，失败时返回 None
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                request = self._create_request(url)

                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read().decode("utf-8")
                    return [line.rstrip("\n\r") for line in data.splitlines()]

            except (
                urllib.error.URLError,
                urllib.error.HTTPError,
                UnicodeDecodeError,
            ) as e:
                last_error = str(e)

                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    break

        return None

    def download_multiple(
        self,
        download_tasks: List[Tuple[str, Union[str, Path]]],
        max_workers: int = None,
        progress_callback: Optional[
            Callable[[int, int, str, float, float], None]
        ] = None,
        show_speed: bool = True,
    ) -> List[DownloadResult]:
        """
        并发下载多个文件，支持实时进度显示和速度统计

        Args:
            download_tasks: 下载任务列表，每个元素为 (url, output_path) 元组
            max_workers: 最大并发数，默认使用实例配置
            progress_callback: 进度回调函数，参数为 (已完成数, 总数, 当前文件名, 速度MB/s, 已用时间)
            show_speed: 是否显示下载速度统计

        Returns:
            下载结果列表
        """
        if max_workers is None:
            max_workers = self.max_concurrent

        results = []
        total_count = len(download_tasks)

        # 创建进度跟踪器
        progress_tracker = DownloadProgress(total_count)

        def file_progress_callback(file_id: str, downloaded: int, total: int):
            """单个文件进度回调"""
            progress_tracker.update_file_progress(file_id, downloaded, total)

            # 获取整体进度信息
            completed, total_files, speed, elapsed = (
                progress_tracker.get_overall_progress()
            )

            if progress_callback:
                progress_callback(completed, total_files, file_id, speed, elapsed)

        def download_single(
            task_info: Tuple[int, Tuple[str, Union[str, Path]]],
        ) -> DownloadResult:
            """下载单个文件的内部函数"""
            task_index, (url, output_path) = task_info
            output_path = Path(output_path)
            file_id = f"file_{task_index}_{output_path.name}"

            start_time = time.time()

            try:
                success, size, duration = self._download_with_progress(
                    url, output_path, file_progress_callback, file_id
                )

                # 标记文件完成
                progress_tracker.complete_file(file_id, size)

                if success:
                    return DownloadResult(
                        url, True, str(output_path), None, size, duration
                    )
                else:
                    return DownloadResult(url, False, None, "下载失败", 0, duration)

            except Exception as e:
                duration = time.time() - start_time
                progress_tracker.complete_file(file_id, 0)
                return DownloadResult(url, False, None, str(e), 0, duration)

        # 使用线程池并发下载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 为任务添加索引
            indexed_tasks = list(enumerate(download_tasks))

            # 提交所有任务
            future_to_task = {
                executor.submit(download_single, task_info): task_info
                for task_info in indexed_tasks
            }

            # 处理完成的任务
            for future in as_completed(future_to_task):
                task_info = future_to_task[future]
                task_index, (url, output_path) = task_info

                try:
                    result = future.result()
                    results.append(result)

                except Exception as e:
                    # 处理异常
                    result = DownloadResult(url, False, None, str(e))
                    results.append(result)

        return results

    def download_multiple_with_stats(
        self,
        download_tasks: List[Tuple[str, Union[str, Path]]],
        max_workers: int = None,
    ) -> Tuple[List[DownloadResult], Dict[str, Any]]:
        """
        并发下载多个文件并返回详细统计信息

        Args:
            download_tasks: 下载任务列表
            max_workers: 最大并发数

        Returns:
            (下载结果列表, 统计信息字典)
        """
        start_time = time.time()
        results = self.download_multiple(download_tasks, max_workers, show_speed=True)
        total_time = time.time() - start_time

        # 计算统计信息
        successful_downloads = [r for r in results if r.success]
        failed_downloads = [r for r in results if not r.success]

        total_size = sum(r.size for r in successful_downloads)
        total_files = len(results)
        success_count = len(successful_downloads)

        avg_speed = 0.0
        if total_time > 0 and total_size > 0:
            avg_speed = (total_size / (1024 * 1024)) / total_time

        stats = {
            "total_files": total_files,
            "successful_files": success_count,
            "failed_files": len(failed_downloads),
            "success_rate": (
                (success_count / total_files * 100) if total_files > 0 else 0
            ),
            "total_size_mb": total_size / (1024 * 1024),
            "total_time_seconds": total_time,
            "average_speed_mbps": avg_speed,
            "max_concurrent": max_workers or self.max_concurrent,
            "failed_urls": [r.url for r in failed_downloads],
        }

        return results, stats

    def get_file_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        获取远程文件信息（不下载文件内容）

        Args:
            url: 文件 URL

        Returns:
            文件信息字典，包含 size, last_modified 等，失败时返回 None
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: "HEAD"  # 使用 HEAD 请求

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                headers = response.headers

                info = {
                    "url": url,
                    "status_code": response.getcode(),
                    "content_type": headers.get("Content-Type", ""),
                    "content_length": int(headers.get("Content-Length", 0)) or None,
                    "last_modified": headers.get("Last-Modified", ""),
                    "etag": headers.get("ETag", ""),
                }

                return info

        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            return None

    def is_url_accessible(self, url: str) -> bool:
        """
        检查 URL 是否可访问

        Args:
            url: 要检查的 URL

        Returns:
            URL 是否可访问
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: "HEAD"

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.getcode() == 200

        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    def get_filename_from_url(self, url: str) -> str:
        """
        从 URL 中提取文件名

        Args:
            url: URL 字符串

        Returns:
            文件名
        """
        parsed_url = urlparse(url)
        filename = Path(parsed_url.path).name

        # 如果没有文件名，使用默认名称
        if not filename or "." not in filename:
            filename = f"download_{hash(url) % 10000}"

        return filename

    @staticmethod
    def is_json_url(url: str) -> bool:
        """
        判断 URL 是否指向 JSON 文件

        Args:
            url: URL 字符串

        Returns:
            是否为 JSON URL
        """
        url_lower = url.lower()
        # .list 文件实际上是标准的 JSON 格式
        return (
            url_lower.endswith(".json")
            or url_lower.endswith(".list")
            or "json" in url_lower
            or url_lower.endswith(".jsonl")
        )

    @staticmethod
    def is_text_url(url: str) -> bool:
        """
        判断 URL 是否指向文本文件

        Args:
            url: URL 字符串

        Returns:
            是否为文本 URL
        """
        url_lower = url.lower()
        # 移除 .list，因为它实际上是 JSON 格式
        text_extensions = [".txt", ".conf", ".cfg", ".ini"]
        return any(url_lower.endswith(ext) for ext in text_extensions)
