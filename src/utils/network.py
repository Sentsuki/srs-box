"""
网络工具
支持文件下载、JSON下载、重试机制和并发下载功能
"""

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from .file_utils import FileUtils
from .network_cache import NetworkCache
from .network_progress import DownloadProgress, DownloadResult


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
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        self._cache = NetworkCache(cache_dir, cache_ttl_hours)

    # ------------------------------------------------------------------
    # 缓存代理方法（保持原有公共接口）
    # ------------------------------------------------------------------

    def clear_cache(self, older_than_hours: int = None) -> int:
        """
        清理缓存文件

        Args:
            older_than_hours: 清理多少小时前的缓存，None 表示清理所有

        Returns:
            清理的文件数量
        """
        return self._cache.clear(older_than_hours)

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息

        Returns:
            缓存信息字典
        """
        return self._cache.get_info()

    # ------------------------------------------------------------------
    # 内部 HTTP 工具
    # ------------------------------------------------------------------

    def _create_request(
        self, url: str, range_header: str = None
    ) -> urllib.request.Request:
        """创建 HTTP 请求对象"""
        request = urllib.request.Request(url)
        request.add_header("User-Agent", self.user_agent)
        if range_header:
            request.add_header("Range", range_header)
        return request

    def _supports_range_requests(self, url: str) -> bool:
        """检查服务器是否支持 Range 请求（断点续传）"""
        try:
            request = self._create_request(url)
            request.get_method = lambda: "HEAD"
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                accept_ranges = response.headers.get("Accept-Ranges", "")
                return "bytes" in accept_ranges.lower()
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    # ------------------------------------------------------------------
    # 核心下载逻辑
    # ------------------------------------------------------------------

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

        Returns:
            (是否下载成功, 文件大小, 下载耗时)
        """
        start_time = time.time()
        output_path = Path(output_path)

        # 命中缓存则直接复制
        if use_cache and self._cache.copy_to(url, output_path):
            size = output_path.stat().st_size
            return True, size, time.time() - start_time

        try:
            resume_pos = 0
            if support_resume and output_path.exists():
                existing_size = output_path.stat().st_size
                if existing_size > 0 and self._supports_range_requests(url):
                    resume_pos = existing_size

            range_header = f"bytes={resume_pos}-" if resume_pos > 0 else None
            request = self._create_request(url, range_header)

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if resume_pos > 0:
                    content_range = response.headers.get("Content-Range", "")
                    if content_range:
                        total_size = int(content_range.split("/")[-1])
                    else:
                        resume_pos = 0
                        total_size = int(response.headers.get("Content-Length", 0))
                else:
                    total_size = int(response.headers.get("Content-Length", 0))

                FileUtils.ensure_dir(output_path.parent)
                downloaded_size = resume_pos
                chunk_size = 8192

                file_mode = "ab" if resume_pos > 0 else "wb"
                with open(output_path, file_mode) as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if progress_callback and file_id:
                            progress_callback(file_id, downloaded_size, total_size)

            if use_cache and output_path.exists():
                self._cache.save_from(url, output_path)

            duration = time.time() - start_time
            final_size = output_path.stat().st_size if output_path.exists() else 0
            return True, final_size, duration

        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            if resume_pos == 0 and output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
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

        retry_delays = [retry_delay * (2**i) for i in range(max_retries + 1)]

        for attempt in range(max_retries + 1):
            try:
                success, size, _ = self._download_with_progress(
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
                if e.code in [400, 401, 403, 404, 410]:
                    break
                if attempt < max_retries:
                    time.sleep(retry_delays[attempt])
                    continue
                break

            except (urllib.error.URLError, Exception):
                if attempt < max_retries:
                    time.sleep(retry_delays[attempt])
                    continue
                break

        return False

    def download_json(
        self, url: str, max_retries: int = 3, retry_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        下载并解析 JSON 数据

        Args:
            url: JSON URL
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）

        Returns:
            JSON 数据字典，失败时返回 None
        """
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
            ):
                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                break
        return None

    def download_text(
        self, url: str, max_retries: int = 3, retry_delay: float = 1.0
    ) -> Optional[List[str]]:
        """
        下载文本内容并返回行列表

        Args:
            url: 文本 URL
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）

        Returns:
            文本行列表，失败时返回 None
        """
        for attempt in range(max_retries + 1):
            try:
                request = self._create_request(url)
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read().decode("utf-8")
                    return [line.rstrip("\n\r") for line in data.splitlines()]
            except (urllib.error.URLError, urllib.error.HTTPError, UnicodeDecodeError):
                if attempt < max_retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                break
        return None

    # ------------------------------------------------------------------
    # 并发下载
    # ------------------------------------------------------------------

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
        并发下载多个文件

        Args:
            download_tasks: 下载任务列表，每个元素为 (url, output_path) 元组
            max_workers: 最大并发数，默认使用实例配置
            progress_callback: 进度回调，参数为 (已完成数, 总数, 当前文件名, 速度MB/s, 已用时间)
            show_speed: 是否显示下载速度统计

        Returns:
            下载结果列表
        """
        if max_workers is None:
            max_workers = self.max_concurrent

        results: List[DownloadResult] = []
        total_count = len(download_tasks)
        progress_tracker = DownloadProgress(total_count)

        def file_progress_callback(file_id: str, downloaded: int, total: int) -> None:
            progress_tracker.update_file_progress(file_id, downloaded, total)
            completed, total_files, speed, elapsed = (
                progress_tracker.get_overall_progress()
            )
            if progress_callback:
                progress_callback(completed, total_files, file_id, speed, elapsed)

        def download_single(
            task_info: Tuple[int, Tuple[str, Union[str, Path]]],
        ) -> DownloadResult:
            task_index, (url, output_path) = task_info
            output_path = Path(output_path)
            file_id = f"file_{task_index}_{output_path.name}"
            start_time = time.time()

            try:
                success, size, duration = self._download_with_progress(
                    url, output_path, file_progress_callback, file_id
                )
                progress_tracker.complete_file(file_id, size)
                if success:
                    return DownloadResult(
                        url, True, str(output_path), None, size, duration
                    )
                return DownloadResult(url, False, None, "下载失败", 0, duration)
            except Exception as e:
                duration = time.time() - start_time
                progress_tracker.complete_file(file_id, 0)
                return DownloadResult(url, False, None, str(e), 0, duration)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            indexed_tasks = list(enumerate(download_tasks))
            future_to_task = {
                executor.submit(download_single, task_info): task_info
                for task_info in indexed_tasks
            }
            for future in as_completed(future_to_task):
                task_info = future_to_task[future]
                _, (url, _) = task_info
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append(DownloadResult(url, False, None, str(e)))

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

        successful = [r for r in results if r.success]
        total_size = sum(r.size for r in successful)
        total_files = len(results)
        success_count = len(successful)

        avg_speed = (
            (total_size / (1024 * 1024)) / total_time
            if total_time > 0 and total_size > 0
            else 0.0
        )

        stats = {
            "total_files": total_files,
            "successful_files": success_count,
            "failed_files": total_files - success_count,
            "success_rate": (
                (success_count / total_files * 100) if total_files > 0 else 0
            ),
            "total_size_mb": total_size / (1024 * 1024),
            "total_time_seconds": total_time,
            "average_speed_mbps": avg_speed,
            "max_concurrent": max_workers or self.max_concurrent,
            "failed_urls": [r.url for r in results if not r.success],
        }

        return results, stats

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def get_file_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        获取远程文件信息（HEAD 请求，不下载内容）

        Args:
            url: 文件 URL

        Returns:
            文件信息字典，失败时返回 None
        """
        try:
            request = self._create_request(url)
            request.get_method = lambda: "HEAD"
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                headers = response.headers
                return {
                    "url": url,
                    "status_code": response.getcode(),
                    "content_type": headers.get("Content-Type", ""),
                    "content_length": int(headers.get("Content-Length", 0)) or None,
                    "last_modified": headers.get("Last-Modified", ""),
                    "etag": headers.get("ETag", ""),
                }
        except (urllib.error.URLError, urllib.error.HTTPError):
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
        text_extensions = [".txt", ".conf", ".cfg", ".ini"]
        return any(url_lower.endswith(ext) for ext in text_extensions)
